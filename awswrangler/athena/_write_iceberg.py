"""Amazon Athena Module containing all to_* write functions."""

from __future__ import annotations

import logging
import re
import typing
import uuid
from typing import Any, Dict, Literal, TypedDict, cast

import boto3
import pandas as pd

from awswrangler import _data_types, _utils, catalog, exceptions, s3
from awswrangler._config import apply_configs
from awswrangler.athena._executions import wait_query
from awswrangler.athena._utils import (
    _get_workgroup_config,
    _start_query_execution,
    _WorkGroupConfig,
)
from awswrangler.typing import GlueTableSettings

_logger: logging.Logger = logging.getLogger(__name__)


def _create_iceberg_table(
    df: pd.DataFrame,
    database: str,
    table: str,
    path: str | None,
    wg_config: _WorkGroupConfig,
    partition_cols: list[str] | None,
    additional_table_properties: dict[str, Any] | None,
    index: bool = False,
    data_source: str | None = None,
    workgroup: str | None = None,
    s3_output: str | None = None,
    encryption: str | None = None,
    kms_key: str | None = None,
    boto3_session: boto3.Session | None = None,
    dtype: dict[str, str] | None = None,
    columns_comments: dict[str, Any] | None = None,
) -> None:
    if not path:
        raise exceptions.InvalidArgumentValue("Must specify table location to create the table.")

    columns_types, _ = catalog.extract_athena_types(df=df, index=index, dtype=dtype)
    cols_str: str = ", ".join(
        [
            f"{k} {v}"
            if (columns_comments is None or columns_comments.get(k) is None)
            else f"{k} {v} COMMENT '{columns_comments[k]}'"
            for k, v in columns_types.items()
        ]
    )
    partition_cols_str: str = f"PARTITIONED BY ({', '.join([col for col in partition_cols])})" if partition_cols else ""
    table_properties_str: str = (
        ", " + ", ".join([f"'{key}'='{value}'" for key, value in additional_table_properties.items()])
        if additional_table_properties
        else ""
    )

    create_sql: str = (
        f"CREATE TABLE IF NOT EXISTS `{table}` ({cols_str}) "
        f"{partition_cols_str} "
        f"LOCATION '{path}' "
        f"TBLPROPERTIES ('table_type' ='ICEBERG', 'format'='parquet'{table_properties_str})"
    )

    query_execution_id: str = _start_query_execution(
        sql=create_sql,
        workgroup=workgroup,
        wg_config=wg_config,
        database=database,
        data_source=data_source,
        s3_output=s3_output,
        encryption=encryption,
        kms_key=kms_key,
        boto3_session=boto3_session,
    )
    wait_query(query_execution_id=query_execution_id, boto3_session=boto3_session)


class _SchemaChanges(TypedDict):
    new_columns: dict[str, str]
    modified_columns: dict[str, str]
    missing_columns: dict[str, str]


def _determine_differences(
    df: pd.DataFrame,
    database: str,
    table: str,
    index: bool,
    partition_cols: list[str] | None,
    boto3_session: boto3.Session | None,
    dtype: dict[str, str] | None,
    catalog_id: str | None,
) -> tuple[_SchemaChanges, list[str]]:
    if partition_cols:
        # Remove columns using partition transform function,
        # as they won't be found in the DataFrame or the Glue catalog.
        # Examples include day(column_name) and truncate(10, column_name).
        pattern = r"[A-Za-z0-9_]+\(.+\)"
        partition_cols = [col for col in partition_cols if re.match(pattern, col) is None]

    frame_columns_types, frame_partitions_types = _data_types.athena_types_from_pandas_partitioned(
        df=df, index=index, partition_cols=partition_cols, dtype=dtype
    )
    frame_columns_types.update(frame_partitions_types)

    # lowercase DataFrame columns, as all the column names from Athena will be lowercased
    frame_columns_types = {k.lower(): v for k, v in frame_columns_types.items()}

    catalog_column_types = typing.cast(
        Dict[str, str],
        catalog.get_table_types(database=database, table=table, catalog_id=catalog_id, boto3_session=boto3_session),
    )

    original_column_names = set(catalog_column_types)
    new_column_names = set(frame_columns_types)

    new_columns = {col: frame_columns_types[col] for col in new_column_names - original_column_names}
    missing_columns = {col: catalog_column_types[col] for col in original_column_names - new_column_names}

    columns_to_change = [
        col
        for col in original_column_names.intersection(new_column_names)
        if frame_columns_types[col] != catalog_column_types[col]
    ]
    modified_columns = {col: frame_columns_types[col] for col in columns_to_change}

    return (
        _SchemaChanges(new_columns=new_columns, modified_columns=modified_columns, missing_columns=missing_columns),
        [key for key in catalog_column_types],
    )


def _alter_iceberg_table(
    database: str,
    table: str,
    schema_changes: _SchemaChanges,
    fill_missing_columns_in_df: bool,
    wg_config: _WorkGroupConfig,
    data_source: str | None = None,
    workgroup: str | None = None,
    s3_output: str | None = None,
    encryption: str | None = None,
    kms_key: str | None = None,
    boto3_session: boto3.Session | None = None,
) -> None:
    sql_statements: list[str] = []

    if schema_changes["new_columns"]:
        sql_statements += _alter_iceberg_table_add_columns_sql(
            table=table,
            columns_to_add=schema_changes["new_columns"],
        )

    if schema_changes["modified_columns"]:
        sql_statements += _alter_iceberg_table_change_columns_sql(
            table=table,
            columns_to_change=schema_changes["modified_columns"],
        )

    if schema_changes["missing_columns"] and not fill_missing_columns_in_df:
        raise exceptions.InvalidArgumentCombination(
            f"Dropping columns of Iceberg tables is not supported: {schema_changes['missing_columns']}. "
            "Please use `fill_missing_columns_in_df=True` to fill missing columns with N/A."
        )

    for statement in sql_statements:
        query_execution_id: str = _start_query_execution(
            sql=statement,
            workgroup=workgroup,
            wg_config=wg_config,
            database=database,
            data_source=data_source,
            s3_output=s3_output,
            encryption=encryption,
            kms_key=kms_key,
            boto3_session=boto3_session,
        )
        wait_query(query_execution_id=query_execution_id, boto3_session=boto3_session)


def _alter_iceberg_table_add_columns_sql(
    table: str,
    columns_to_add: dict[str, str],
) -> list[str]:
    add_cols_str = ", ".join([f"{col_name} {columns_to_add[col_name]}" for col_name in columns_to_add])

    return [f"ALTER TABLE {table} ADD COLUMNS ({add_cols_str})"]


def _alter_iceberg_table_change_columns_sql(
    table: str,
    columns_to_change: dict[str, str],
) -> list[str]:
    sql_statements = []

    for col_name, col_type in columns_to_change.items():
        sql_statements.append(f"ALTER TABLE {table} CHANGE COLUMN {col_name} {col_name} {col_type}")

    return sql_statements


def _validate_args(
    df: pd.DataFrame,
    temp_path: str | None,
    wg_config: _WorkGroupConfig,
    mode: Literal["append", "overwrite", "overwrite_partitions"],
    partition_cols: list[str] | None,
    merge_cols: list[str] | None,
) -> None:
    if df.empty is True:
        raise exceptions.EmptyDataFrame("DataFrame cannot be empty.")

    if not temp_path and not wg_config.s3_output:
        raise exceptions.InvalidArgumentCombination(
            "Either path or workgroup path must be specified to store the temporary results."
        )

    if mode == "overwrite_partitions":
        if not partition_cols:
            raise exceptions.InvalidArgumentCombination(
                "When mode is 'overwrite_partitions' partition_cols must be specified."
            )
        if merge_cols:
            raise exceptions.InvalidArgumentCombination(
                "When mode is 'overwrite_partitions' merge_cols must not be specified."
            )


@apply_configs
@_utils.validate_distributed_kwargs(
    unsupported_kwargs=["boto3_session", "s3_additional_kwargs"],
)
def to_iceberg(
    df: pd.DataFrame,
    database: str,
    table: str,
    temp_path: str | None = None,
    index: bool = False,
    table_location: str | None = None,
    partition_cols: list[str] | None = None,
    merge_cols: list[str] | None = None,
    keep_files: bool = True,
    data_source: str | None = None,
    s3_output: str | None = None,
    workgroup: str = "primary",
    mode: Literal["append", "overwrite", "overwrite_partitions"] = "append",
    encryption: str | None = None,
    kms_key: str | None = None,
    boto3_session: boto3.Session | None = None,
    s3_additional_kwargs: dict[str, Any] | None = None,
    additional_table_properties: dict[str, Any] | None = None,
    dtype: dict[str, str] | None = None,
    catalog_id: str | None = None,
    schema_evolution: bool = False,
    fill_missing_columns_in_df: bool = True,
    glue_table_settings: GlueTableSettings | None = None,
) -> None:
    """
    Insert into Athena Iceberg table using INSERT INTO ... SELECT. Will create Iceberg table if it does not exist.

    Creates temporary external table, writes staged files and inserts via INSERT INTO ... SELECT.

    Parameters
    ----------
    df : pd.DataFrame
        Pandas DataFrame.
    database : str
        AWS Glue/Athena database name - It is only the origin database from where the query will be launched.
        You can still using and mixing several databases writing the full table name within the sql
        (e.g. `database.table`).
    table : str
        AWS Glue/Athena table name.
    temp_path : str
        Amazon S3 location to store temporary results. Workgroup config will be used if not provided.
    index: bool
        Should consider the DataFrame index as a column?.
    table_location : str, optional
        Amazon S3 location for the table. Will only be used to create a new table if it does not exist.
    partition_cols: List[str], optional
        List of column names that will be used to create partitions, including support for transform
        functions (e.g. "day(ts)").

        https://docs.aws.amazon.com/athena/latest/ug/querying-iceberg-creating-tables.html#querying-iceberg-partitioning
    merge_cols: List[str], optional
        List of column names that will be used for conditional inserts and updates.

        https://docs.aws.amazon.com/athena/latest/ug/merge-into-statement.html
    keep_files : bool
        Whether staging files produced by Athena are retained. 'True' by default.
    data_source : str, optional
        Data Source / Catalog name. If None, 'AwsDataCatalog' will be used by default.
    s3_output : str, optional
        Amazon S3 path used for query execution.
    workgroup : str
        Athena workgroup. Primary by default.
    mode: str
        ``append`` (default), ``overwrite``, ``overwrite_partitions``.
    encryption : str, optional
        Valid values: [None, 'SSE_S3', 'SSE_KMS']. Notice: 'CSE_KMS' is not supported.
    kms_key : str, optional
        For SSE-KMS, this is the KMS key ARN or ID.
    boto3_session : boto3.Session(), optional
        Boto3 Session. The default boto3 session will be used if boto3_session receive None.
    s3_additional_kwargs: dict[str, Any], optional
        Forwarded to botocore requests.
        e.g. s3_additional_kwargs={'RequestPayer': 'requester'}
    additional_table_properties: dict[str, Any], optional
        Additional table properties.
        e.g. additional_table_properties={'write_target_data_file_size_bytes': '536870912'}

        https://docs.aws.amazon.com/athena/latest/ug/querying-iceberg-creating-tables.html#querying-iceberg-table-properties
    dtype: dict[str, str], optional
        Dictionary of columns names and Athena/Glue types to be casted.
        Useful when you have columns with undetermined or mixed data types.
        e.g. {'col name': 'bigint', 'col2 name': 'int'}
    catalog_id : str, optional
        The ID of the Data Catalog from which to retrieve Databases.
        If none is provided, the AWS account ID is used by default
    schema_evolution: bool, optional
        If ``True`` allows schema evolution for new columns or changes in column types.
        Columns missing from the DataFrame that are present in the Iceberg schema
        will throw an error unless ``fill_missing_columns_in_df`` is set to ``True``.
        Default is ``False``.
    fill_missing_columns_in_df: bool, optional
        If ``True``, fill columns that was missing in the DataFrame with ``NULL`` values.
        Default is ``True``.
    columns_comments: GlueTableSettings, optional
        Glue/Athena catalog: Settings for writing to the Glue table.
        Currently only the 'columns_comments' attribute is supported for this function.
        Columns comments can only be added with this function when creating a new table.

    Returns
    -------
    None

    Examples
    --------
    Insert into an existing Iceberg table

    >>> import awswrangler as wr
    >>> import pandas as pd
    >>> wr.athena.to_iceberg(
    ...     df=pd.DataFrame({'col': [1, 2, 3]}),
    ...     database='my_database',
    ...     table='my_table',
    ...     temp_path='s3://bucket/temp/',
    ... )

    Create Iceberg table and insert data (table doesn't exist, requires table_location)

    >>> import awswrangler as wr
    >>> import pandas as pd
    >>> wr.athena.to_iceberg(
    ...     df=pd.DataFrame({'col': [1, 2, 3]}),
    ...     database='my_database',
    ...     table='my_table2',
    ...     table_location='s3://bucket/my_table2/',
    ...     temp_path='s3://bucket/temp/',
    ... )

    """
    wg_config: _WorkGroupConfig = _get_workgroup_config(session=boto3_session, workgroup=workgroup)
    temp_table: str = f"temp_table_{uuid.uuid4().hex}"

    _validate_args(
        df=df,
        temp_path=temp_path,
        wg_config=wg_config,
        mode=mode,
        partition_cols=partition_cols,
        merge_cols=merge_cols,
    )

    glue_table_settings = cast(
        GlueTableSettings,
        glue_table_settings if glue_table_settings else {},
    )

    try:
        # Create Iceberg table if it doesn't exist
        if not catalog.does_table_exist(
            database=database, table=table, boto3_session=boto3_session, catalog_id=catalog_id
        ):
            _create_iceberg_table(
                df=df,
                database=database,
                table=table,
                path=table_location,
                wg_config=wg_config,
                partition_cols=partition_cols,
                additional_table_properties=additional_table_properties,
                index=index,
                data_source=data_source,
                workgroup=workgroup,
                s3_output=s3_output,
                encryption=encryption,
                kms_key=kms_key,
                boto3_session=boto3_session,
                dtype=dtype,
                columns_comments=glue_table_settings.get("columns_comments"),
            )
        else:
            schema_differences, catalog_cols = _determine_differences(
                df=df,
                database=database,
                table=table,
                index=index,
                partition_cols=partition_cols,
                boto3_session=boto3_session,
                dtype=dtype,
                catalog_id=catalog_id,
            )

            # Add missing columns to the DataFrame
            if fill_missing_columns_in_df and schema_differences["missing_columns"]:
                for col_name, col_type in schema_differences["missing_columns"].items():
                    df[col_name] = None
                    df[col_name] = df[col_name].astype(_data_types.athena2pandas(col_type))

                schema_differences["missing_columns"] = {}

                # Ensure that the ordering of the DF is the same as in the catalog.
                # This is required for the INSERT command to work.
                df = df[catalog_cols]

            if schema_evolution is False and any([schema_differences[x] for x in schema_differences]):  # type: ignore[literal-required]
                raise exceptions.InvalidArgumentValue(f"Schema change detected: {schema_differences}")

            _alter_iceberg_table(
                database=database,
                table=table,
                schema_changes=schema_differences,
                fill_missing_columns_in_df=fill_missing_columns_in_df,
                wg_config=wg_config,
                data_source=data_source,
                workgroup=workgroup,
                s3_output=s3_output,
                encryption=encryption,
                kms_key=kms_key,
                boto3_session=boto3_session,
            )

        # if mode == "overwrite_partitions", drop matched partitions
        if mode == "overwrite_partitions":
            delete_from_iceberg_table(
                df=df,
                database=database,
                table=table,
                merge_cols=partition_cols,  # type: ignore[arg-type]
                temp_path=temp_path,
                keep_files=False,
                data_source=data_source,
                workgroup=workgroup,
                encryption=encryption,
                kms_key=kms_key,
                boto3_session=boto3_session,
                s3_additional_kwargs=s3_additional_kwargs,
                catalog_id=catalog_id,
            )
        # if mode == "overwrite", delete whole data from table (but not table itself)
        elif mode == "overwrite":
            delete_sql_statement = f"DELETE FROM {table}"
            delete_query_execution_id: str = _start_query_execution(
                sql=delete_sql_statement,
                workgroup=workgroup,
                wg_config=wg_config,
                database=database,
                data_source=data_source,
                encryption=encryption,
                kms_key=kms_key,
                boto3_session=boto3_session,
            )
            wait_query(query_execution_id=delete_query_execution_id, boto3_session=boto3_session)

        # Create temporary external table, write the results
        s3.to_parquet(
            df=df,
            path=temp_path or wg_config.s3_output,
            dataset=True,
            database=database,
            table=temp_table,
            boto3_session=boto3_session,
            s3_additional_kwargs=s3_additional_kwargs,
            dtype=dtype,
            catalog_id=catalog_id,
            glue_table_settings=glue_table_settings,
        )

        # Insert or merge into Iceberg table
        sql_statement: str
        if merge_cols:
            sql_statement = f"""
                MERGE INTO "{database}"."{table}" target
                USING "{database}"."{temp_table}" source
                ON {' AND '.join([f'target."{x}" = source."{x}"' for x in merge_cols])}
                WHEN MATCHED THEN
                    UPDATE SET {', '.join([f'"{x}" = source."{x}"' for x in df.columns])}
                WHEN NOT MATCHED THEN
                    INSERT ({', '.join([f'"{x}"' for x in df.columns])})
                    VALUES ({', '.join([f'source."{x}"' for x in df.columns])})
            """
        else:
            sql_statement = f"""
            INSERT INTO "{database}"."{table}" ({', '.join([f'"{x}"' for x in df.columns])})
            SELECT {', '.join([f'"{x}"' for x in df.columns])}
              FROM "{database}"."{temp_table}"
            """

        query_execution_id: str = _start_query_execution(
            sql=sql_statement,
            workgroup=workgroup,
            wg_config=wg_config,
            database=database,
            data_source=data_source,
            s3_output=s3_output,
            encryption=encryption,
            kms_key=kms_key,
            boto3_session=boto3_session,
        )
        wait_query(query_execution_id=query_execution_id, boto3_session=boto3_session)

    except Exception as ex:
        _logger.error(ex)

        raise
    finally:
        catalog.delete_table_if_exists(
            database=database, table=temp_table, boto3_session=boto3_session, catalog_id=catalog_id
        )

        if keep_files is False:
            s3.delete_objects(
                path=temp_path or wg_config.s3_output,  # type: ignore[arg-type]
                boto3_session=boto3_session,
                s3_additional_kwargs=s3_additional_kwargs,
            )


@apply_configs
@_utils.validate_distributed_kwargs(
    unsupported_kwargs=["boto3_session", "s3_additional_kwargs"],
)
def delete_from_iceberg_table(
    df: pd.DataFrame,
    database: str,
    table: str,
    merge_cols: list[str],
    temp_path: str | None = None,
    keep_files: bool = True,
    data_source: str | None = None,
    workgroup: str = "primary",
    encryption: str | None = None,
    kms_key: str | None = None,
    boto3_session: boto3.Session | None = None,
    s3_additional_kwargs: dict[str, Any] | None = None,
    catalog_id: str | None = None,
) -> None:
    """
    Delete rows from an Iceberg table.

    Creates temporary external table, writes staged files and then deletes any rows which match the contents of the temporary table.

    Parameters
    ----------
    df: pandas.DataFrame
        Pandas DataFrame containing the IDs of rows that are to be deleted from the Iceberg table.
    database: str
        Database name.
    table: str
        Table name.
    merge_cols: list[str]
        List of columns to be used to determine which rows of the Iceberg table should be deleted.

        `MERGE INTO <https://docs.aws.amazon.com/athena/latest/ug/merge-into-statement.html>`_
    temp_path: str, optional
        S3 path to temporarily store the DataFrame.
    keep_files: bool
        Whether staging files produced by Athena are retained. ``True`` by default.
    data_source: str, optional
        The AWS KMS key ID or alias used to encrypt the data.
    workgroup: str, optional
        Athena workgroup name.
    encryption: str, optional
        Valid values: [``None``, ``"SSE_S3"``, ``"SSE_KMS"``]. Notice: ``"CSE_KMS"`` is not supported.
    kms_key: str, optional
        For SSE-KMS, this is the KMS key ARN or ID.
    boto3_session: boto3.Session(), optional
        Boto3 Session. The default boto3 session will be used if ``boto3_session`` receive None.
    s3_additional_kwargs: Optional[Dict[str, Any]]
        Forwarded to botocore requests.
        e.g. ```s3_additional_kwargs={"RequestPayer": "requester"}```
    catalog_id: str, optional
        The ID of the Data Catalog which contains the database and table.
        If none is provided, the AWS account ID is used by default.

    Returns
    -------
    None

    Examples
    --------
    >>> import awswrangler as wr
    >>> import pandas as pd
    >>> df = pd.DataFrame({"id": [1, 2, 3], "col": ["foo", "bar", "baz"]})
    >>> wr.athena.to_iceberg(
    ...     df=df,
    ...     database="my_database",
    ...     table="my_table",
    ...     temp_path="s3://bucket/temp/",
    ... )
    >>> df_delete = pd.DataFrame({"id": [1, 3]})
    >>> wr.athena.delete_from_iceberg_table(
    ...     df=df_delete,
    ...     database="my_database",
    ...     table="my_table",
    ...     merge_cols=["id"],
    ... )
    >>> wr.athena.read_sql_table(table="my_table", database="my_database")
        id  col
    0   2   bar
    """
    if df.empty is True:
        raise exceptions.EmptyDataFrame("DataFrame cannot be empty.")

    if not merge_cols:
        raise exceptions.InvalidArgumentValue("Merge columns must be specified.")

    wg_config: _WorkGroupConfig = _get_workgroup_config(session=boto3_session, workgroup=workgroup)
    temp_table: str = f"temp_table_{uuid.uuid4().hex}"

    if not temp_path and not wg_config.s3_output:
        raise exceptions.InvalidArgumentCombination(
            "Either path or workgroup path must be specified to store the temporary results."
        )

    if not catalog.does_table_exist(database=database, table=table, boto3_session=boto3_session, catalog_id=catalog_id):
        raise exceptions.InvalidTable(f"Table {table} does not exist in database {database}.")

    df = df[merge_cols].drop_duplicates(ignore_index=True)

    try:
        # Create temporary external table, write the results
        s3.to_parquet(
            df=df,
            path=temp_path or wg_config.s3_output,
            dataset=True,
            database=database,
            table=temp_table,
            boto3_session=boto3_session,
            s3_additional_kwargs=s3_additional_kwargs,
            catalog_id=catalog_id,
            index=False,
        )

        sql_statement = f"""
            MERGE INTO "{database}"."{table}" target
            USING "{database}"."{temp_table}" source
            ON {' AND '.join([f'target."{x}" = source."{x}"' for x in merge_cols])}
            WHEN MATCHED THEN
                DELETE
        """

        query_execution_id: str = _start_query_execution(
            sql=sql_statement,
            workgroup=workgroup,
            wg_config=wg_config,
            database=database,
            data_source=data_source,
            encryption=encryption,
            kms_key=kms_key,
            boto3_session=boto3_session,
        )
        wait_query(query_execution_id=query_execution_id, boto3_session=boto3_session)

    except Exception as ex:
        _logger.error(ex)

        raise

    finally:
        catalog.delete_table_if_exists(
            database=database, table=temp_table, boto3_session=boto3_session, catalog_id=catalog_id
        )

        if keep_files is False:
            s3.delete_objects(
                path=temp_path or wg_config.s3_output,  # type: ignore[arg-type]
                boto3_session=boto3_session,
                s3_additional_kwargs=s3_additional_kwargs,
            )
