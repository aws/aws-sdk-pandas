"""Amazon Athena Module containing all to_* write functions."""

import logging
import typing
import uuid
from typing import Any, Dict, List, Optional, Set, TypedDict, cast

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
    path: str,
    wg_config: _WorkGroupConfig,
    partition_cols: Optional[List[str]],
    additional_table_properties: Optional[Dict[str, Any]],
    index: bool = False,
    data_source: Optional[str] = None,
    workgroup: Optional[str] = None,
    encryption: Optional[str] = None,
    kms_key: Optional[str] = None,
    boto3_session: Optional[boto3.Session] = None,
    dtype: Optional[Dict[str, str]] = None,
    columns_comments: Optional[Dict[str, Any]] = None,
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
        encryption=encryption,
        kms_key=kms_key,
        boto3_session=boto3_session,
    )
    wait_query(query_execution_id=query_execution_id, boto3_session=boto3_session)


class _SchemaChanges(TypedDict):
    to_add: Dict[str, str]
    to_change: Dict[str, str]
    to_remove: Set[str]


def _determine_differences(
    df: pd.DataFrame,
    database: str,
    table: str,
    index: bool,
    partition_cols: Optional[List[str]],
    boto3_session: Optional[boto3.Session],
    dtype: Optional[Dict[str, str]],
    catalog_id: Optional[str],
) -> _SchemaChanges:
    frame_columns_types, frame_partitions_types = _data_types.athena_types_from_pandas_partitioned(
        df=df, index=index, partition_cols=partition_cols, dtype=dtype
    )
    frame_columns_types.update(frame_partitions_types)

    catalog_column_types = typing.cast(
        Dict[str, str],
        catalog.get_table_types(database=database, table=table, catalog_id=catalog_id, boto3_session=boto3_session),
    )

    original_columns = set(catalog_column_types)
    new_columns = set(frame_columns_types)

    to_add = {col: frame_columns_types[col] for col in new_columns - original_columns}
    to_remove = original_columns - new_columns

    columns_to_change = [
        col
        for col in original_columns.intersection(new_columns)
        if frame_columns_types[col] != catalog_column_types[col]
    ]
    to_change = {col: frame_columns_types[col] for col in columns_to_change}

    return _SchemaChanges(to_add=to_add, to_change=to_change, to_remove=to_remove)


def _alter_iceberg_table(
    database: str,
    table: str,
    schema_changes: _SchemaChanges,
    wg_config: _WorkGroupConfig,
    data_source: Optional[str] = None,
    workgroup: Optional[str] = None,
    encryption: Optional[str] = None,
    kms_key: Optional[str] = None,
    boto3_session: Optional[boto3.Session] = None,
) -> None:
    sql_statements: List[str] = []

    if schema_changes["to_add"]:
        sql_statements += _alter_iceberg_table_add_columns_sql(
            table=table,
            columns_to_add=schema_changes["to_add"],
        )

    if schema_changes["to_change"]:
        sql_statements += _alter_iceberg_table_change_columns_sql(
            table=table,
            columns_to_change=schema_changes["to_change"],
        )

    if schema_changes["to_remove"]:
        raise exceptions.InvalidArgumentCombination("Removing columns of Iceberg tables is not currently supported.")

    for statement in sql_statements:
        query_execution_id: str = _start_query_execution(
            sql=statement,
            workgroup=workgroup,
            wg_config=wg_config,
            database=database,
            data_source=data_source,
            encryption=encryption,
            kms_key=kms_key,
            boto3_session=boto3_session,
        )
        wait_query(query_execution_id=query_execution_id, boto3_session=boto3_session)


def _alter_iceberg_table_add_columns_sql(
    table: str,
    columns_to_add: Dict[str, str],
) -> List[str]:
    add_cols_str = ", ".join([f"{col_name} {columns_to_add[col_name]}" for col_name in columns_to_add])

    return [f"ALTER TABLE {table} ADD COLUMNS ({add_cols_str})"]


def _alter_iceberg_table_change_columns_sql(
    table: str,
    columns_to_change: Dict[str, str],
) -> List[str]:
    sql_statements = []

    for col_name, col_type in columns_to_change.items():
        sql_statements.append(f"ALTER TABLE {table} CHANGE COLUMN {col_name} {col_name} {col_type}")

    return sql_statements


@apply_configs
@_utils.validate_distributed_kwargs(
    unsupported_kwargs=["boto3_session", "s3_additional_kwargs"],
)
def to_iceberg(
    df: pd.DataFrame,
    database: str,
    table: str,
    temp_path: Optional[str] = None,
    index: bool = False,
    table_location: Optional[str] = None,
    partition_cols: Optional[List[str]] = None,
    keep_files: bool = True,
    data_source: Optional[str] = None,
    workgroup: Optional[str] = None,
    encryption: Optional[str] = None,
    kms_key: Optional[str] = None,
    boto3_session: Optional[boto3.Session] = None,
    s3_additional_kwargs: Optional[Dict[str, Any]] = None,
    additional_table_properties: Optional[Dict[str, Any]] = None,
    dtype: Optional[Dict[str, str]] = None,
    catalog_id: Optional[str] = None,
    schema_evolution: bool = False,
    glue_table_settings: Optional[GlueTableSettings] = None,
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
    keep_files : bool
        Whether staging files produced by Athena are retained. 'True' by default.
    data_source : str, optional
        Data Source / Catalog name. If None, 'AwsDataCatalog' will be used by default.
    workgroup : str, optional
        Athena workgroup.
    encryption : str, optional
        Valid values: [None, 'SSE_S3', 'SSE_KMS']. Notice: 'CSE_KMS' is not supported.
    kms_key : str, optional
        For SSE-KMS, this is the KMS key ARN or ID.
    boto3_session : boto3.Session(), optional
        Boto3 Session. The default boto3 session will be used if boto3_session receive None.
    s3_additional_kwargs : Optional[Dict[str, Any]]
        Forwarded to botocore requests.
        e.g. s3_additional_kwargs={'RequestPayer': 'requester'}
    additional_table_properties : Optional[Dict[str, Any]]
        Additional table properties.
        e.g. additional_table_properties={'write_target_data_file_size_bytes': '536870912'}

        https://docs.aws.amazon.com/athena/latest/ug/querying-iceberg-creating-tables.html#querying-iceberg-table-properties
    dtype: Optional[Dict[str, str]]
        Dictionary of columns names and Athena/Glue types to be casted.
        Useful when you have columns with undetermined or mixed data types.
        e.g. {'col name': 'bigint', 'col2 name': 'int'}
    catalog_id : str, optional
        The ID of the Data Catalog from which to retrieve Databases.
        If none is provided, the AWS account ID is used by default
    schema_evolution: bool
        If True allows schema evolution for new columns or changes in column types.
    columns_comments: Optional[GlueTableSettings]
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
    if df.empty is True:
        raise exceptions.EmptyDataFrame("DataFrame cannot be empty.")

    wg_config: _WorkGroupConfig = _get_workgroup_config(session=boto3_session, workgroup=workgroup)
    temp_table: str = f"temp_table_{uuid.uuid4().hex}"

    if not temp_path and not wg_config.s3_output:
        raise exceptions.InvalidArgumentCombination(
            "Either path or workgroup path must be specified to store the temporary results."
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
                path=table_location,  # type: ignore[arg-type]
                wg_config=wg_config,
                partition_cols=partition_cols,
                additional_table_properties=additional_table_properties,
                index=index,
                data_source=data_source,
                workgroup=workgroup,
                encryption=encryption,
                kms_key=kms_key,
                boto3_session=boto3_session,
                dtype=dtype,
                columns_comments=glue_table_settings.get("columns_comments"),
            )
        else:
            schema_differences = _determine_differences(
                df=df,
                database=database,
                table=table,
                index=index,
                partition_cols=partition_cols,
                boto3_session=boto3_session,
                dtype=dtype,
                catalog_id=catalog_id,
            )
            if schema_evolution is False and any([schema_differences[x] for x in schema_differences]):  # type: ignore[literal-required]
                raise exceptions.InvalidArgumentValue(f"Schema change detected: {schema_differences}")

            _alter_iceberg_table(
                database=database,
                table=table,
                schema_changes=schema_differences,
                wg_config=wg_config,
                data_source=data_source,
                workgroup=workgroup,
                encryption=encryption,
                kms_key=kms_key,
                boto3_session=boto3_session,
            )

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

        # Insert into iceberg table
        query_execution_id: str = _start_query_execution(
            sql=f'INSERT INTO "{database}"."{table}" SELECT * FROM "{database}"."{temp_table}"',
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
