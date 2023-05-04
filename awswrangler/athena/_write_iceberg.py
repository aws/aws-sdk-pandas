"""Amazon Athena Module containing all to_* write functions."""

import logging
import uuid
from typing import Any, Dict, Optional

import boto3
import pandas as pd

from awswrangler import _utils, catalog, exceptions, s3
from awswrangler._config import apply_configs
from awswrangler.athena._executions import wait_query
from awswrangler.athena._utils import (
    _get_workgroup_config,
    _start_query_execution,
    _WorkGroupConfig,
)

_logger: logging.Logger = logging.getLogger(__name__)


def _create_iceberg_table(
    df: pd.DataFrame,
    database: str,
    table: str,
    path: str,
    wg_config: _WorkGroupConfig,
    index: bool = False,
    data_source: Optional[str] = None,
    workgroup: Optional[str] = None,
    encryption: Optional[str] = None,
    kms_key: Optional[str] = None,
    boto3_session: Optional[boto3.Session] = None,
) -> None:
    if not path:
        raise exceptions.InvalidArgumentValue("Must specify table location to create the table.")

    columns_types, _ = catalog.extract_athena_types(df=df, index=index)
    cols_str: str = ", ".join([f"{k} {v}" for k, v in columns_types.items()])

    create_sql: str = (
        f"CREATE TABLE IF NOT EXISTS {table} ({cols_str}) "
        f"LOCATION '{path}' "
        f"TBLPROPERTIES ( 'table_type' ='ICEBERG', 'format'='parquet' )"
    )

    query_id: str = _start_query_execution(
        sql=create_sql,
        workgroup=workgroup,
        wg_config=wg_config,
        database=database,
        data_source=data_source,
        encryption=encryption,
        kms_key=kms_key,
        boto3_session=boto3_session,
    )
    wait_query(query_id)


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
    keep_files: bool = True,
    data_source: Optional[str] = None,
    workgroup: Optional[str] = None,
    encryption: Optional[str] = None,
    kms_key: Optional[str] = None,
    boto3_session: Optional[boto3.Session] = None,
    s3_additional_kwargs: Optional[Dict[str, Any]] = None,
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

    try:
        # Create Iceberg table if it doesn't exist
        if not catalog.does_table_exist(database=database, table=table, boto3_session=boto3_session):
            _create_iceberg_table(
                df=df,
                database=database,
                table=table,
                path=table_location,  # type: ignore[arg-type]
                wg_config=wg_config,
                index=index,
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
        )

        # Insert into iceberg table
        query_id: str = _start_query_execution(
            sql=f'INSERT INTO "{database}"."{table}" SELECT * FROM "{database}"."{temp_table}"',
            workgroup=workgroup,
            wg_config=wg_config,
            database=database,
            data_source=data_source,
            encryption=encryption,
            kms_key=kms_key,
            boto3_session=boto3_session,
        )
        wait_query(query_id)

    except Exception as ex:
        _logger.error(ex)

        raise
    finally:
        catalog.delete_table_if_exists(database=database, table=temp_table, boto3_session=boto3_session)

        if keep_files is False:
            s3.delete_objects(
                path=temp_path or wg_config.s3_output,  # type: ignore[arg-type]
                boto3_session=boto3_session,
                s3_additional_kwargs=s3_additional_kwargs,
            )
