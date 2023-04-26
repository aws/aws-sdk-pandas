"""Amazon Athena Module containing all to_* write functions."""

import logging
import uuid
from typing import Any, Dict, Optional

import boto3
import pandas as pd

from awswrangler import _utils, catalog, exceptions, s3
from awswrangler._config import apply_configs
from awswrangler.athena._utils import (
    _get_workgroup_config,
    _start_query_execution,
    _WorkGroupConfig,
    wait_query,
)

_logger: logging.Logger = logging.getLogger(__name__)


@apply_configs
@_utils.validate_distributed_kwargs(
    unsupported_kwargs=["boto3_session", "s3_additional_kwargs"],
)
def insert_iceberg(
    df: pd.DataFrame,
    database: str,
    table: str,
    path: str,
    keep_files: bool = True,
    data_source: Optional[str] = None,
    workgroup: Optional[str] = None,
    encryption: Optional[str] = None,
    kms_key: Optional[str] = None,
    boto3_session: Optional[boto3.Session] = None,
    s3_additional_kwargs: Optional[Dict[str, Any]] = None,
) -> None:
    """
    Insert into Athena Iceberg table using INSERT INTO ... SELECT.

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
    path : str
        Amazon S3 path for temporary results.
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

    """
    if df.empty is True:
        raise exceptions.EmptyDataFrame("DataFrame cannot be empty.")

    wg_config: _WorkGroupConfig = _get_workgroup_config(session=boto3_session, workgroup=workgroup)
    temp_table: str = f"temp_table_{uuid.uuid4().hex}"

    try:
        # Create temporary external table, write the results
        s3.to_parquet(
            df=df,
            path=path,
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
                path=path,
                boto3_session=boto3_session,
                s3_additional_kwargs=s3_additional_kwargs,
            )
