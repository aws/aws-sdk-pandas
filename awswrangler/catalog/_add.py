"""AWS Glue Catalog Delete Module."""

import logging
from typing import Any, Dict, List, Optional, Tuple

import boto3

from awswrangler import _utils, exceptions
from awswrangler._config import apply_configs
from awswrangler.catalog._definitions import (
    _check_column_type,
    _csv_partition_definition,
    _parquet_partition_definition,
    _update_table_definition,
)
from awswrangler.catalog._utils import _catalog_id, sanitize_table_name

_logger: logging.Logger = logging.getLogger(__name__)


def _add_partitions(
    database: str,
    table: str,
    boto3_session: Optional[boto3.Session],
    inputs: List[Dict[str, Any]],
    catalog_id: Optional[str] = None,
) -> None:
    chunks: List[List[Dict[str, Any]]] = _utils.chunkify(lst=inputs, max_length=100)
    client_glue: boto3.client = _utils.client(service_name="glue", session=boto3_session)
    for chunk in chunks:  # pylint: disable=too-many-nested-blocks
        res: Dict[str, Any] = client_glue.batch_create_partition(
            **_catalog_id(catalog_id=catalog_id, DatabaseName=database, TableName=table, PartitionInputList=chunk)
        )
        if ("Errors" in res) and res["Errors"]:
            for error in res["Errors"]:
                if "ErrorDetail" in error:
                    if "ErrorCode" in error["ErrorDetail"]:
                        if error["ErrorDetail"]["ErrorCode"] != "AlreadyExistsException":
                            raise exceptions.ServiceApiError(str(res["Errors"]))


@apply_configs
def add_csv_partitions(
    database: str,
    table: str,
    partitions_values: Dict[str, List[str]],
    bucketing_info: Optional[Tuple[List[str], int]] = None,
    catalog_id: Optional[str] = None,
    compression: Optional[str] = None,
    sep: str = ",",
    boto3_session: Optional[boto3.Session] = None,
    columns_types: Optional[Dict[str, str]] = None,
) -> None:
    """Add partitions (metadata) to a CSV Table in the AWS Glue Catalog.

    Parameters
    ----------
    database : str
        Database name.
    table : str
        Table name.
    partitions_values: Dict[str, List[str]]
        Dictionary with keys as S3 path locations and values as a list of partitions values as str
        (e.g. {'s3://bucket/prefix/y=2020/m=10/': ['2020', '10']}).
    bucketing_info: Tuple[List[str], int], optional
        Tuple consisting of the column names used for bucketing as the first element and the number of buckets as the
        second element.
        Only `str`, `int` and `bool` are supported as column data types for bucketing.
    catalog_id : str, optional
        The ID of the Data Catalog from which to retrieve Databases.
        If none is provided, the AWS account ID is used by default.
    compression: str, optional
        Compression style (``None``, ``gzip``, etc).
    sep : str
        String of length 1. Field delimiter for the output file.
    boto3_session : boto3.Session(), optional
        Boto3 Session. The default boto3 session will be used if boto3_session receive None.
    columns_types: Optional[Dict[str, str]]
        Only required for Hive compability.
        Dictionary with keys as column names and values as data types (e.g. {'col0': 'bigint', 'col1': 'double'}).
        P.S. Only materialized columns please, not partition columns.

    Returns
    -------
    None
        None.

    Examples
    --------
    >>> import awswrangler as wr
    >>> wr.catalog.add_csv_partitions(
    ...     database='default',
    ...     table='my_table',
    ...     partitions_values={
    ...         's3://bucket/prefix/y=2020/m=10/': ['2020', '10'],
    ...         's3://bucket/prefix/y=2020/m=11/': ['2020', '11'],
    ...         's3://bucket/prefix/y=2020/m=12/': ['2020', '12']
    ...     }
    ... )

    """
    inputs: List[Dict[str, Any]] = [
        _csv_partition_definition(
            location=k,
            values=v,
            bucketing_info=bucketing_info,
            compression=compression,
            sep=sep,
            columns_types=columns_types,
        )
        for k, v in partitions_values.items()
    ]
    _add_partitions(database=database, table=table, boto3_session=boto3_session, inputs=inputs, catalog_id=catalog_id)


@apply_configs
def add_parquet_partitions(
    database: str,
    table: str,
    partitions_values: Dict[str, List[str]],
    bucketing_info: Optional[Tuple[List[str], int]] = None,
    catalog_id: Optional[str] = None,
    compression: Optional[str] = None,
    boto3_session: Optional[boto3.Session] = None,
    columns_types: Optional[Dict[str, str]] = None,
) -> None:
    """Add partitions (metadata) to a Parquet Table in the AWS Glue Catalog.

    Parameters
    ----------
    database : str
        Database name.
    table : str
        Table name.
    partitions_values: Dict[str, List[str]]
        Dictionary with keys as S3 path locations and values as a list of partitions values as str
        (e.g. {'s3://bucket/prefix/y=2020/m=10/': ['2020', '10']}).
    bucketing_info: Tuple[List[str], int], optional
        Tuple consisting of the column names used for bucketing as the first element and the number of buckets as the
        second element.
        Only `str`, `int` and `bool` are supported as column data types for bucketing.
    catalog_id : str, optional
        The ID of the Data Catalog from which to retrieve Databases.
        If none is provided, the AWS account ID is used by default.
    compression: str, optional
        Compression style (``None``, ``snappy``, ``gzip``, etc).
    boto3_session : boto3.Session(), optional
        Boto3 Session. The default boto3 session will be used if boto3_session receive None.
    columns_types: Optional[Dict[str, str]]
        Only required for Hive compability.
        Dictionary with keys as column names and values as data types (e.g. {'col0': 'bigint', 'col1': 'double'}).
        P.S. Only materialized columns please, not partition columns.

    Returns
    -------
    None
        None.

    Examples
    --------
    >>> import awswrangler as wr
    >>> wr.catalog.add_parquet_partitions(
    ...     database='default',
    ...     table='my_table',
    ...     partitions_values={
    ...         's3://bucket/prefix/y=2020/m=10/': ['2020', '10'],
    ...         's3://bucket/prefix/y=2020/m=11/': ['2020', '11'],
    ...         's3://bucket/prefix/y=2020/m=12/': ['2020', '12']
    ...     }
    ... )

    """
    table = sanitize_table_name(table=table)
    if partitions_values:
        inputs: List[Dict[str, Any]] = [
            _parquet_partition_definition(
                location=k,
                values=v,
                bucketing_info=bucketing_info,
                compression=compression,
                columns_types=columns_types,
            )
            for k, v in partitions_values.items()
        ]
        _add_partitions(
            database=database, table=table, boto3_session=boto3_session, inputs=inputs, catalog_id=catalog_id
        )


@apply_configs
def add_column(
    database: str,
    table: str,
    column_name: str,
    column_type: str = "string",
    column_comment: Optional[str] = None,
    boto3_session: Optional[boto3.Session] = None,
    catalog_id: Optional[str] = None,
) -> None:
    """Add a column in a AWS Glue Catalog table.

    Parameters
    ----------
    database : str
        Database name.
    table : str
        Table name.
    column_name : str
        Column name
    column_type : str
        Column type.
    column_comment : str
        Column Comment
    boto3_session : boto3.Session(), optional
        Boto3 Session. The default boto3 session will be used if boto3_session receive None.
    catalog_id : str, optional
        The ID of the Data Catalog from which to retrieve Databases.
        If none is provided, the AWS account ID is used by default.

    Returns
    -------
    None
        None

    Examples
    --------
    >>> import awswrangler as wr
    >>> wr.catalog.add_column(
    ...     database='my_db',
    ...     table='my_table',
    ...     column_name='my_col',
    ...     column_type='int'
    ... )
    """
    if _check_column_type(column_type):
        client_glue: boto3.client = _utils.client(service_name="glue", session=boto3_session)
        table_res: Dict[str, Any] = client_glue.get_table(DatabaseName=database, Name=table)
        table_input: Dict[str, Any] = _update_table_definition(table_res)
        table_input["StorageDescriptor"]["Columns"].append(
            {"Name": column_name, "Type": column_type, "Comment": column_comment}
        )
        res: Dict[str, Any] = client_glue.update_table(
            **_catalog_id(catalog_id=catalog_id, DatabaseName=database, TableInput=table_input)
        )
        if ("Errors" in res) and res["Errors"]:
            for error in res["Errors"]:
                if "ErrorDetail" in error:
                    if "ErrorCode" in error["ErrorDetail"]:
                        raise exceptions.ServiceApiError(str(res["Errors"]))
