"""AWS Glue Catalog Module."""

import logging
from typing import Any, Dict, Optional

import boto3  # type: ignore

from awswrangler import _utils, athena

logger: logging.Logger = logging.getLogger(__name__)


def delete_table_if_exists(database, name, boto3_session: Optional[boto3.Session] = None):
    """Delete Glue table if exists.

    Parameters
    ----------
    database : str
        Database name.
    name : str
        Table name.
    boto3_session : boto3.Session(), optional
        Boto3 Session. The default boto3 session will be used if boto3_session receive None.

    Returns
    -------
    None
        None.

    Examples
    --------
    >>> import awswrangler as wr
    >>> wr.catalog.delete_table_if_exists(database="default", name="my_table")

    """
    client_glue: boto3.client = _utils.client(service_name="glue", session=boto3_session)
    try:
        client_glue.delete_table(DatabaseName=database, Name=name)
    except client_glue.exceptions.EntityNotFoundException:
        pass


def does_table_exists(database, name, boto3_session: Optional[boto3.Session] = None):
    """Check if the table exists.

    Parameters
    ----------
    database : str
        Database name.
    name : str
        Table name.
    boto3_session : boto3.Session(), optional
        Boto3 Session. The default boto3 session will be used if boto3_session receive None.

    Returns
    -------
    bool
        True if exists, otherwise False.

    Examples
    --------
    >>> import awswrangler as wr
    >>> wr.catalog.does_table_exists(database="default", name="my_table")

    """
    client_glue: boto3.client = _utils.client(service_name="glue", session=boto3_session)
    try:
        client_glue.get_table(DatabaseName=database, Name=name)
        return True
    except client_glue.exceptions.EntityNotFoundException:
        return False


def create_parquet_table(
    database: str,
    name: str,
    path: str,
    columns_types: Dict[str, str],
    partitions_types: Optional[Dict[str, str]],
    compression: Optional[str] = None,
    description: Optional[str] = None,
    parameters: Optional[Dict[str, str]] = None,
    columns_comments: Optional[Dict[str, str]] = None,
    boto3_session: Optional[boto3.Session] = None,
) -> None:
    """Create a Parquet Table (Metadata Only) in the AWS Glue Catalog.

    https://docs.aws.amazon.com/athena/latest/ug/data-types.html

    Parameters
    ----------
    database : str
        Database name.
    name : str
        Table name.
    path : str
        Amazon S3 path (e.g. s3://bucket/prefix/).
    columns_types: Dict[str, str]
        Dictionary with keys as column names and vales as data types (e.g. {"col0": "bigint", "col1": "double"}).
    partitions_types: Dict[str, str], optional
        Dictionary with keys as partition names and vales as data types (e.g. {"col2": "date"}).
    compression: str, optional
        Compression style (``None``, ``snappy``, ``gzip``, etc).
    description: str, optional
        Table description
    parameters: Dict[str, str], optional
        Key/value pairs to tag the table.
    columns_comments: Dict[str, str], optional
        Columns names and the related comments (e.g. {"col0": "Column 0.", "col1": "Column 1.", "col2": "Partition."}).
    boto3_session : boto3.Session(), optional
        Boto3 Session. The default boto3 session will be used if boto3_session receive None.

    Returns
    -------
    None
        None.

    Examples
    --------
    >>> import awswrangler as wr
    >>> wr.catalog.create_parquet_table(
    ...     database="default",
    ...     name="my_table",
    ...     path="s3://bucket/prefix/",
    ...     columns_types={"col0": "bigint", "col1": "double"},
    ...     partitions_types={"col2": "date"},
    ...     compression="snappy",
    ...     description="My own table!",
    ...     parameters={"source": "postgresql"},
    ...     columns_comments={"col0": "Column 0.", "col1": "Column 1.", "col2": "Partition."}
    ... )

    """
    name = athena.normalize_table_name(name=name)
    partitions_types = {} if partitions_types is None else partitions_types
    table_input: Dict[str, Any] = _parquet_table_definition(
        name=name, path=path, columns_types=columns_types, partitions_types=partitions_types, compression=compression
    )
    if description is not None:
        table_input["Description"] = description
    if parameters is not None:
        for k, v in parameters.items():
            table_input["Parameters"][k] = v
    if columns_comments is not None:
        for col in table_input["StorageDescriptor"]["Columns"]:
            name = col["Name"]
            if name in columns_comments:
                col["Comment"] = columns_comments[name]
        for par in table_input["PartitionKeys"]:
            name = par["Name"]
            if name in columns_comments:
                par["Comment"] = columns_comments[name]
    client_glue: boto3.client = _utils.client(service_name="glue", session=boto3_session)
    client_glue.create_table(DatabaseName=database, TableInput=table_input)


def _parquet_table_definition(
    name: str, path: str, columns_types: Dict[str, str], partitions_types: Dict[str, str], compression: Optional[str]
) -> Dict[str, Any]:
    compressed: bool = compression is not None
    return {
        "Name": name,
        "PartitionKeys": [{"Name": cname, "Type": dtype} for cname, dtype in partitions_types.items()],
        "TableType": "EXTERNAL_TABLE",
        "Parameters": {"classification": "parquet", "compressionType": str(compression).lower(), "typeOfData": "file"},
        "StorageDescriptor": {
            "Columns": [{"Name": cname, "Type": dtype} for cname, dtype in columns_types.items()],
            "Location": path,
            "InputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
            "OutputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
            "Compressed": compressed,
            "NumberOfBuckets": -1,
            "SerdeInfo": {
                "SerializationLibrary": "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
                "Parameters": {"serialization.format": "1"},
            },
            "StoredAsSubDirectories": False,
            "SortColumns": [],
            "Parameters": {
                "CrawlerSchemaDeserializerVersion": "1.0",
                "classification": "parquet",
                "compressionType": str(compression).lower(),
                "typeOfData": "file",
            },
        },
    }
