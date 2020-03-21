"""AWS Glue Catalog Module."""

import itertools
import logging
from typing import Any, Dict, Iterator, List, Optional

import boto3  # type: ignore
import pandas as pd  # type: ignore

from awswrangler import _utils, athena, exceptions

logger: logging.Logger = logging.getLogger(__name__)


def delete_table_if_exists(database: str, table: str, boto3_session: Optional[boto3.Session] = None):
    """Delete Glue table if exists.

    Parameters
    ----------
    database : str
        Database name.
    table : str
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
        client_glue.delete_table(DatabaseName=database, Name=table)
    except client_glue.exceptions.EntityNotFoundException:
        pass


def does_table_exist(database: str, table: str, boto3_session: Optional[boto3.Session] = None):
    """Check if the table exists.

    Parameters
    ----------
    database : str
        Database name.
    table : str
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
    >>> wr.catalog.does_table_exist(database="default", name="my_table")

    """
    client_glue: boto3.client = _utils.client(service_name="glue", session=boto3_session)
    try:
        client_glue.get_table(DatabaseName=database, Name=table)
        return True
    except client_glue.exceptions.EntityNotFoundException:
        return False


def create_parquet_table(
    database: str,
    table: str,
    path: str,
    columns_types: Dict[str, str],
    partitions_types: Optional[Dict[str, str]],
    compression: Optional[str] = None,
    description: Optional[str] = None,
    parameters: Optional[Dict[str, str]] = None,
    columns_comments: Optional[Dict[str, str]] = None,
    mode: str = "overwrite",
    boto3_session: Optional[boto3.Session] = None,
) -> None:
    """Create a Parquet Table (Metadata Only) in the AWS Glue Catalog.

    https://docs.aws.amazon.com/athena/latest/ug/data-types.html

    Parameters
    ----------
    database : str
        Database name.
    table : str
        Table name.
    path : str
        Amazon S3 path (e.g. s3://bucket/prefix/).
    columns_types: Dict[str, str]
        Dictionary with keys as column names and vales as data types (e.g. {"col0": "bigint", "col1": "double"}).
    partitions_types: Dict[str, str], optional
        Dictionary with keys as partition names and values as data types (e.g. {"col2": "date"}).
    compression: str, optional
        Compression style (``None``, ``snappy``, ``gzip``, etc).
    description: str, optional
        Table description
    parameters: Dict[str, str], optional
        Key/value pairs to tag the table.
    columns_comments: Dict[str, str], optional
        Columns names and the related comments (e.g. {"col0": "Column 0.", "col1": "Column 1.", "col2": "Partition."}).
    mode: str
        Only "overwrite" available by now.
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
    ...     table="my_table",
    ...     path="s3://bucket/prefix/",
    ...     columns_types={"col0": "bigint", "col1": "double"},
    ...     partitions_types={"col2": "date"},
    ...     compression="snappy",
    ...     description="My own table!",
    ...     parameters={"source": "postgresql"},
    ...     columns_comments={"col0": "Column 0.", "col1": "Column 1.", "col2": "Partition."}
    ... )

    """
    table = athena.normalize_table_name(table=table)
    partitions_types = {} if partitions_types is None else partitions_types
    table_input: Dict[str, Any] = _parquet_table_definition(
        table=table, path=path, columns_types=columns_types, partitions_types=partitions_types, compression=compression
    )
    if description is not None:
        table_input["Description"] = description
    if parameters is not None:
        for k, v in parameters.items():
            table_input["Parameters"][k] = v
    if columns_comments is not None:
        for col in table_input["StorageDescriptor"]["Columns"]:
            name: str = col["Name"]
            if name in columns_comments:
                col["Comment"] = columns_comments[name]
        for par in table_input["PartitionKeys"]:
            name = par["Name"]
            if name in columns_comments:
                par["Comment"] = columns_comments[name]
    if mode == "overwrite":
        delete_table_if_exists(database=database, table=table, boto3_session=boto3_session)
    client_glue: boto3.client = _utils.client(service_name="glue", session=boto3_session)
    client_glue.create_table(DatabaseName=database, TableInput=table_input)


def _parquet_table_definition(
    table: str, path: str, columns_types: Dict[str, str], partitions_types: Dict[str, str], compression: Optional[str]
) -> Dict[str, Any]:
    compressed: bool = compression is not None
    return {
        "Name": table,
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


def add_parquet_partitions(
    database: str,
    table: str,
    partitions_values: Dict[str, List[str]],
    compression: Optional[str] = None,
    boto3_session: Optional[boto3.Session] = None,
) -> None:
    """Create a Parquet Table (Metadata Only) in the AWS Glue Catalog.

    https://docs.aws.amazon.com/athena/latest/ug/data-types.html

    Parameters
    ----------
    database : str
        Database name.
    table : str
        Table name.
    partitions_values: Dict[str, List[str]]
        Dictionary with keys as S3 path locations and values as a list of partitions values as str
        (e.g. {"s3://bucket/prefix/y=2020/m=10/": ["2020", "10"]}).
    compression: str, optional
        Compression style (``None``, ``snappy``, ``gzip``, etc).
    boto3_session : boto3.Session(), optional
        Boto3 Session. The default boto3 session will be used if boto3_session receive None.

    Returns
    -------
    None
        None.

    Examples
    --------
    >>> import awswrangler as wr
    >>> wr.catalog.add_parquet_partitions(
    ...     database="default",
    ...     table="my_table",
    ...     partitions_values={
    ...         "s3://bucket/prefix/y=2020/m=10/": ["2020", "10"]},
    ...         "s3://bucket/prefix/y=2020/m=11/": ["2020", "11"]},
    ...         "s3://bucket/prefix/y=2020/m=12/": ["2020", "12"]},
    ... )

    """
    inputs: List[Dict[str, Any]] = [
        _parquet_partition_definition(location=k, values=v, compression=compression)
        for k, v in partitions_values.items()
    ]
    chunks: List[List[Dict[str, Any]]] = _utils.chunkify(lst=inputs, max_length=100)
    client_glue: boto3.client = _utils.client(service_name="glue", session=boto3_session)
    for chunk in chunks:
        res: Dict[str, Any] = client_glue.batch_create_partition(
            DatabaseName=database, TableName=table, PartitionInputList=chunk
        )
        if ("Errors" in res) and res["Errors"]:  # pragma: no cover
            raise exceptions.ServiceApiError(str(res["Errors"]))


def _parquet_partition_definition(location: str, values: List[str], compression: Optional[str]) -> Dict[str, Any]:
    compressed: bool = compression is not None
    return {
        "StorageDescriptor": {
            "InputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
            "OutputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
            "Location": location,
            "Compressed": compressed,
            "SerdeInfo": {
                "Parameters": {"serialization.format": "1"},
                "SerializationLibrary": "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
            },
            "StoredAsSubDirectories": False,
        },
        "Values": values,
    }


def get_table_types(database: str, table: str, boto3_session: Optional[boto3.Session] = None):
    """Get all columns and types from a table.

    Parameters
    ----------
    database : str
        Database name.
    table : str
        Table name.
    boto3_session : boto3.Session(), optional
        Boto3 Session. The default boto3 session will be used if boto3_session receive None.

    Returns
    -------
    Dict[str, str]
        A dictionary as {"col name": "col dtype"}.

    Examples
    --------
    >>> import awswrangler as wr
    >>> wr.catalog.get_table_types(database="default", name="my_table")
    {"col0": "int", "col1": "double}

    """
    client_glue: boto3.client = _utils.client(service_name="glue", session=boto3_session)
    response: Dict[str, Any] = client_glue.get_table(DatabaseName=database, Name=table)
    dtypes: Dict[str, str] = {}
    for col in response["Table"]["StorageDescriptor"]["Columns"]:
        dtypes[col["Name"]] = col["Type"]
    for par in response["Table"]["PartitionKeys"]:
        dtypes[par["Name"]] = par["Type"]
    return dtypes


def get_databases(
    catalog_id: Optional[str] = None, boto3_session: Optional[boto3.Session] = None
) -> Iterator[Dict[str, Any]]:
    """Get an iterator of databases.

    Parameters
    ----------
    catalog_id : str, optional
        The ID of the Data Catalog from which to retrieve Databases.
        If none is provided, the AWS account ID is used by default.
    boto3_session : boto3.Session(), optional
        Boto3 Session. The default boto3 session will be used if boto3_session receive None.

    Returns
    -------
    Iterator[Dict[str, Any]]
        Iterator of Databases.

    Examples
    --------
    >>> import awswrangler as wr
    >>> dbs = wr.catalog.get_databases()

    """
    client_glue: boto3.client = _utils.client(service_name="glue", session=boto3_session)
    paginator = client_glue.get_paginator("get_databases")
    if catalog_id is None:
        response_iterator: Iterator = paginator.paginate()
    else:
        response_iterator = paginator.paginate(CatalogId=catalog_id)
    for page in response_iterator:
        for db in page["DatabaseList"]:
            yield db


def databases(
    limit: int = 100, catalog_id: Optional[str] = None, boto3_session: Optional[boto3.Session] = None
) -> pd.DataFrame:
    """Get a Pandas DataFrame with all listed databases.

    Parameters
    ----------
    limit : int, optional
        Max number of tables to be returned.
    catalog_id : str, optional
        The ID of the Data Catalog from which to retrieve Databases.
        If none is provided, the AWS account ID is used by default.
    boto3_session : boto3.Session(), optional
        Boto3 Session. The default boto3 session will be used if boto3_session receive None.

    Returns
    -------
    pandas.DataFrame
        Pandas Dataframe filled by formatted infos.

    Examples
    --------
    >>> import awswrangler as wr
    >>> df_dbs = wr.catalog.databases()

    """
    database_iter: Iterator[Dict[str, Any]] = get_databases(catalog_id=catalog_id, boto3_session=boto3_session)
    dbs = itertools.islice(database_iter, limit)
    df_dict: Dict[str, List] = {"Database": [], "Description": []}
    for db in dbs:
        df_dict["Database"].append(db["Name"])
        if "Description" in db:
            df_dict["Description"].append(db["Description"])
        else:
            df_dict["Description"].append("")
    return pd.DataFrame(data=df_dict)
