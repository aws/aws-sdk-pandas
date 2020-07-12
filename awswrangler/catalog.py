"""AWS Glue Catalog Module."""
# pylint: disable=redefined-outer-name

import itertools
import logging
import re
import unicodedata
from typing import Any, Dict, Iterator, List, Optional, Tuple, Union
from urllib.parse import quote_plus as _quote_plus

import boto3  # type: ignore
import pandas as pd  # type: ignore
import sqlalchemy  # type: ignore

from awswrangler import _data_types, _utils, exceptions

_logger: logging.Logger = logging.getLogger(__name__)


def create_database(
    name: str,
    description: Optional[str] = None,
    catalog_id: Optional[str] = None,
    boto3_session: Optional[boto3.Session] = None,
) -> None:
    """Create a database in AWS Glue Catalog.

    Parameters
    ----------
    name : str
        Database name.
    description : str, optional
        A Descrption for the Database.
    catalog_id : str, optional
        The ID of the Data Catalog from which to retrieve Databases.
        If none is provided, the AWS account ID is used by default.
    boto3_session : boto3.Session(), optional
        Boto3 Session. The default boto3 session will be used if boto3_session receive None.

    Returns
    -------
    None
        None.

    Examples
    --------
    >>> import awswrangler as wr
    >>> wr.catalog.create_database(
    ...     name='awswrangler_test'
    ... )
    """
    args: Dict[str, str] = {}
    client_glue: boto3.client = _utils.client(service_name="glue", session=boto3_session)
    args["Name"] = name
    if description is not None:
        args["Description"] = description

    if catalog_id is not None:
        client_glue.create_database(CatalogId=catalog_id, DatabaseInput=args)  # pragma: no cover
    else:
        client_glue.create_database(DatabaseInput=args)


def delete_database(name: str, catalog_id: Optional[str] = None, boto3_session: Optional[boto3.Session] = None) -> None:
    """Create a database in AWS Glue Catalog.

    Parameters
    ----------
    name : str
        Database name.
    catalog_id : str, optional
        The ID of the Data Catalog from which to retrieve Databases.
        If none is provided, the AWS account ID is used by default.
    boto3_session : boto3.Session(), optional
        Boto3 Session. The default boto3 session will be used if boto3_session receive None.

    Returns
    -------
    None
        None.

    Examples
    --------
    >>> import awswrangler as wr
    >>> wr.catalog.delete_database(
    ...     name='awswrangler_test'
    ... )
    """
    client_glue: boto3.client = _utils.client(service_name="glue", session=boto3_session)

    if catalog_id is not None:
        client_glue.delete_database(CatalogId=catalog_id, Name=name)  # pragma: no cover
    else:
        client_glue.delete_database(Name=name)


def delete_table_if_exists(database: str, table: str, boto3_session: Optional[boto3.Session] = None) -> bool:
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
    bool
        True if deleted, otherwise False.

    Examples
    --------
    >>> import awswrangler as wr
    >>> wr.catalog.delete_table_if_exists(database='default', name='my_table')  # deleted
    True
    >>> wr.catalog.delete_table_if_exists(database='default', name='my_table')  # Nothing to be deleted
    False

    """
    client_glue: boto3.client = _utils.client(service_name="glue", session=boto3_session)
    try:
        client_glue.delete_table(DatabaseName=database, Name=table)
        return True
    except client_glue.exceptions.EntityNotFoundException:
        return False


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
    >>> wr.catalog.does_table_exist(database='default', name='my_table')

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
    partitions_types: Optional[Dict[str, str]] = None,
    compression: Optional[str] = None,
    description: Optional[str] = None,
    parameters: Optional[Dict[str, str]] = None,
    columns_comments: Optional[Dict[str, str]] = None,
    mode: str = "overwrite",
    catalog_versioning: bool = False,
    projection_enabled: bool = False,
    projection_types: Optional[Dict[str, str]] = None,
    projection_ranges: Optional[Dict[str, str]] = None,
    projection_values: Optional[Dict[str, str]] = None,
    projection_intervals: Optional[Dict[str, str]] = None,
    projection_digits: Optional[Dict[str, str]] = None,
    boto3_session: Optional[boto3.Session] = None,
) -> None:
    """Create a Parquet Table (Metadata Only) in the AWS Glue Catalog.

    'https://docs.aws.amazon.com/athena/latest/ug/data-types.html'

    Parameters
    ----------
    database : str
        Database name.
    table : str
        Table name.
    path : str
        Amazon S3 path (e.g. s3://bucket/prefix/).
    columns_types: Dict[str, str]
        Dictionary with keys as column names and vales as data types (e.g. {'col0': 'bigint', 'col1': 'double'}).
    partitions_types: Dict[str, str], optional
        Dictionary with keys as partition names and values as data types (e.g. {'col2': 'date'}).
    compression: str, optional
        Compression style (``None``, ``snappy``, ``gzip``, etc).
    description: str, optional
        Table description
    parameters: Dict[str, str], optional
        Key/value pairs to tag the table.
    columns_comments: Dict[str, str], optional
        Columns names and the related comments (e.g. {'col0': 'Column 0.', 'col1': 'Column 1.', 'col2': 'Partition.'}).
    mode: str
        'overwrite' to recreate any possible existing table or 'append' to keep any possible existing table.
    catalog_versioning : bool
        If True and `mode="overwrite"`, creates an archived version of the table catalog before updating it.
    projection_enabled : bool
        Enable Partition Projection on Athena (https://docs.aws.amazon.com/athena/latest/ug/partition-projection.html)
    projection_types : Optional[Dict[str, str]]
        Dictionary of partitions names and Athena projections types.
        Valid types: "enum", "integer", "date", "injected"
        https://docs.aws.amazon.com/athena/latest/ug/partition-projection-supported-types.html
        (e.g. {'col_name': 'enum', 'col2_name': 'integer'})
    projection_ranges: Optional[Dict[str, str]]
        Dictionary of partitions names and Athena projections ranges.
        https://docs.aws.amazon.com/athena/latest/ug/partition-projection-supported-types.html
        (e.g. {'col_name': '0,10', 'col2_name': '-1,8675309'})
    projection_values: Optional[Dict[str, str]]
        Dictionary of partitions names and Athena projections values.
        https://docs.aws.amazon.com/athena/latest/ug/partition-projection-supported-types.html
        (e.g. {'col_name': 'A,B,Unknown', 'col2_name': 'foo,boo,bar'})
    projection_intervals: Optional[Dict[str, str]]
        Dictionary of partitions names and Athena projections intervals.
        https://docs.aws.amazon.com/athena/latest/ug/partition-projection-supported-types.html
        (e.g. {'col_name': '1', 'col2_name': '5'})
    projection_digits: Optional[Dict[str, str]]
        Dictionary of partitions names and Athena projections digits.
        https://docs.aws.amazon.com/athena/latest/ug/partition-projection-supported-types.html
        (e.g. {'col_name': '1', 'col2_name': '2'})
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
    ...     database='default',
    ...     table='my_table',
    ...     path='s3://bucket/prefix/',
    ...     columns_types={'col0': 'bigint', 'col1': 'double'},
    ...     partitions_types={'col2': 'date'},
    ...     compression='snappy',
    ...     description='My own table!',
    ...     parameters={'source': 'postgresql'},
    ...     columns_comments={'col0': 'Column 0.', 'col1': 'Column 1.', 'col2': 'Partition.'}
    ... )

    """
    table = sanitize_table_name(table=table)
    partitions_types = {} if partitions_types is None else partitions_types

    session: boto3.Session = _utils.ensure_session(session=boto3_session)
    cat_table_input: Optional[Dict[str, Any]] = _get_table_input(database=database, table=table, boto3_session=session)
    _logger.debug("cat_table_input: %s", cat_table_input)
    table_input: Dict[str, Any]
    if (cat_table_input is not None) and (mode in ("append", "overwrite_partitions")):
        table_input = cat_table_input
        updated: bool = False
        cat_cols: Dict[str, str] = {x["Name"]: x["Type"] for x in table_input["StorageDescriptor"]["Columns"]}
        for c, t in columns_types.items():
            if c not in cat_cols:
                _logger.debug("New column %s with type %s.", c, t)
                table_input["StorageDescriptor"]["Columns"].append({"Name": c, "Type": t})
                updated = True
            elif t != cat_cols[c]:  # Data type change detected!
                raise exceptions.InvalidArgumentValue(
                    f"Data type change detected on column {c}. Old type: {cat_cols[c]}. New type {t}."
                )
        if updated is True:
            mode = "update"
    else:
        table_input = _parquet_table_definition(
            table=table,
            path=path,
            columns_types=columns_types,
            partitions_types=partitions_types,
            compression=compression,
        )
    table_exist: bool = cat_table_input is not None
    _logger.debug("table_exist: %s", table_exist)
    _create_table(
        database=database,
        table=table,
        description=description,
        parameters=parameters,
        columns_comments=columns_comments,
        mode=mode,
        catalog_versioning=catalog_versioning,
        boto3_session=session,
        table_input=table_input,
        table_exist=table_exist,
        partitions_types=partitions_types,
        projection_enabled=projection_enabled,
        projection_types=projection_types,
        projection_ranges=projection_ranges,
        projection_values=projection_values,
        projection_intervals=projection_intervals,
        projection_digits=projection_digits,
    )


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
    ...     database='default',
    ...     table='my_table',
    ...     partitions_values={
    ...         's3://bucket/prefix/y=2020/m=10/': ['2020', '10'],
    ...         's3://bucket/prefix/y=2020/m=11/': ['2020', '11'],
    ...         's3://bucket/prefix/y=2020/m=12/': ['2020', '12']
    ...     }
    ... )

    """
    if partitions_values:
        inputs: List[Dict[str, Any]] = [
            _parquet_partition_definition(location=k, values=v, compression=compression)
            for k, v in partitions_values.items()
        ]
        _add_partitions(database=database, table=table, boto3_session=boto3_session, inputs=inputs)


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


def get_table_types(
    database: str, table: str, boto3_session: Optional[boto3.Session] = None
) -> Optional[Dict[str, str]]:
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
    Optional[Dict[str, str]]
        If table exists, a dictionary like {'col name': 'col data type'}. Otherwise None.

    Examples
    --------
    >>> import awswrangler as wr
    >>> wr.catalog.get_table_types(database='default', name='my_table')
    {'col0': 'int', 'col1': double}

    """
    client_glue: boto3.client = _utils.client(service_name="glue", session=boto3_session)
    try:
        response: Dict[str, Any] = client_glue.get_table(DatabaseName=database, Name=table)
    except client_glue.exceptions.EntityNotFoundException:
        return None
    dtypes: Dict[str, str] = {}
    for col in response["Table"]["StorageDescriptor"]["Columns"]:
        dtypes[col["Name"]] = col["Type"]
    if "PartitionKeys" in response["Table"]:
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
        Pandas DataFrame filled by formatted infos.

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
        df_dict["Description"].append(db.get("Description", ""))
    return pd.DataFrame(data=df_dict)


def get_tables(
    catalog_id: Optional[str] = None,
    database: Optional[str] = None,
    name_contains: Optional[str] = None,
    name_prefix: Optional[str] = None,
    name_suffix: Optional[str] = None,
    boto3_session: Optional[boto3.Session] = None,
) -> Iterator[Dict[str, Any]]:
    """Get an iterator of tables.

    Note
    ----
    Please, does not filter using name_contains and name_prefix/name_suffix at the same time.
    Only name_prefix and name_suffix can be combined together.

    Parameters
    ----------
    catalog_id : str, optional
        The ID of the Data Catalog from which to retrieve Databases.
        If none is provided, the AWS account ID is used by default.
    database : str, optional
        Database name.
    name_contains : str, optional
        Select by a specific string on table name
    name_prefix : str, optional
        Select by a specific prefix on table name
    name_suffix : str, optional
        Select by a specific suffix on table name
    boto3_session : boto3.Session(), optional
        Boto3 Session. The default boto3 session will be used if boto3_session receive None.

    Returns
    -------
    Iterator[Dict[str, Any]]
        Iterator of tables.

    Examples
    --------
    >>> import awswrangler as wr
    >>> tables = wr.catalog.get_tables()

    """
    client_glue: boto3.client = _utils.client(service_name="glue", session=boto3_session)
    paginator = client_glue.get_paginator("get_tables")
    args: Dict[str, str] = {}
    if catalog_id is not None:
        args["CatalogId"] = catalog_id
    if (name_prefix is not None) and (name_suffix is not None) and (name_contains is not None):
        raise exceptions.InvalidArgumentCombination(
            "Please, does not filter using name_contains and "
            "name_prefix/name_suffix at the same time. Only "
            "name_prefix and name_suffix can be combined together."
        )
    if (name_prefix is not None) and (name_suffix is not None):
        args["Expression"] = f"{name_prefix}*{name_suffix}"
    elif name_contains is not None:
        args["Expression"] = f"*{name_contains}*"
    elif name_prefix is not None:
        args["Expression"] = f"{name_prefix}*"
    elif name_suffix is not None:
        args["Expression"] = f"*{name_suffix}"
    if database is not None:
        dbs: List[str] = [database]
    else:
        dbs = [x["Name"] for x in get_databases(catalog_id=catalog_id)]
    for db in dbs:
        args["DatabaseName"] = db
        response_iterator = paginator.paginate(**args)
        for page in response_iterator:
            for tbl in page["TableList"]:
                yield tbl


def tables(
    limit: int = 100,
    catalog_id: Optional[str] = None,
    database: Optional[str] = None,
    search_text: Optional[str] = None,
    name_contains: Optional[str] = None,
    name_prefix: Optional[str] = None,
    name_suffix: Optional[str] = None,
    boto3_session: Optional[boto3.Session] = None,
) -> pd.DataFrame:
    """Get a DataFrame with tables filtered by a search term, prefix, suffix.

    Parameters
    ----------
    limit : int, optional
        Max number of tables to be returned.
    catalog_id : str, optional
        The ID of the Data Catalog from which to retrieve Databases.
        If none is provided, the AWS account ID is used by default.
    database : str, optional
        Database name.
    search_text : str, optional
        Select only tables with the given string in table's properties.
    name_contains : str, optional
        Select by a specific string on table name
    name_prefix : str, optional
        Select by a specific prefix on table name
    name_suffix : str, optional
        Select by a specific suffix on table name
    boto3_session : boto3.Session(), optional
        Boto3 Session. The default boto3 session will be used if boto3_session receive None.

    Returns
    -------
    Iterator[Dict[str, Any]]
        Pandas Dataframe filled by formatted infos.

    Examples
    --------
    >>> import awswrangler as wr
    >>> df_tables = wr.catalog.tables()

    """
    if search_text is None:
        table_iter = get_tables(
            catalog_id=catalog_id,
            database=database,
            name_contains=name_contains,
            name_prefix=name_prefix,
            name_suffix=name_suffix,
            boto3_session=boto3_session,
        )
        tbls: List[Dict[str, Any]] = list(itertools.islice(table_iter, limit))
    else:
        tbls = list(search_tables(text=search_text, catalog_id=catalog_id, boto3_session=boto3_session))
        if database is not None:
            tbls = [x for x in tbls if x["DatabaseName"] == database]
        if name_contains is not None:
            tbls = [x for x in tbls if name_contains in x["Name"]]
        if name_prefix is not None:
            tbls = [x for x in tbls if x["Name"].startswith(name_prefix)]
        if name_suffix is not None:
            tbls = [x for x in tbls if x["Name"].endswith(name_suffix)]
        tbls = tbls[:limit]

    df_dict: Dict[str, List] = {"Database": [], "Table": [], "Description": [], "Columns": [], "Partitions": []}
    for tbl in tbls:
        df_dict["Database"].append(tbl["DatabaseName"])
        df_dict["Table"].append(tbl["Name"])
        df_dict["Description"].append(tbl.get("Description", ""))
        if "Columns" in tbl["StorageDescriptor"]:
            df_dict["Columns"].append(", ".join([x["Name"] for x in tbl["StorageDescriptor"]["Columns"]]))
        else:
            df_dict["Columns"].append("")  # pragma: no cover
        if "PartitionKeys" in tbl:
            df_dict["Partitions"].append(", ".join([x["Name"] for x in tbl["PartitionKeys"]]))
        else:
            df_dict["Partitions"].append("")  # pragma: no cover
    return pd.DataFrame(data=df_dict)


def search_tables(text: str, catalog_id: Optional[str] = None, boto3_session: Optional[boto3.Session] = None):
    """Get Pandas DataFrame of tables filtered by a search string.

    Parameters
    ----------
    text : str, optional
        Select only tables with the given string in table's properties.
    catalog_id : str, optional
        The ID of the Data Catalog from which to retrieve Databases.
        If none is provided, the AWS account ID is used by default.
    boto3_session : boto3.Session(), optional
        Boto3 Session. The default boto3 session will be used if boto3_session receive None.

    Returns
    -------
    Iterator[Dict[str, Any]]
        Iterator of tables.

    Examples
    --------
    >>> import awswrangler as wr
    >>> df_tables = wr.catalog.search_tables(text='my_property')

    """
    client_glue: boto3.client = _utils.client(service_name="glue", session=boto3_session)
    args: Dict[str, Any] = {"SearchText": text}
    if catalog_id is not None:
        args["CatalogId"] = catalog_id
    response: Dict[str, Any] = client_glue.search_tables(**args)
    for tbl in response["TableList"]:
        yield tbl
    while "NextToken" in response:  # pragma: no cover
        args["NextToken"] = response["NextToken"]
        response = client_glue.search_tables(**args)
        for tbl in response["TableList"]:
            yield tbl


def get_table_location(database: str, table: str, boto3_session: Optional[boto3.Session] = None) -> str:
    """Get table's location on Glue catalog.

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
    str
        Table's location.

    Examples
    --------
    >>> import awswrangler as wr
    >>> wr.catalog.get_table_location(database='default', table='my_table')
    's3://bucket/prefix/'

    """
    client_glue: boto3.client = _utils.client(service_name="glue", session=boto3_session)
    res: Dict[str, Any] = client_glue.get_table(DatabaseName=database, Name=table)
    try:
        return res["Table"]["StorageDescriptor"]["Location"]
    except KeyError:  # pragma: no cover
        raise exceptions.InvalidTable(f"{database}.{table}")


def table(
    database: str, table: str, catalog_id: Optional[str] = None, boto3_session: Optional[boto3.Session] = None
) -> pd.DataFrame:
    """Get table details as Pandas DataFrame.

    Parameters
    ----------
    database : str
        Database name.
    table : str
        Table name.
    catalog_id : str, optional
        The ID of the Data Catalog from which to retrieve Databases.
        If none is provided, the AWS account ID is used by default.
    boto3_session : boto3.Session(), optional
        Boto3 Session. The default boto3 session will be used if boto3_session receive None.

    Returns
    -------
    pandas.DataFrame
        Pandas DataFrame filled by formatted infos.

    Examples
    --------
    >>> import awswrangler as wr
    >>> df_table = wr.catalog.table(database='default', name='my_table')

    """
    client_glue: boto3.client = _utils.client(service_name="glue", session=boto3_session)
    if catalog_id is None:
        tbl: Dict[str, Any] = client_glue.get_table(DatabaseName=database, Name=table)["Table"]
    else:
        tbl = client_glue.get_table(CatalogId=catalog_id, DatabaseName=database, Name=table)["Table"]
    df_dict: Dict[str, List] = {"Column Name": [], "Type": [], "Partition": [], "Comment": []}
    for col in tbl["StorageDescriptor"]["Columns"]:
        df_dict["Column Name"].append(col["Name"])
        df_dict["Type"].append(col["Type"])
        df_dict["Partition"].append(False)
        if "Comment" in col:
            df_dict["Comment"].append(col["Comment"])
        else:
            df_dict["Comment"].append("")
    if "PartitionKeys" in tbl:
        for col in tbl["PartitionKeys"]:
            df_dict["Column Name"].append(col["Name"])
            df_dict["Type"].append(col["Type"])
            df_dict["Partition"].append(True)
            if "Comment" in col:
                df_dict["Comment"].append(col["Comment"])
            else:
                df_dict["Comment"].append("")
    return pd.DataFrame(data=df_dict)


def _sanitize_name(name: str) -> str:
    name = "".join(c for c in unicodedata.normalize("NFD", name) if unicodedata.category(c) != "Mn")  # strip accents
    name = re.sub("[^A-Za-z0-9_]+", "_", name)  # Replacing non alphanumeric characters by underscore
    return re.sub("([a-z0-9])([A-Z])", r"\1_\2", name).lower()  # Converting CamelCase to snake_case


def sanitize_column_name(column: str) -> str:
    """Convert the column name to be compatible with Amazon Athena.

    https://docs.aws.amazon.com/athena/latest/ug/tables-databases-columns-names.html

    Possible transformations:
    - Strip accents
    - Remove non alphanumeric characters
    - Convert CamelCase to snake_case

    Parameters
    ----------
    column : str
        Column name.

    Returns
    -------
    str
        Normalized column name.

    Examples
    --------
    >>> import awswrangler as wr
    >>> wr.catalog.sanitize_column_name('MyNewColumn')
    'my_new_column'

    """
    return _sanitize_name(name=column)


def sanitize_dataframe_columns_names(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize all columns names to be compatible with Amazon Athena.

    https://docs.aws.amazon.com/athena/latest/ug/tables-databases-columns-names.html

    Possible transformations:
    - Strip accents
    - Remove non alphanumeric characters
    - Convert CamelCase to snake_case

    Note
    ----
    After transformation, some column names might not be unique anymore.
    Example: the columns ["A", "a"] will be sanitized to ["a", "a"]

    Parameters
    ----------
    df : pandas.DataFrame
        Original Pandas DataFrame.

    Returns
    -------
    pandas.DataFrame
        Original Pandas DataFrame with columns names normalized.

    Examples
    --------
    >>> import awswrangler as wr
    >>> df_normalized = wr.catalog.sanitize_dataframe_columns_names(df=pd.DataFrame({'A': [1, 2]}))

    """
    df.columns = [sanitize_column_name(x) for x in df.columns]
    return df


def sanitize_table_name(table: str) -> str:
    """Convert the table name to be compatible with Amazon Athena.

    https://docs.aws.amazon.com/athena/latest/ug/tables-databases-columns-names.html

    Possible transformations:
    - Strip accents
    - Remove non alphanumeric characters
    - Convert CamelCase to snake_case

    Parameters
    ----------
    table : str
        Table name.

    Returns
    -------
    str
        Normalized table name.

    Examples
    --------
    >>> import awswrangler as wr
    >>> wr.catalog.sanitize_table_name('MyNewTable')
    'my_new_table'

    """
    return _sanitize_name(name=table)


def drop_duplicated_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Drop all repeated columns (duplicated names).

    Note
    ----
    This transformation will run `inplace` and will make changes in the original DataFrame.

    Note
    ----
    It is different from Panda's drop_duplicates() function which considers the column values.
    wr.catalog.drop_duplicated_columns() will deduplicate by column name.

    Parameters
    ----------
    df : pandas.DataFrame
        Original Pandas DataFrame.

    Returns
    -------
    pandas.DataFrame
        Pandas DataFrame without duplicated columns.

    Examples
    --------
    >>> import awswrangler as wr
    >>> df = pd.DataFrame({"A": [1, 2], "B": [3, 4]})
    >>> df.columns = ["A", "A"]
    >>> wr.catalog.drop_duplicated_columns(df=df)
       A
    0  1
    1  2

    """
    duplicated = df.columns.duplicated()
    if duplicated.any():
        _logger.warning("Dropping duplicated columns...")
        columns = df.columns.values
        columns[duplicated] = "AWSDataWranglerDuplicatedMarker"
        df.columns = columns
        df.drop(columns="AWSDataWranglerDuplicatedMarker", inplace=True)
    return df


def get_connection(
    name: str, catalog_id: Optional[str] = None, boto3_session: Optional[boto3.Session] = None
) -> Dict[str, Any]:
    """Get Glue connection details.

    Parameters
    ----------
    name : str
        Connection name.
    catalog_id : str, optional
        The ID of the Data Catalog from which to retrieve Databases.
        If none is provided, the AWS account ID is used by default.
    boto3_session : boto3.Session(), optional
        Boto3 Session. The default boto3 session will be used if boto3_session receive None.

    Returns
    -------
    Dict[str, Any]
        API Response for:
        https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/glue.html#Glue.Client.get_connection

    Examples
    --------
    >>> import awswrangler as wr
    >>> res = wr.catalog.get_connection(name='my_connection')

    """
    client_glue: boto3.client = _utils.client(service_name="glue", session=boto3_session)
    if catalog_id is None:
        return client_glue.get_connection(Name=name, HidePassword=False)["Connection"]
    return client_glue.get_connection(CatalogId=catalog_id, Name=name, HidePassword=False)["Connection"]


def get_engine(
    connection: str, catalog_id: Optional[str] = None, boto3_session: Optional[boto3.Session] = None
) -> sqlalchemy.engine.Engine:
    """Return a SQLAlchemy Engine from a Glue Catalog Connection.

    Only Redshift, PostgreSQL and MySQL are supported.

    Parameters
    ----------
    connection : str
        Connection name.
    catalog_id : str, optional
        The ID of the Data Catalog from which to retrieve Databases.
        If none is provided, the AWS account ID is used by default.
    boto3_session : boto3.Session(), optional
        Boto3 Session. The default boto3 session will be used if boto3_session receive None.

    Returns
    -------
    sqlalchemy.engine.Engine
        SQLAlchemy Engine.

    Examples
    --------
    >>> import awswrangler as wr
    >>> res = wr.catalog.get_engine(name='my_connection')

    """
    details: Dict[str, Any] = get_connection(name=connection, catalog_id=catalog_id, boto3_session=boto3_session)[
        "ConnectionProperties"
    ]
    db_type: str = details["JDBC_CONNECTION_URL"].split(":")[1].lower()
    host: str = details["JDBC_CONNECTION_URL"].split(":")[2].replace("/", "")
    port, database = details["JDBC_CONNECTION_URL"].split(":")[3].split("/")
    user: str = _quote_plus(details["USERNAME"])
    password: str = _quote_plus(details["PASSWORD"])
    if db_type == "postgresql":
        _utils.ensure_postgresql_casts()
    if db_type in ("redshift", "postgresql"):
        conn_str: str = f"{db_type}+psycopg2://{user}:{password}@{host}:{port}/{database}"
        return sqlalchemy.create_engine(
            conn_str, echo=False, executemany_mode="values", executemany_values_page_size=100_000
        )
    if db_type == "mysql":
        conn_str = f"mysql+pymysql://{user}:{password}@{host}:{port}/{database}"
        return sqlalchemy.create_engine(conn_str, echo=False)
    raise exceptions.InvalidDatabaseType(  # pragma: no cover
        f"{db_type} is not a valid Database type." f" Only Redshift, PostgreSQL and MySQL are supported."
    )


def create_csv_table(
    database: str,
    table: str,
    path: str,
    columns_types: Dict[str, str],
    partitions_types: Optional[Dict[str, str]] = None,
    compression: Optional[str] = None,
    description: Optional[str] = None,
    parameters: Optional[Dict[str, str]] = None,
    columns_comments: Optional[Dict[str, str]] = None,
    mode: str = "overwrite",
    catalog_versioning: bool = False,
    sep: str = ",",
    boto3_session: Optional[boto3.Session] = None,
    projection_enabled: bool = False,
    projection_types: Optional[Dict[str, str]] = None,
    projection_ranges: Optional[Dict[str, str]] = None,
    projection_values: Optional[Dict[str, str]] = None,
    projection_intervals: Optional[Dict[str, str]] = None,
    projection_digits: Optional[Dict[str, str]] = None,
) -> None:
    """Create a CSV Table (Metadata Only) in the AWS Glue Catalog.

    'https://docs.aws.amazon.com/athena/latest/ug/data-types.html'

    Parameters
    ----------
    database : str
        Database name.
    table : str
        Table name.
    path : str
        Amazon S3 path (e.g. s3://bucket/prefix/).
    columns_types: Dict[str, str]
        Dictionary with keys as column names and vales as data types (e.g. {'col0': 'bigint', 'col1': 'double'}).
    partitions_types: Dict[str, str], optional
        Dictionary with keys as partition names and values as data types (e.g. {'col2': 'date'}).
    compression : str, optional
        Compression style (``None``, ``gzip``, etc).
    description : str, optional
        Table description
    parameters : Dict[str, str], optional
        Key/value pairs to tag the table.
    columns_comments: Dict[str, str], optional
        Columns names and the related comments (e.g. {'col0': 'Column 0.', 'col1': 'Column 1.', 'col2': 'Partition.'}).
    mode : str
        'overwrite' to recreate any possible axisting table or 'append' to keep any possible axisting table.
    catalog_versioning : bool
        If True and `mode="overwrite"`, creates an archived version of the table catalog before updating it.
    sep : str
        String of length 1. Field delimiter for the output file.
    projection_enabled : bool
        Enable Partition Projection on Athena (https://docs.aws.amazon.com/athena/latest/ug/partition-projection.html)
    projection_types : Optional[Dict[str, str]]
        Dictionary of partitions names and Athena projections types.
        Valid types: "enum", "integer", "date", "injected"
        https://docs.aws.amazon.com/athena/latest/ug/partition-projection-supported-types.html
        (e.g. {'col_name': 'enum', 'col2_name': 'integer'})
    projection_ranges: Optional[Dict[str, str]]
        Dictionary of partitions names and Athena projections ranges.
        https://docs.aws.amazon.com/athena/latest/ug/partition-projection-supported-types.html
        (e.g. {'col_name': '0,10', 'col2_name': '-1,8675309'})
    projection_values: Optional[Dict[str, str]]
        Dictionary of partitions names and Athena projections values.
        https://docs.aws.amazon.com/athena/latest/ug/partition-projection-supported-types.html
        (e.g. {'col_name': 'A,B,Unknown', 'col2_name': 'foo,boo,bar'})
    projection_intervals: Optional[Dict[str, str]]
        Dictionary of partitions names and Athena projections intervals.
        https://docs.aws.amazon.com/athena/latest/ug/partition-projection-supported-types.html
        (e.g. {'col_name': '1', 'col2_name': '5'})
    projection_digits: Optional[Dict[str, str]]
        Dictionary of partitions names and Athena projections digits.
        https://docs.aws.amazon.com/athena/latest/ug/partition-projection-supported-types.html
        (e.g. {'col_name': '1', 'col2_name': '2'})
    boto3_session : boto3.Session(), optional
        Boto3 Session. The default boto3 session will be used if boto3_session receive None.

    Returns
    -------
    None
        None.

    Examples
    --------
    >>> import awswrangler as wr
    >>> wr.catalog.create_csv_table(
    ...     database='default',
    ...     table='my_table',
    ...     path='s3://bucket/prefix/',
    ...     columns_types={'col0': 'bigint', 'col1': 'double'},
    ...     partitions_types={'col2': 'date'},
    ...     compression='gzip',
    ...     description='My own table!',
    ...     parameters={'source': 'postgresql'},
    ...     columns_comments={'col0': 'Column 0.', 'col1': 'Column 1.', 'col2': 'Partition.'}
    ... )

    """
    table = sanitize_table_name(table=table)
    partitions_types = {} if partitions_types is None else partitions_types
    table_input: Dict[str, Any] = _csv_table_definition(
        table=table,
        path=path,
        columns_types=columns_types,
        partitions_types=partitions_types,
        compression=compression,
        sep=sep,
    )
    session: boto3.Session = _utils.ensure_session(session=boto3_session)
    _create_table(
        database=database,
        table=table,
        description=description,
        parameters=parameters,
        columns_comments=columns_comments,
        mode=mode,
        catalog_versioning=catalog_versioning,
        boto3_session=session,
        table_input=table_input,
        table_exist=does_table_exist(database=database, table=table, boto3_session=session),
        partitions_types=partitions_types,
        projection_enabled=projection_enabled,
        projection_types=projection_types,
        projection_ranges=projection_ranges,
        projection_values=projection_values,
        projection_intervals=projection_intervals,
        projection_digits=projection_digits,
    )


def _create_table(  # pylint: disable=too-many-branches,too-many-statements
    database: str,
    table: str,
    description: Optional[str],
    parameters: Optional[Dict[str, str]],
    mode: str,
    catalog_versioning: bool,
    boto3_session: Optional[boto3.Session],
    table_input: Dict[str, Any],
    table_exist: bool,
    projection_enabled: bool,
    partitions_types: Optional[Dict[str, str]] = None,
    columns_comments: Optional[Dict[str, str]] = None,
    projection_types: Optional[Dict[str, str]] = None,
    projection_ranges: Optional[Dict[str, str]] = None,
    projection_values: Optional[Dict[str, str]] = None,
    projection_intervals: Optional[Dict[str, str]] = None,
    projection_digits: Optional[Dict[str, str]] = None,
):
    # Description
    if description is not None:
        table_input["Description"] = description

    # Parameters & Projection
    parameters = parameters if parameters else {}
    partitions_types = partitions_types if partitions_types else {}
    projection_types = projection_types if projection_types else {}
    projection_ranges = projection_ranges if projection_ranges else {}
    projection_values = projection_values if projection_values else {}
    projection_intervals = projection_intervals if projection_intervals else {}
    projection_digits = projection_digits if projection_digits else {}
    projection_types = {sanitize_column_name(k): v for k, v in projection_types.items()}
    projection_ranges = {sanitize_column_name(k): v for k, v in projection_ranges.items()}
    projection_values = {sanitize_column_name(k): v for k, v in projection_values.items()}
    projection_intervals = {sanitize_column_name(k): v for k, v in projection_intervals.items()}
    projection_digits = {sanitize_column_name(k): v for k, v in projection_digits.items()}
    for k, v in partitions_types.items():
        if v == "date":
            table_input["Parameters"][f"projection.{k}.format"] = "yyyy-MM-dd"
        elif v == "timestamp":
            table_input["Parameters"][f"projection.{k}.format"] = "yyyy-MM-dd HH:mm:ss"
            table_input["Parameters"][f"projection.{k}.interval.unit"] = "SECONDS"
            table_input["Parameters"][f"projection.{k}.interval"] = "1"
    for k, v in projection_types.items():
        table_input["Parameters"][f"projection.{k}.type"] = v
    for k, v in projection_ranges.items():
        table_input["Parameters"][f"projection.{k}.range"] = v
    for k, v in projection_values.items():
        table_input["Parameters"][f"projection.{k}.values"] = v
    for k, v in projection_intervals.items():
        table_input["Parameters"][f"projection.{k}.interval"] = str(v)
    for k, v in projection_digits.items():
        table_input["Parameters"][f"projection.{k}.digits"] = str(v)
    parameters["projection.enabled"] = "true" if projection_enabled is True else "false"
    for k, v in parameters.items():
        table_input["Parameters"][k] = v

    # Column comments
    columns_comments = columns_comments if columns_comments else {}
    columns_comments = {sanitize_column_name(k): v for k, v in columns_comments.items()}
    if columns_comments:
        for col in table_input["StorageDescriptor"]["Columns"]:
            name: str = col["Name"]
            if name in columns_comments:
                col["Comment"] = columns_comments[name]
        for par in table_input["PartitionKeys"]:
            name = par["Name"]
            if name in columns_comments:
                par["Comment"] = columns_comments[name]

    _logger.debug("table_input: %s", table_input)

    session: boto3.Session = _utils.ensure_session(session=boto3_session)
    client_glue: boto3.client = _utils.client(service_name="glue", session=session)
    skip_archive: bool = not catalog_versioning
    if mode not in ("overwrite", "append", "overwrite_partitions", "update"):  # pragma: no cover
        raise exceptions.InvalidArgument(
            f"{mode} is not a valid mode. It must be 'overwrite', 'append' or 'overwrite_partitions'."
        )
    if (table_exist is True) and (mode == "overwrite"):
        _logger.debug("Fetching existing partitions...")
        partitions_values: List[List[str]] = list(
            _get_partitions(database=database, table=table, boto3_session=session).values()
        )
        _logger.debug("Number of old partitions: %s", len(partitions_values))
        _logger.debug("Deleting existing partitions...")
        client_glue.batch_delete_partition(
            DatabaseName=database, TableName=table, PartitionsToDelete=[{"Values": v} for v in partitions_values]
        )
        _logger.debug("Updating table...")
        client_glue.update_table(DatabaseName=database, TableInput=table_input, SkipArchive=skip_archive)
    elif (table_exist is True) and (mode in ("append", "overwrite_partitions", "update")):
        if parameters is not None:
            upsert_table_parameters(parameters=parameters, database=database, table=table, boto3_session=session)
        if mode == "update":
            client_glue.update_table(DatabaseName=database, TableInput=table_input, SkipArchive=skip_archive)
    elif table_exist is False:
        client_glue.create_table(DatabaseName=database, TableInput=table_input)


def _csv_table_definition(
    table: str,
    path: str,
    columns_types: Dict[str, str],
    partitions_types: Dict[str, str],
    compression: Optional[str],
    sep: str,
) -> Dict[str, Any]:
    compressed: bool = compression is not None
    return {
        "Name": table,
        "PartitionKeys": [{"Name": cname, "Type": dtype} for cname, dtype in partitions_types.items()],
        "TableType": "EXTERNAL_TABLE",
        "Parameters": {
            "classification": "csv",
            "compressionType": str(compression).lower(),
            "typeOfData": "file",
            "delimiter": sep,
            "columnsOrdered": "true",
            "areColumnsQuoted": "false",
        },
        "StorageDescriptor": {
            "Columns": [{"Name": cname, "Type": dtype} for cname, dtype in columns_types.items()],
            "Location": path,
            "InputFormat": "org.apache.hadoop.mapred.TextInputFormat",
            "OutputFormat": "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
            "Compressed": compressed,
            "NumberOfBuckets": -1,
            "SerdeInfo": {
                "Parameters": {"field.delim": sep, "escape.delim": "\\"},
                "SerializationLibrary": "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe",
            },
            "StoredAsSubDirectories": False,
            "SortColumns": [],
            "Parameters": {
                "classification": "csv",
                "compressionType": str(compression).lower(),
                "typeOfData": "file",
                "delimiter": sep,
                "columnsOrdered": "true",
                "areColumnsQuoted": "false",
            },
        },
    }


def add_csv_partitions(
    database: str,
    table: str,
    partitions_values: Dict[str, List[str]],
    compression: Optional[str] = None,
    sep: str = ",",
    boto3_session: Optional[boto3.Session] = None,
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
    compression: str, optional
        Compression style (``None``, ``gzip``, etc).
    sep : str
        String of length 1. Field delimiter for the output file.
    boto3_session : boto3.Session(), optional
        Boto3 Session. The default boto3 session will be used if boto3_session receive None.

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
        _csv_partition_definition(location=k, values=v, compression=compression, sep=sep)
        for k, v in partitions_values.items()
    ]
    _add_partitions(database=database, table=table, boto3_session=boto3_session, inputs=inputs)


def _add_partitions(database: str, table: str, boto3_session: Optional[boto3.Session], inputs: List[Dict[str, Any]]):
    chunks: List[List[Dict[str, Any]]] = _utils.chunkify(lst=inputs, max_length=100)
    client_glue: boto3.client = _utils.client(service_name="glue", session=boto3_session)
    for chunk in chunks:  # pylint: disable=too-many-nested-blocks
        res: Dict[str, Any] = client_glue.batch_create_partition(
            DatabaseName=database, TableName=table, PartitionInputList=chunk
        )
        if ("Errors" in res) and res["Errors"]:
            for error in res["Errors"]:
                if "ErrorDetail" in error:
                    if "ErrorCode" in error["ErrorDetail"]:
                        if error["ErrorDetail"]["ErrorCode"] != "AlreadyExistsException":  # pragma: no cover
                            raise exceptions.ServiceApiError(str(res["Errors"]))


def _csv_partition_definition(location: str, values: List[str], compression: Optional[str], sep: str) -> Dict[str, Any]:
    compressed: bool = compression is not None
    return {
        "StorageDescriptor": {
            "InputFormat": "org.apache.hadoop.mapred.TextInputFormat",
            "OutputFormat": "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
            "Location": location,
            "Compressed": compressed,
            "SerdeInfo": {
                "Parameters": {"field.delim": sep, "escape.delim": "\\"},
                "SerializationLibrary": "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe",
            },
            "StoredAsSubDirectories": False,
        },
        "Values": values,
    }


def get_parquet_partitions(
    database: str,
    table: str,
    expression: Optional[str] = None,
    catalog_id: Optional[str] = None,
    boto3_session: Optional[boto3.Session] = None,
) -> Dict[str, List[str]]:
    """Get all partitions from a Table in the AWS Glue Catalog.

    Expression argument instructions:
    https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/glue.html#Glue.Client.get_partitions

    Parameters
    ----------
    database : str
        Database name.
    table : str
        Table name.
    expression : str, optional
        An expression that filters the partitions to be returned.
    catalog_id : str, optional
        The ID of the Data Catalog from which to retrieve Databases.
        If none is provided, the AWS account ID is used by default.
    boto3_session : boto3.Session(), optional
        Boto3 Session. The default boto3 session will be used if boto3_session receive None.

    Returns
    -------
    Dict[str, List[str]]
        partitions_values: Dictionary with keys as S3 path locations and values as a
        list of partitions values as str (e.g. {'s3://bucket/prefix/y=2020/m=10/': ['2020', '10']}).

    Examples
    --------
    Fetch all partitions

    >>> import awswrangler as wr
    >>> wr.catalog.get_parquet_partitions(
    ...     database='default',
    ...     table='my_table',
    ... )
    {
        's3://bucket/prefix/y=2020/m=10/': ['2020', '10'],
        's3://bucket/prefix/y=2020/m=11/': ['2020', '11'],
        's3://bucket/prefix/y=2020/m=12/': ['2020', '12']
    }

    Filtering partitions

    >>> import awswrangler as wr
    >>> wr.catalog.get_parquet_partitions(
    ...     database='default',
    ...     table='my_table',
    ...     expression='m=10'
    ... )
    {
        's3://bucket/prefix/y=2020/m=10/': ['2020', '10']
    }

    """
    return _get_partitions(
        database=database, table=table, expression=expression, catalog_id=catalog_id, boto3_session=boto3_session
    )


def get_csv_partitions(
    database: str,
    table: str,
    expression: Optional[str] = None,
    catalog_id: Optional[str] = None,
    boto3_session: Optional[boto3.Session] = None,
) -> Dict[str, List[str]]:
    """Get all partitions from a Table in the AWS Glue Catalog.

    Expression argument instructions:
    https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/glue.html#Glue.Client.get_partitions

    Parameters
    ----------
    database : str
        Database name.
    table : str
        Table name.
    expression : str, optional
        An expression that filters the partitions to be returned.
    catalog_id : str, optional
        The ID of the Data Catalog from which to retrieve Databases.
        If none is provided, the AWS account ID is used by default.
    boto3_session : boto3.Session(), optional
        Boto3 Session. The default boto3 session will be used if boto3_session receive None.

    Returns
    -------
    Dict[str, List[str]]
        partitions_values: Dictionary with keys as S3 path locations and values as a
        list of partitions values as str (e.g. {'s3://bucket/prefix/y=2020/m=10/': ['2020', '10']}).

    Examples
    --------
    Fetch all partitions

    >>> import awswrangler as wr
    >>> wr.catalog.get_csv_partitions(
    ...     database='default',
    ...     table='my_table',
    ... )
    {
        's3://bucket/prefix/y=2020/m=10/': ['2020', '10'],
        's3://bucket/prefix/y=2020/m=11/': ['2020', '11'],
        's3://bucket/prefix/y=2020/m=12/': ['2020', '12']
    }

    Filtering partitions

    >>> import awswrangler as wr
    >>> wr.catalog.get_csv_partitions(
    ...     database='default',
    ...     table='my_table',
    ...     expression='m=10'
    ... )
    {
        's3://bucket/prefix/y=2020/m=10/': ['2020', '10']
    }

    """
    return _get_partitions(
        database=database, table=table, expression=expression, catalog_id=catalog_id, boto3_session=boto3_session
    )


def get_partitions(
    database: str,
    table: str,
    expression: Optional[str] = None,
    catalog_id: Optional[str] = None,
    boto3_session: Optional[boto3.Session] = None,
) -> Dict[str, List[str]]:
    """Get all partitions from a Table in the AWS Glue Catalog.

    Expression argument instructions:
    https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/glue.html#Glue.Client.get_partitions

    Parameters
    ----------
    database : str
        Database name.
    table : str
        Table name.
    expression : str, optional
        An expression that filters the partitions to be returned.
    catalog_id : str, optional
        The ID of the Data Catalog from which to retrieve Databases.
        If none is provided, the AWS account ID is used by default.
    boto3_session : boto3.Session(), optional
        Boto3 Session. The default boto3 session will be used if boto3_session receive None.

    Returns
    -------
    Dict[str, List[str]]
        partitions_values: Dictionary with keys as S3 path locations and values as a
        list of partitions values as str (e.g. {'s3://bucket/prefix/y=2020/m=10/': ['2020', '10']}).

    Examples
    --------
    Fetch all partitions

    >>> import awswrangler as wr
    >>> wr.catalog.get_partitions(
    ...     database='default',
    ...     table='my_table',
    ... )
    {
        's3://bucket/prefix/y=2020/m=10/': ['2020', '10'],
        's3://bucket/prefix/y=2020/m=11/': ['2020', '11'],
        's3://bucket/prefix/y=2020/m=12/': ['2020', '12']
    }

    Filtering partitions

    >>> import awswrangler as wr
    >>> wr.catalog.get_partitions(
    ...     database='default',
    ...     table='my_table',
    ...     expression='m=10'
    ... )
    {
        's3://bucket/prefix/y=2020/m=10/': ['2020', '10']
    }

    """
    return _get_partitions(
        database=database, table=table, expression=expression, catalog_id=catalog_id, boto3_session=boto3_session
    )


def _get_partitions(
    database: str,
    table: str,
    expression: Optional[str] = None,
    catalog_id: Optional[str] = None,
    boto3_session: Optional[boto3.Session] = None,
) -> Dict[str, List[str]]:
    client_glue: boto3.client = _utils.client(service_name="glue", session=boto3_session)

    args: Dict[str, Any] = {
        "DatabaseName": database,
        "TableName": table,
        "MaxResults": 1_000,
        "Segment": {"SegmentNumber": 0, "TotalSegments": 1},
    }
    if expression is not None:
        args["Expression"] = expression
    if catalog_id is not None:
        args["CatalogId"] = catalog_id

    partitions_values: Dict[str, List[str]] = {}
    _logger.debug("Starting pagination...")

    response: Dict[str, Any] = client_glue.get_partitions(**args)
    token: Optional[str] = _append_partitions(partitions_values=partitions_values, response=response)
    while token is not None:
        args["NextToken"] = response["NextToken"]
        response = client_glue.get_partitions(**args)
        token = _append_partitions(partitions_values=partitions_values, response=response)

    _logger.debug("Pagination done.")
    return partitions_values


def _append_partitions(partitions_values: Dict[str, List[str]], response: Dict[str, Any]) -> Optional[str]:
    _logger.debug("response: %s", response)
    token: Optional[str] = response.get("NextToken", None)
    if (response is not None) and ("Partitions" in response):
        for partition in response["Partitions"]:
            location: Optional[str] = partition["StorageDescriptor"].get("Location")
            if location is not None:
                values: List[str] = partition["Values"]
                partitions_values[location] = values
    else:
        token = None  # pragma: no cover
    return token


def extract_athena_types(
    df: pd.DataFrame,
    index: bool = False,
    partition_cols: Optional[List[str]] = None,
    dtype: Optional[Dict[str, str]] = None,
    file_format: str = "parquet",
) -> Tuple[Dict[str, str], Dict[str, str]]:
    """Extract columns and partitions types (Amazon Athena) from Pandas DataFrame.

    https://docs.aws.amazon.com/athena/latest/ug/data-types.html

    Parameters
    ----------
    df : pandas.DataFrame
        Pandas DataFrame.
    index : bool
        Should consider the DataFrame index as a column?.
    partition_cols : List[str], optional
        List of partitions names.
    dtype: Dict[str, str], optional
        Dictionary of columns names and Athena/Glue types to be casted.
        Useful when you have columns with undetermined or mixed data types.
        (e.g. {'col name': 'bigint', 'col2 name': 'int'})
    file_format : str, optional
        File format to be consided to place the index column: "parquet" | "csv".

    Returns
    -------
    Tuple[Dict[str, str], Optional[Dict[str, str]]]
        columns_types: Dictionary with keys as column names and vales as
        data types (e.g. {'col0': 'bigint', 'col1': 'double'}). /
        partitions_types: Dictionary with keys as partition names
        and values as data types (e.g. {'col2': 'date'}).

    Examples
    --------
    >>> import awswrangler as wr
    >>> columns_types, partitions_types = wr.catalog.extract_athena_types(
    ...     df=df, index=False, partition_cols=["par0", "par1"], file_format="csv"
    ... )

    """
    if file_format == "parquet":
        index_left: bool = False
    elif file_format == "csv":
        index_left = True
    else:
        raise exceptions.InvalidArgumentValue("file_format argument must be parquet or csv")
    return _data_types.athena_types_from_pandas_partitioned(
        df=df, index=index, partition_cols=partition_cols, dtype=dtype, index_left=index_left
    )


def get_table_parameters(
    database: str, table: str, catalog_id: Optional[str] = None, boto3_session: Optional[boto3.Session] = None
) -> Dict[str, str]:
    """Get all parameters.

    Parameters
    ----------
    database : str
        Database name.
    table : str
        Table name.
    catalog_id : str, optional
        The ID of the Data Catalog from which to retrieve Databases.
        If none is provided, the AWS account ID is used by default.
    boto3_session : boto3.Session(), optional
        Boto3 Session. The default boto3 session will be used if boto3_session receive None.

    Returns
    -------
    Dict[str, str]
        Dictionary of parameters.

    Examples
    --------
    >>> import awswrangler as wr
    >>> pars = wr.catalog.get_table_parameters(database="...", table="...")

    """
    client_glue: boto3.client = _utils.client(service_name="glue", session=boto3_session)
    args: Dict[str, str] = {}
    if catalog_id is not None:
        args["CatalogId"] = catalog_id  # pragma: no cover
    args["DatabaseName"] = database
    args["Name"] = table
    response: Dict[str, Any] = client_glue.get_table(**args)
    parameters: Dict[str, str] = response["Table"]["Parameters"]
    return parameters


def get_table_description(
    database: str, table: str, catalog_id: Optional[str] = None, boto3_session: Optional[boto3.Session] = None
) -> Optional[str]:
    """Get table description.

    Parameters
    ----------
    database : str
        Database name.
    table : str
        Table name.
    catalog_id : str, optional
        The ID of the Data Catalog from which to retrieve Databases.
        If none is provided, the AWS account ID is used by default.
    boto3_session : boto3.Session(), optional
        Boto3 Session. The default boto3 session will be used if boto3_session receive None.

    Returns
    -------
    Optional[str]
        Description if exists.

    Examples
    --------
    >>> import awswrangler as wr
    >>> desc = wr.catalog.get_table_description(database="...", table="...")

    """
    client_glue: boto3.client = _utils.client(service_name="glue", session=boto3_session)
    args: Dict[str, str] = {}
    if catalog_id is not None:
        args["CatalogId"] = catalog_id  # pragma: no cover
    args["DatabaseName"] = database
    args["Name"] = table
    response: Dict[str, Any] = client_glue.get_table(**args)
    desc: Optional[str] = response["Table"].get("Description", None)
    return desc


def get_columns_comments(
    database: str, table: str, catalog_id: Optional[str] = None, boto3_session: Optional[boto3.Session] = None
) -> Dict[str, str]:
    """Get all columns comments.

    Parameters
    ----------
    database : str
        Database name.
    table : str
        Table name.
    catalog_id : str, optional
        The ID of the Data Catalog from which to retrieve Databases.
        If none is provided, the AWS account ID is used by default.
    boto3_session : boto3.Session(), optional
        Boto3 Session. The default boto3 session will be used if boto3_session receive None.

    Returns
    -------
    Dict[str, str]
        Columns comments. e.g. {"col1": "foo boo bar"}.

    Examples
    --------
    >>> import awswrangler as wr
    >>> pars = wr.catalog.get_table_parameters(database="...", table="...")

    """
    client_glue: boto3.client = _utils.client(service_name="glue", session=boto3_session)
    args: Dict[str, str] = {}
    if catalog_id is not None:
        args["CatalogId"] = catalog_id  # pragma: no cover
    args["DatabaseName"] = database
    args["Name"] = table
    response: Dict[str, Any] = client_glue.get_table(**args)
    comments: Dict[str, str] = {}
    for c in response["Table"]["StorageDescriptor"]["Columns"]:
        comments[c["Name"]] = c["Comment"]
    if "PartitionKeys" in response["Table"]:
        for p in response["Table"]["PartitionKeys"]:
            comments[p["Name"]] = p["Comment"]
    return comments


def upsert_table_parameters(
    parameters: Dict[str, str],
    database: str,
    table: str,
    catalog_id: Optional[str] = None,
    boto3_session: Optional[boto3.Session] = None,
) -> Dict[str, str]:
    """Insert or Update the received parameters.

    Parameters
    ----------
    parameters : Dict[str, str]
        e.g. {"source": "mysql", "destination":  "datalake"}
    database : str
        Database name.
    table : str
        Table name.
    catalog_id : str, optional
        The ID of the Data Catalog from which to retrieve Databases.
        If none is provided, the AWS account ID is used by default.
    boto3_session : boto3.Session(), optional
        Boto3 Session. The default boto3 session will be used if boto3_session receive None.

    Returns
    -------
    Dict[str, str]
       All parameters after the upsert.

    Examples
    --------
    >>> import awswrangler as wr
    >>> pars = wr.catalog.upsert_table_parameters(
    ...     parameters={"source": "mysql", "destination":  "datalake"},
    ...     database="...",
    ...     table="...")

    """
    session: boto3.Session = _utils.ensure_session(session=boto3_session)
    pars: Dict[str, str] = get_table_parameters(
        database=database, table=table, catalog_id=catalog_id, boto3_session=session
    )
    for k, v in parameters.items():
        pars[k] = v
    overwrite_table_parameters(
        parameters=pars, database=database, table=table, catalog_id=catalog_id, boto3_session=session
    )
    return pars


def overwrite_table_parameters(
    parameters: Dict[str, str],
    database: str,
    table: str,
    catalog_id: Optional[str] = None,
    boto3_session: Optional[boto3.Session] = None,
) -> Dict[str, str]:
    """Overwrite all existing parameters.

    Parameters
    ----------
    parameters : Dict[str, str]
        e.g. {"source": "mysql", "destination":  "datalake"}
    database : str
        Database name.
    table : str
        Table name.
    catalog_id : str, optional
        The ID of the Data Catalog from which to retrieve Databases.
        If none is provided, the AWS account ID is used by default.
    boto3_session : boto3.Session(), optional
        Boto3 Session. The default boto3 session will be used if boto3_session receive None.

    Returns
    -------
    Dict[str, str]
       All parameters after the overwrite (The same received).

    Examples
    --------
    >>> import awswrangler as wr
    >>> pars = wr.catalog.overwrite_table_parameters(
    ...     parameters={"source": "mysql", "destination":  "datalake"},
    ...     database="...",
    ...     table="...")

    """
    session: boto3.Session = _utils.ensure_session(session=boto3_session)
    table_input: Optional[Dict[str, Any]] = _get_table_input(
        database=database, table=table, catalog_id=catalog_id, boto3_session=session
    )
    if table_input is None:
        raise exceptions.InvalidTable(f"Table {table} does not exist on database {database}.")
    table_input["Parameters"] = parameters
    args2: Dict[str, Union[str, Dict[str, Any]]] = {}
    if catalog_id is not None:
        args2["CatalogId"] = catalog_id  # pragma: no cover
    args2["DatabaseName"] = database
    args2["TableInput"] = table_input
    client_glue: boto3.client = _utils.client(service_name="glue", session=session)
    client_glue.update_table(**args2)
    return parameters


def _get_table_input(
    database: str, table: str, boto3_session: Optional[boto3.Session], catalog_id: Optional[str] = None
) -> Optional[Dict[str, Any]]:
    client_glue: boto3.client = _utils.client(service_name="glue", session=boto3_session)
    args: Dict[str, str] = {}
    if catalog_id is not None:
        args["CatalogId"] = catalog_id  # pragma: no cover
    args["DatabaseName"] = database
    args["Name"] = table
    try:
        response: Dict[str, Any] = client_glue.get_table(**args)
    except client_glue.exceptions.EntityNotFoundException:
        return None

    table_input: Dict[str, Any] = {}
    for k, v in response["Table"].items():
        if k in [
            "Name",
            "Description",
            "Owner",
            "LastAccessTime",
            "LastAnalyzedTime",
            "Retention",
            "StorageDescriptor",
            "PartitionKeys",
            "ViewOriginalText",
            "ViewExpandedText",
            "TableType",
            "Parameters",
            "TargetTable",
        ]:
            table_input[k] = v

    return table_input
