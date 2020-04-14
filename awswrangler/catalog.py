"""AWS Glue Catalog Module."""
# pylint: disable=redefined-outer-name

import itertools
import logging
import re
import unicodedata
from typing import Any, Dict, Iterator, List, Optional, Tuple
from urllib.parse import quote_plus

import boto3  # type: ignore
import pandas as pd  # type: ignore
import sqlalchemy  # type: ignore

from awswrangler import _data_types, _utils, exceptions

_logger: logging.Logger = logging.getLogger(__name__)


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
    partitions_types: Optional[Dict[str, str]],
    compression: Optional[str] = None,
    description: Optional[str] = None,
    parameters: Optional[Dict[str, str]] = None,
    columns_comments: Optional[Dict[str, str]] = None,
    mode: str = "overwrite",
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
        Only 'overwrite' available by now.
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
    table_input: Dict[str, Any] = _parquet_table_definition(
        table=table, path=path, columns_types=columns_types, partitions_types=partitions_types, compression=compression
    )
    _create_table(
        database=database,
        table=table,
        description=description,
        parameters=parameters,
        columns_comments=columns_comments,
        mode=mode,
        boto3_session=boto3_session,
        table_input=table_input,
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


def get_table_types(database: str, table: str, boto3_session: Optional[boto3.Session] = None) -> Dict[str, str]:
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
        A dictionary as {'col name': 'col data type'}.

    Examples
    --------
    >>> import awswrangler as wr
    >>> wr.catalog.get_table_types(database='default', name='my_table')
    {'col0': 'int', 'col1': double}

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
        if "Description" in db:
            df_dict["Description"].append(db["Description"])
        else:  # pragma: no cover
            df_dict["Description"].append("")
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
        args["Expression"] = f"{name_prefix}.*{name_contains}.*{name_suffix}"
    elif (name_prefix is not None) and (name_suffix is not None):
        args["Expression"] = f"{name_prefix}.*{name_suffix}"
    elif name_contains is not None:
        args["Expression"] = f".*{name_contains}.*"
    elif name_prefix is not None:
        args["Expression"] = f"{name_prefix}.*"
    elif name_suffix is not None:
        args["Expression"] = f".*{name_suffix}"
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
    for table in tbls:
        df_dict["Database"].append(table["DatabaseName"])
        df_dict["Table"].append(table["Name"])
        if "Description" in table:
            df_dict["Description"].append(table["Description"])
        else:
            df_dict["Description"].append("")
        df_dict["Columns"].append(", ".join([x["Name"] for x in table["StorageDescriptor"]["Columns"]]))
        df_dict["Partitions"].append(", ".join([x["Name"] for x in table["PartitionKeys"]]))
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
    >>> wr.catalog.get_table_location(database='default', name='my_table')
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
    name = re.sub("[^A-Za-z0-9_]+", "_", name)  # Removing non alphanumeric characters
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
    duplicated_cols = df.columns.duplicated()
    duplicated_cols_names: List[str] = list(df.columns[duplicated_cols])
    if len(duplicated_cols_names) > 0:
        _logger.warning(f"Dropping repeated columns: {duplicated_cols_names}")
    return df.loc[:, ~duplicated_cols]


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
    user: str = quote_plus(details["USERNAME"])
    password: str = quote_plus(details["PASSWORD"])
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
    partitions_types: Optional[Dict[str, str]],
    compression: Optional[str] = None,
    description: Optional[str] = None,
    parameters: Optional[Dict[str, str]] = None,
    columns_comments: Optional[Dict[str, str]] = None,
    mode: str = "overwrite",
    sep: str = ",",
    boto3_session: Optional[boto3.Session] = None,
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
    compression: str, optional
        Compression style (``None``, ``gzip``, etc).
    description: str, optional
        Table description
    parameters: Dict[str, str], optional
        Key/value pairs to tag the table.
    columns_comments: Dict[str, str], optional
        Columns names and the related comments (e.g. {'col0': 'Column 0.', 'col1': 'Column 1.', 'col2': 'Partition.'}).
    mode: str
        Only 'overwrite' available by now.
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
    _create_table(
        database=database,
        table=table,
        description=description,
        parameters=parameters,
        columns_comments=columns_comments,
        mode=mode,
        boto3_session=boto3_session,
        table_input=table_input,
    )


def _create_table(
    database: str,
    table: str,
    description: Optional[str],
    parameters: Optional[Dict[str, str]],
    columns_comments: Optional[Dict[str, str]],
    mode: str,
    boto3_session: Optional[boto3.Session],
    table_input: Dict[str, Any],
):
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
    session: boto3.Session = _utils.ensure_session(session=boto3_session)
    if mode == "overwrite":
        delete_table_if_exists(database=database, table=table, boto3_session=session)
    client_glue: boto3.client = _utils.client(service_name="glue", session=session)
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


def _get_partitions(
    database: str,
    table: str,
    expression: Optional[str] = None,
    catalog_id: Optional[str] = None,
    boto3_session: Optional[boto3.Session] = None,
) -> Dict[str, List[str]]:
    client_glue: boto3.client = _utils.client(service_name="glue", session=boto3_session)
    paginator = client_glue.get_paginator("get_partitions")
    args: Dict[str, Any] = {}
    if expression is not None:
        args["Expression"] = expression
    if catalog_id is not None:
        args["CatalogId"] = catalog_id
    response_iterator = paginator.paginate(
        DatabaseName=database, TableName=table, PaginationConfig={"PageSize": 1000}, **args
    )
    partitions_values: Dict[str, List[str]] = {}
    for page in response_iterator:
        if (page is not None) and ("Partitions" in page):
            for partition in page["Partitions"]:
                location: Optional[str] = partition["StorageDescriptor"].get("Location")
                if location is not None:
                    values: List[str] = partition["Values"]
                    partitions_values[location] = values
    return partitions_values


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
