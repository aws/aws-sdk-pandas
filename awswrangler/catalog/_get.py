"""AWS Glue Catalog Get Module."""
# pylint: disable=redefined-outer-name

import base64
import itertools
import logging
from typing import Any, Dict, Iterator, List, Optional, Union, cast
from urllib.parse import quote_plus as _quote_plus

import boto3
import pandas as pd
import sqlalchemy

from awswrangler import _utils, exceptions
from awswrangler._config import apply_configs
from awswrangler.catalog._utils import _catalog_id, _extract_dtypes_from_table_details

_logger: logging.Logger = logging.getLogger(__name__)


def _get_table_input(
    database: str, table: str, boto3_session: Optional[boto3.Session], catalog_id: Optional[str] = None
) -> Optional[Dict[str, Any]]:
    client_glue: boto3.client = _utils.client(service_name="glue", session=boto3_session)
    try:
        response: Dict[str, Any] = client_glue.get_table(
            **_catalog_id(catalog_id=catalog_id, DatabaseName=database, Name=table)
        )
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
        token = None
    return token


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


@apply_configs
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
    >>> wr.catalog.get_table_types(database='default', table='my_table')
    {'col0': 'int', 'col1': double}

    """
    client_glue: boto3.client = _utils.client(service_name="glue", session=boto3_session)
    try:
        response: Dict[str, Any] = client_glue.get_table(DatabaseName=database, Name=table)
    except client_glue.exceptions.EntityNotFoundException:
        return None
    return _extract_dtypes_from_table_details(response=response)


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
    response_iterator = paginator.paginate(**_catalog_id(catalog_id=catalog_id))
    for page in response_iterator:
        for db in page["DatabaseList"]:
            yield db


@apply_configs
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
    df_dict: Dict[str, List[str]] = {"Database": [], "Description": []}
    for db in dbs:
        df_dict["Database"].append(db["Name"])
        df_dict["Description"].append(db.get("Description", ""))
    return pd.DataFrame(data=df_dict)


@apply_configs
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
        try:
            for page in response_iterator:
                for tbl in page["TableList"]:
                    yield tbl
        except client_glue.exceptions.EntityNotFoundException:
            continue


@apply_configs
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
    pandas.DataFrame
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

    df_dict: Dict[str, List[str]] = {"Database": [], "Table": [], "Description": [], "Columns": [], "Partitions": []}
    for tbl in tbls:
        df_dict["Database"].append(tbl["DatabaseName"])
        df_dict["Table"].append(tbl["Name"])
        df_dict["Description"].append(tbl.get("Description", ""))
        if "Columns" in tbl["StorageDescriptor"]:
            df_dict["Columns"].append(", ".join([x["Name"] for x in tbl["StorageDescriptor"]["Columns"]]))
        else:
            df_dict["Columns"].append("")
        if "PartitionKeys" in tbl:
            df_dict["Partitions"].append(", ".join([x["Name"] for x in tbl["PartitionKeys"]]))
        else:
            df_dict["Partitions"].append("")
    return pd.DataFrame(data=df_dict)


def search_tables(
    text: str, catalog_id: Optional[str] = None, boto3_session: Optional[boto3.Session] = None
) -> Iterator[Dict[str, Any]]:
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
    while "NextToken" in response:
        args["NextToken"] = response["NextToken"]
        response = client_glue.search_tables(**args)
        for tbl in response["TableList"]:
            yield tbl


@apply_configs
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
    tbl = client_glue.get_table(**_catalog_id(catalog_id=catalog_id, DatabaseName=database, Name=table))["Table"]
    df_dict: Dict[str, List[Union[str, bool]]] = {"Column Name": [], "Type": [], "Partition": [], "Comment": []}
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


@apply_configs
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
        return cast(str, res["Table"]["StorageDescriptor"]["Location"])
    except KeyError as ex:
        raise exceptions.InvalidTable(f"{database}.{table}") from ex


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

    res = client_glue.get_connection(**_catalog_id(catalog_id=catalog_id, Name=name, HidePassword=False))["Connection"]
    if "ENCRYPTED_PASSWORD" in res["ConnectionProperties"]:
        client_kms = _utils.client(service_name="kms", session=boto3_session)
        pwd = client_kms.decrypt(CiphertextBlob=base64.b64decode(res["ConnectionProperties"]["ENCRYPTED_PASSWORD"]))[
            "Plaintext"
        ]
        res["ConnectionProperties"]["PASSWORD"] = pwd
    return cast(Dict[str, Any], res)


def get_engine(
    connection: str,
    catalog_id: Optional[str] = None,
    boto3_session: Optional[boto3.Session] = None,
    **sqlalchemy_kwargs: Any,
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
    sqlalchemy_kwargs
        keyword arguments forwarded to sqlalchemy.create_engine().
        https://docs.sqlalchemy.org/en/13/core/engines.html

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
        sqlalchemy_kwargs["executemany_mode"] = "values"
        sqlalchemy_kwargs["executemany_values_page_size"] = 100_000
        return sqlalchemy.create_engine(conn_str, **sqlalchemy_kwargs)
    if db_type == "mysql":
        conn_str = f"mysql+pymysql://{user}:{password}@{host}:{port}/{database}"
        return sqlalchemy.create_engine(conn_str, **sqlalchemy_kwargs)
    raise exceptions.InvalidDatabaseType(
        f"{db_type} is not a valid Database type." f" Only Redshift, PostgreSQL and MySQL are supported."
    )


@apply_configs
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


@apply_configs
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


@apply_configs
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
    response: Dict[str, Any] = client_glue.get_table(
        **_catalog_id(catalog_id=catalog_id, DatabaseName=database, Name=table)
    )
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
    response: Dict[str, Any] = client_glue.get_table(
        **_catalog_id(catalog_id=catalog_id, DatabaseName=database, Name=table)
    )
    desc: Optional[str] = response["Table"].get("Description", None)
    return desc


@apply_configs
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
    response: Dict[str, Any] = client_glue.get_table(
        **_catalog_id(catalog_id=catalog_id, DatabaseName=database, Name=table)
    )
    comments: Dict[str, str] = {}
    for c in response["Table"]["StorageDescriptor"]["Columns"]:
        comments[c["Name"]] = c["Comment"]
    if "PartitionKeys" in response["Table"]:
        for p in response["Table"]["PartitionKeys"]:
            comments[p["Name"]] = p["Comment"]
    return comments


@apply_configs
def get_table_versions(
    database: str, table: str, catalog_id: Optional[str] = None, boto3_session: Optional[boto3.Session] = None
) -> List[Dict[str, Any]]:
    """Get all versions.

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
    List[Dict[str, Any]
        List of table inputs:
        https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/glue.html#Glue.Client.get_table_versions

    Examples
    --------
    >>> import awswrangler as wr
    >>> tables_versions = wr.catalog.get_table_versions(database="...", table="...")

    """
    client_glue: boto3.client = _utils.client(service_name="glue", session=boto3_session)
    paginator = client_glue.get_paginator("get_table_versions")
    versions: List[Dict[str, Any]] = []
    response_iterator = paginator.paginate(**_catalog_id(DatabaseName=database, TableName=table, catalog_id=catalog_id))
    for page in response_iterator:
        for tbl in page["TableVersions"]:
            versions.append(tbl)
    return versions


@apply_configs
def get_table_number_of_versions(
    database: str, table: str, catalog_id: Optional[str] = None, boto3_session: Optional[boto3.Session] = None
) -> int:
    """Get tatal number of versions.

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
    int
        Total number of versions.

    Examples
    --------
    >>> import awswrangler as wr
    >>> num = wr.catalog.get_table_number_of_versions(database="...", table="...")

    """
    client_glue: boto3.client = _utils.client(service_name="glue", session=boto3_session)
    paginator = client_glue.get_paginator("get_table_versions")
    count: int = 0
    response_iterator = paginator.paginate(**_catalog_id(DatabaseName=database, TableName=table, catalog_id=catalog_id))
    for page in response_iterator:
        count += len(page["TableVersions"])
    return count
