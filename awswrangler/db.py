"""Database Module. Currently wrapping up all Redshift, PostgreSQL and MySQL functionalities."""

import logging
from typing import Any, Dict, Iterator, List, Optional, Tuple, Union
from urllib.parse import quote_plus

import boto3  # type: ignore
import pandas as pd  # type: ignore
import pyarrow as pa  # type: ignore
import sqlalchemy  # type: ignore
from sqlalchemy.sql.visitors import VisitableType  # type: ignore

from awswrangler import _data_types, _utils, exceptions

_logger: logging.Logger = logging.getLogger(__name__)


def to_sql(df: pd.DataFrame, con: sqlalchemy.engine.Engine, **pandas_kwargs) -> None:
    """Write records stored in a DataFrame to a SQL database.

    Support for Redshift, PostgreSQL and MySQL.

    Support for all pandas to_sql() arguments:
    https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.to_sql.html

    Parameters
    ----------
    df : pandas.DataFrame
        Pandas DataFrame https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html
    con : sqlalchemy.engine.Engine
        SQLAlchemy Engine. Please use,
        wr.db.get_engine(), wr.db.get_redshift_temp_engine() or wr.catalog.get_engine()
    pandas_kwargs
        keyword arguments forwarded to pandas.DataFrame.to_csv()
        https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.to_sql.html

    Returns
    -------
    None
        None.

    Examples
    --------
    Writing to Redshift with temporary credentials

    >>> import awswrangler as wr
    >>> import pandas as pd
    >>> wr.db.to_sql(
    ...     df=pd.DataFrame({'col': [1, 2, 3]}),
    ...     con=wr.db.get_redshift_temp_engine(cluster_identifier="...", user="...")
    ... )

    Writing to Redshift from Glue Catalog Connections

    >>> import awswrangler as wr
    >>> import pandas as pd
    >>> wr.db.to_sql(
    ...     df=pd.DataFrame({'col': [1, 2, 3]}),
    ...     con=wr.catalog.get_engine(connection="...")
    ... )

    """
    if df.empty is True:  # pragma: no cover
        raise exceptions.EmptyDataFrame()
    if not isinstance(con, sqlalchemy.engine.Engine):  # pragma: no cover
        raise exceptions.InvalidConnection(
            "Invalid 'con' argument, please pass a "
            "SQLAlchemy Engine. Use wr.db.get_engine(), "
            "wr.db.get_redshift_temp_engine() or wr.catalog.get_engine()"
        )
    if "dtype" in pandas_kwargs:
        cast_columns: Dict[str, VisitableType] = pandas_kwargs["dtype"]
    else:
        cast_columns = {}
    dtypes: Dict[str, VisitableType] = _data_types.sqlalchemy_types_from_pandas(
        df=df, db_type=con.name, cast_columns=cast_columns
    )
    pandas_kwargs["dtype"] = dtypes
    pandas_kwargs["con"] = con
    df.to_sql(**pandas_kwargs)


def read_sql_query(
    sql: str,
    con: sqlalchemy.engine.Engine,
    index_col: Optional[Union[str, List[str]]] = None,
    params: Optional[Union[List, Tuple, Dict]] = None,
    chunksize: Optional[int] = None,
    dtype: Optional[Dict[str, pa.DataType]] = None,
) -> Union[pd.DataFrame, Iterator[pd.DataFrame]]:
    """Return a DataFrame corresponding to the result set of the query string.

    Support for Redshift, PostgreSQL and MySQL.

    Parameters
    ----------
    sql : str
        Pandas DataFrame https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html
    con : sqlalchemy.engine.Engine
        SQLAlchemy Engine. Please use,
        wr.db.get_engine(), wr.db.get_redshift_temp_engine() or wr.catalog.get_engine()
    index_col : Union[str, List[str]], optional
        Column(s) to set as index(MultiIndex).
    params :  Union[List, Tuple, Dict], optional
        List of parameters to pass to execute method.
        The syntax used to pass parameters is database driver dependent.
        Check your database driver documentation for which of the five syntax styles,
        described in PEP 249’s paramstyle, is supported.
        Eg. for psycopg2, uses %(name)s so use params={‘name’ : ‘value’}.
    chunksize : int, optional
        If specified, return an iterator where chunksize is the number of rows to include in each chunk.
    dtype : Dict[str, pyarrow.DataType], optional
        Specifying the datatype for columns.
        The keys should be the column names and the values should be the PyArrow types.

    Returns
    -------
    Union[pandas.DataFrame, Iterator[pandas.DataFrame]]
        Result as Pandas DataFrame(s).

    Examples
    --------
    Reading from Redshift with temporary credentials

    >>> import awswrangler as wr
    >>> df = wr.db.read_sql_query(
    ...     sql="SELECT * FROM public.my_table",
    ...     con=wr.db.get_redshift_temp_engine(cluster_identifier="...", user="...")
    ... )

    Reading from Redshift from Glue Catalog Connections

    >>> import awswrangler as wr
    >>> df = wr.db.read_sql_query(
    ...     sql="SELECT * FROM public.my_table",
    ...     con=wr.catalog.get_engine(connection="...")
    ... )

    """
    if not isinstance(con, sqlalchemy.engine.Engine):  # pragma: no cover
        raise exceptions.InvalidConnection(
            "Invalid 'con' argument, please pass a "
            "SQLAlchemy Engine. Use wr.db.get_engine(), "
            "wr.db.get_redshift_temp_engine() or wr.catalog.get_engine()"
        )
    with con.connect() as _con:
        args = _convert_params(sql, params)
        cursor = _con.execute(*args)
        if chunksize is None:
            return _records2df(records=cursor.fetchall(), cols_names=cursor.keys(), index=index_col, dtype=dtype)
        return _iterate_cursor(cursor=cursor, chunksize=chunksize, index=index_col, dtype=dtype)


def _iterate_cursor(
    cursor, chunksize: int, index: Optional[Union[str, List[str]]], dtype: Optional[Dict[str, pa.DataType]] = None
) -> Iterator[pd.DataFrame]:
    while True:
        records = cursor.fetchmany(chunksize)
        if not records:
            break
        df: pd.DataFrame = _records2df(records=records, cols_names=cursor.keys(), index=index, dtype=dtype)
        yield df


def _records2df(
    records: List[Tuple[Any]],
    cols_names: List[str],
    index: Optional[Union[str, List[str]]],
    dtype: Optional[Dict[str, pa.DataType]] = None,
) -> pd.DataFrame:
    arrays: List[pa.Array] = []
    for col_values, col_name in zip(tuple(zip(*records)), cols_names):  # Transposing
        if (dtype is None) or (col_name not in dtype):
            array: pa.Array = pa.array(obj=col_values, safe=True)  # Creating Arrow array
        else:
            array = pa.array(obj=col_values, type=dtype[col_name], safe=True)  # Creating Arrow array with dtype
        arrays.append(array)
    table = pa.Table.from_arrays(arrays=arrays, names=cols_names)  # Creating arrow Table
    df: pd.DataFrame = table.to_pandas(  # Creating Pandas DataFrame
        use_threads=True,
        split_blocks=True,
        self_destruct=True,
        integer_object_nulls=False,
        date_as_object=True,
        types_mapper=_data_types.pyarrow2pandas_extension,
    )
    if index is not None:
        df.set_index(index, inplace=True)
    return df


def _convert_params(sql: str, params: Optional[Union[List, Tuple, Dict]]) -> List[Any]:
    args: List[Any] = [sql]
    if params is not None:
        if hasattr(params, "keys"):
            return args + [params]
        return args + [list(params)]
    return args


def read_sql_table(
    table: str,
    con: sqlalchemy.engine.Engine,
    schema: Optional[str] = None,
    index_col: Optional[Union[str, List[str]]] = None,
    params: Optional[Union[List, Tuple, Dict]] = None,
    chunksize: Optional[int] = None,
    dtype: Optional[Dict[str, pa.DataType]] = None,
) -> Union[pd.DataFrame, Iterator[pd.DataFrame]]:
    """Return a DataFrame corresponding to the result set of the query string.

    Support for Redshift, PostgreSQL and MySQL.

    Parameters
    ----------
    table : str
        Nable name.
    con : sqlalchemy.engine.Engine
        SQLAlchemy Engine. Please use,
        wr.db.get_engine(), wr.db.get_redshift_temp_engine() or wr.catalog.get_engine()
    schema : str, optional
        Name of SQL schema in database to query (if database flavor supports this).
        Uses default schema if None (default).
    index_col : Union[str, List[str]], optional
        Column(s) to set as index(MultiIndex).
    params :  Union[List, Tuple, Dict], optional
        List of parameters to pass to execute method.
        The syntax used to pass parameters is database driver dependent.
        Check your database driver documentation for which of the five syntax styles,
        described in PEP 249’s paramstyle, is supported.
        Eg. for psycopg2, uses %(name)s so use params={‘name’ : ‘value’}.
    chunksize : int, optional
        If specified, return an iterator where chunksize is the number of rows to include in each chunk.
    dtype : Dict[str, pyarrow.DataType], optional
        Specifying the datatype for columns.
        The keys should be the column names and the values should be the PyArrow types.

    Returns
    -------
    Union[pandas.DataFrame, Iterator[pandas.DataFrame]]
        Result as Pandas DataFrame(s).

    Examples
    --------
    Reading from Redshift with temporary credentials

    >>> import awswrangler as wr
    >>> df = wr.db.read_sql_table(
    ...     table="my_table",
    ...     schema="public",
    ...     con=wr.db.get_redshift_temp_engine(cluster_identifier="...", user="...")
    ... )

    Reading from Redshift from Glue Catalog Connections

    >>> import awswrangler as wr
    >>> df = wr.db.read_sql_table(
    ...     table="my_table",
    ...     schema="public",
    ...     con=wr.catalog.get_engine(connection="...")
    ... )

    """
    if schema is None:
        sql: str = f"SELECT * FROM {table}"
    else:
        sql = f"SELECT * FROM {schema}.{table}"
    return read_sql_query(sql=sql, con=con, index_col=index_col, params=params, chunksize=chunksize, dtype=dtype)


def get_redshift_temp_engine(
    cluster_identifier: str,
    user: str,
    database: Optional[str] = None,
    duration: int = 900,
    boto3_session: Optional[boto3.Session] = None,
) -> sqlalchemy.engine.Engine:
    """Get Glue connection details.

    Parameters
    ----------
    cluster_identifier : str
        The unique identifier of a cluster.
        This parameter is case sensitive.
    user : str, optional
        The name of a database user.
    database : str, optional
        Database name. If None, the default Database is used.
    duration : int, optional
        The number of seconds until the returned temporary password expires.
        Constraint: minimum 900, maximum 3600.
        Default: 900
    boto3_session : boto3.Session(), optional
        Boto3 Session. The default boto3 session will be used if boto3_session receive None.

    Returns
    -------
    sqlalchemy.engine.Engine
        SQLAlchemy Engine.

    Examples
    --------
    >>> import awswrangler as wr
    >>> engine = wr.db.get_redshift_temp_engine('my_cluster', 'my_user')

    """
    client_redshift: boto3.client = _utils.client(service_name="redshift", session=boto3_session)
    res: Dict[str, Any] = client_redshift.get_cluster_credentials(
        DbUser=user, ClusterIdentifier=cluster_identifier, DurationSeconds=duration, AutoCreate=False
    )
    _user: str = quote_plus(res["DbUser"])
    password: str = quote_plus(res["DbPassword"])
    cluster: Dict[str, Any] = client_redshift.describe_clusters(ClusterIdentifier=cluster_identifier)["Clusters"][0]
    host: str = cluster["Endpoint"]["Address"]
    port: str = cluster["Endpoint"]["Port"]
    if database is None:
        database = cluster["DBName"]
    conn_str: str = f"redshift+psycopg2://{_user}:{password}@{host}:{port}/{database}"
    return sqlalchemy.create_engine(
        conn_str, echo=False, executemany_mode="values", executemany_values_page_size=100_000
    )


def get_engine(db_type: str, host: str, port: int, database: str, user: str, password: str) -> sqlalchemy.engine.Engine:
    """Return a SQLAlchemy Engine from the given arguments.

    Only Redshift, PostgreSQL and MySQL are supported.

    Parameters
    ----------
    db_type : str
        Database type: "redshift", "mysql" or "postgresql".
    host : str
        Host address.
    port : str
        Port number.
    database : str
        Database name.
    user : str
        Username.
    password : str
        Password.

    Returns
    -------
    sqlalchemy.engine.Engine
        SQLAlchemy Engine.

    Examples
    --------
    >>> import awswrangler as wr
    >>> engine = wr.db.get_engine(
    ...     db_type="postgresql",
    ...     host="...",
    ...     port=1234,
    ...     database="...",
    ...     user="...",
    ...     password="..."
    ... )

    """
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
