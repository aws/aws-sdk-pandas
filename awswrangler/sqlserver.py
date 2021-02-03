"""Amazon Microsoft SQL Server Module."""


import importlib
import logging
from typing import Any, Callable, Dict, Iterator, List, Optional, Tuple, TypeVar, Union

import boto3
import pandas as pd
import pyarrow as pa

from awswrangler import _data_types
from awswrangler import _databases as _db_utils
from awswrangler import exceptions

__all__ = ["connect", "read_sql_query", "read_sql_table", "to_sql"]

_pyodbc_found = importlib.util.find_spec("pyodbc")
if _pyodbc_found:
    import pyodbc  # pylint: disable=import-error

_logger: logging.Logger = logging.getLogger(__name__)
FuncT = TypeVar("FuncT", bound=Callable[..., Any])


def _check_for_pyodbc(func: FuncT) -> FuncT:
    def inner(*args: Any, **kwargs: Any) -> Any:
        if not _pyodbc_found:
            raise ModuleNotFoundError(
                "You need to install pyodbc respectively the "
                "AWS Data Wrangler package with the `sqlserver` extra for using the sqlserver module"
            )
        return func(*args, **kwargs)

    return inner  # type: ignore


def _validate_connection(con: "pyodbc.Connection") -> None:
    if not isinstance(con, pyodbc.Connection):
        raise exceptions.InvalidConnection(
            "Invalid 'conn' argument, please pass a "
            "pyodbc.Connection object. Use pyodbc.connect() to use "
            "credentials directly or wr.sqlserver.connect() to fetch it from the Glue Catalog."
        )


def _get_table_identifier(schema: Optional[str], table: str) -> str:
    schema_str = f'"{schema}".' if schema else ""
    table_identifier = f'{schema_str}"{table}"'
    return table_identifier


def _drop_table(cursor: "pyodbc.Cursor", schema: Optional[str], table: str) -> None:
    table_identifier = _get_table_identifier(schema, table)
    sql = f"IF OBJECT_ID(N'{table_identifier}', N'U') IS NOT NULL DROP TABLE {table_identifier}"
    _logger.debug("Drop table query:\n%s", sql)
    cursor.execute(sql)


def _does_table_exist(cursor: "pyodbc.Cursor", schema: Optional[str], table: str) -> bool:
    schema_str = f"TABLE_SCHEMA = '{schema}' AND" if schema else ""
    cursor.execute(f"SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE " f"{schema_str} TABLE_NAME = '{table}'")
    return len(cursor.fetchall()) > 0


def _create_table(
    df: pd.DataFrame,
    cursor: "pyodbc.Cursor",
    table: str,
    schema: str,
    mode: str,
    index: bool,
    dtype: Optional[Dict[str, str]],
    varchar_lengths: Optional[Dict[str, int]],
) -> None:
    if mode == "overwrite":
        _drop_table(cursor=cursor, schema=schema, table=table)
    elif _does_table_exist(cursor=cursor, schema=schema, table=table):
        return
    sqlserver_types: Dict[str, str] = _data_types.database_types_from_pandas(
        df=df,
        index=index,
        dtype=dtype,
        varchar_lengths_default="VARCHAR(MAX)",
        varchar_lengths=varchar_lengths,
        converter_func=_data_types.pyarrow2sqlserver,
    )
    cols_str: str = "".join([f"{k} {v},\n" for k, v in sqlserver_types.items()])[:-2]
    table_identifier = _get_table_identifier(schema, table)
    sql = (
        f"IF OBJECT_ID(N'{table_identifier}', N'U') IS NULL BEGIN CREATE TABLE {table_identifier} (\n{cols_str}); END;"
    )
    _logger.debug("Create table query:\n%s", sql)
    cursor.execute(sql)


@_check_for_pyodbc
def connect(
    connection: Optional[str] = None,
    secret_id: Optional[str] = None,
    catalog_id: Optional[str] = None,
    dbname: Optional[str] = None,
    odbc_driver_version: int = 17,
    boto3_session: Optional[boto3.Session] = None,
    timeout: Optional[int] = 0,
) -> "pyodbc.Connection":
    """Return a pyodbc connection from a Glue Catalog Connection.

    https://github.com/mkleehammer/pyodbc

    Parameters
    ----------
    connection : Optional[str]
        Glue Catalog Connection name.
    secret_id: Optional[str]:
        Specifies the secret containing the version that you want to retrieve.
        You can specify either the Amazon Resource Name (ARN) or the friendly name of the secret.
    catalog_id : str, optional
        The ID of the Data Catalog.
        If none is provided, the AWS account ID is used by default.
    dbname: Optional[str]
        Optional database name to overwrite the stored one.
    odbc_driver_version : int
        Major version of the OBDC Driver version that is installed and should be used.
    boto3_session : boto3.Session(), optional
        Boto3 Session. The default boto3 session will be used if boto3_session receive None.
    timeout: Optional[int]
        This is the time in seconds before the connection to the server will time out.
        The default is None which means no timeout.
        This parameter is forwarded to pyodbc.
        https://github.com/mkleehammer/pyodbc/wiki/The-pyodbc-Module#connect

    Returns
    -------
    pyodbc.Connection
        pyodbc connection.

    Examples
    --------
    >>> import awswrangler as wr
    >>> con = wr.sqlserver.connect(connection="MY_GLUE_CONNECTION", odbc_driver_version=17)
    >>> with con.cursor() as cursor:
    >>>     cursor.execute("SELECT 1")
    >>>     print(cursor.fetchall())
    >>> con.close()

    """
    attrs: _db_utils.ConnectionAttributes = _db_utils.get_connection_attributes(
        connection=connection, secret_id=secret_id, catalog_id=catalog_id, dbname=dbname, boto3_session=boto3_session
    )
    if attrs.kind != "sqlserver":
        raise exceptions.InvalidDatabaseType(
            f"Invalid connection type ({attrs.kind}. It must be a sqlserver connection.)"
        )
    connection_str = (
        f"DRIVER={{ODBC Driver {odbc_driver_version} for SQL Server}};"
        f"SERVER={attrs.host},{attrs.port};"
        f"DATABASE={attrs.database};"
        f"UID={attrs.user};"
        f"PWD={attrs.password}"
    )

    return pyodbc.connect(connection_str, timeout=timeout)


@_check_for_pyodbc
def read_sql_query(
    sql: str,
    con: "pyodbc.Connection",
    index_col: Optional[Union[str, List[str]]] = None,
    params: Optional[Union[List[Any], Tuple[Any, ...], Dict[Any, Any]]] = None,
    chunksize: Optional[int] = None,
    dtype: Optional[Dict[str, pa.DataType]] = None,
    safe: bool = True,
) -> Union[pd.DataFrame, Iterator[pd.DataFrame]]:
    """Return a DataFrame corresponding to the result set of the query string.

    Parameters
    ----------
    sql : str
        SQL query.
    con : pyodbc.Connection
        Use pyodbc.connect() to use credentials directly or wr.sqlserver.connect() to fetch it from the Glue Catalog.
    index_col : Union[str, List[str]], optional
        Column(s) to set as index(MultiIndex).
    params :  Union[List, Tuple, Dict], optional
        List of parameters to pass to execute method.
        The syntax used to pass parameters is database driver dependent.
        Check your database driver documentation for which of the five syntax styles,
        described in PEP 249’s paramstyle, is supported.
    chunksize : int, optional
        If specified, return an iterator where chunksize is the number of rows to include in each chunk.
    dtype : Dict[str, pyarrow.DataType], optional
        Specifying the datatype for columns.
        The keys should be the column names and the values should be the PyArrow types.
    safe : bool
        Check for overflows or other unsafe data type conversions.

    Returns
    -------
    Union[pandas.DataFrame, Iterator[pandas.DataFrame]]
        Result as Pandas DataFrame(s).

    Examples
    --------
    Reading from Microsoft SQL Server using a Glue Catalog Connections
    >>> import awswrangler as wr
    >>> con = wr.sqlserver.connect(connection="MY_GLUE_CONNECTION", odbc_driver_version=17)
    >>> df = wr.sqlserver.read_sql_query(
    ...     sql="SELECT * FROM dbo.my_table",
    ...     con=con
    ... )
    >>> con.close()
    """
    _validate_connection(con=con)
    return _db_utils.read_sql_query(
        sql=sql, con=con, index_col=index_col, params=params, chunksize=chunksize, dtype=dtype, safe=safe
    )


@_check_for_pyodbc
def read_sql_table(
    table: str,
    con: "pyodbc.Connection",
    schema: Optional[str] = None,
    index_col: Optional[Union[str, List[str]]] = None,
    params: Optional[Union[List[Any], Tuple[Any, ...], Dict[Any, Any]]] = None,
    chunksize: Optional[int] = None,
    dtype: Optional[Dict[str, pa.DataType]] = None,
    safe: bool = True,
) -> Union[pd.DataFrame, Iterator[pd.DataFrame]]:
    """Return a DataFrame corresponding the table.

    Parameters
    ----------
    table : str
        Table name.
    con : pyodbc.Connection
        Use pyodbc.connect() to use credentials directly or wr.sqlserver.connect() to fetch it from the Glue Catalog.
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
    chunksize : int, optional
        If specified, return an iterator where chunksize is the number of rows to include in each chunk.
    dtype : Dict[str, pyarrow.DataType], optional
        Specifying the datatype for columns.
        The keys should be the column names and the values should be the PyArrow types.
    safe : bool
        Check for overflows or other unsafe data type conversions.

    Returns
    -------
    Union[pandas.DataFrame, Iterator[pandas.DataFrame]]
        Result as Pandas DataFrame(s).

    Examples
    --------
    Reading from Microsoft SQL Server using a Glue Catalog Connections

    >>> import awswrangler as wr
    >>> con = wr.sqlserver.connect(connection="MY_GLUE_CONNECTION", odbc_driver_version=17)
    >>> df = wr.sqlserver.read_sql_table(
    ...     table="my_table",
    ...     schema="dbo",
    ...     con=con
    ... )
    >>> con.close()
    """
    table_identifier = _get_table_identifier(schema, table)
    sql: str = f"SELECT * FROM {table_identifier}"
    return read_sql_query(
        sql=sql, con=con, index_col=index_col, params=params, chunksize=chunksize, dtype=dtype, safe=safe
    )


@_check_for_pyodbc
def to_sql(
    df: pd.DataFrame,
    con: "pyodbc.Connection",
    table: str,
    schema: str,
    mode: str = "append",
    index: bool = False,
    dtype: Optional[Dict[str, str]] = None,
    varchar_lengths: Optional[Dict[str, int]] = None,
) -> None:
    """Write records stored in a DataFrame into Microsoft SQL Server.

    Parameters
    ----------
    df : pandas.DataFrame
        Pandas DataFrame https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html
    con : pyodbc.Connection
        Use pyodbc.connect() to use credentials directly or wr.sqlserver.connect() to fetch it from the Glue Catalog.
    table : str
        Table name
    schema : str
        Schema name
    mode : str
        Append or overwrite.
    index : bool
        True to store the DataFrame index as a column in the table,
        otherwise False to ignore it.
    dtype: Dict[str, str], optional
        Dictionary of columns names and Microsoft SQL Server types to be casted.
        Useful when you have columns with undetermined or mixed data types.
        (e.g. {'col name': 'TEXT', 'col2 name': 'FLOAT'})
    varchar_lengths : Dict[str, int], optional
        Dict of VARCHAR length by columns. (e.g. {"col1": 10, "col5": 200}).

    Returns
    -------
    None
        None.

    Examples
    --------
    Writing to Microsoft SQL Server using a Glue Catalog Connections

    >>> import awswrangler as wr
    >>> con = wr.sqlserver.connect(connection="MY_GLUE_CONNECTION", odbc_driver_version=17)
    >>> wr.sqlserver.to_sql(
    ...     df=df,
    ...     table="table",
    ...     schema="dbo",
    ...     con=con
    ... )
    >>> con.close()

    """
    if df.empty is True:
        raise exceptions.EmptyDataFrame()
    _validate_connection(con=con)
    try:
        with con.cursor() as cursor:
            _create_table(
                df=df,
                cursor=cursor,
                table=table,
                schema=schema,
                mode=mode,
                index=index,
                dtype=dtype,
                varchar_lengths=varchar_lengths,
            )
            if index:
                df.reset_index(level=df.index.names, inplace=True)
            placeholders: str = ", ".join(["?"] * len(df.columns))
            table_identifier = _get_table_identifier(schema, table)
            sql: str = f"INSERT INTO {table_identifier} VALUES ({placeholders})"
            _logger.debug("sql: %s", sql)
            parameters: List[List[Any]] = _db_utils.extract_parameters(df=df)
            cursor.executemany(sql, parameters)
            con.commit()
    except Exception as ex:
        con.rollback()
        _logger.error(ex)
        raise
