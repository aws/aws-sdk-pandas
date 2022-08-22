"""Amazon PostgreSQL Module."""

import logging
from ssl import SSLContext
from typing import Any, Dict, Iterator, List, Optional, Tuple, Union

import boto3
import pandas as pd
import pg8000
import pyarrow as pa

from awswrangler import _data_types
from awswrangler import _databases as _db_utils
from awswrangler import exceptions
from awswrangler._config import apply_configs

_logger: logging.Logger = logging.getLogger(__name__)


def _validate_connection(con: pg8000.Connection) -> None:
    if not isinstance(con, pg8000.Connection):
        raise exceptions.InvalidConnection(
            "Invalid 'conn' argument, please pass a "
            "pg8000.Connection object. Use pg8000.connect() to use "
            "credentials directly or wr.postgresql.connect() to fetch it from the Glue Catalog."
        )


def _drop_table(cursor: pg8000.Cursor, schema: Optional[str], table: str) -> None:
    schema_str = f'"{schema}".' if schema else ""
    sql = f'DROP TABLE IF EXISTS {schema_str}"{table}"'
    _logger.debug("Drop table query:\n%s", sql)
    cursor.execute(sql)


def _does_table_exist(cursor: pg8000.Cursor, schema: Optional[str], table: str) -> bool:
    schema_str = f"TABLE_SCHEMA = '{schema}' AND" if schema else ""
    cursor.execute(
        f"SELECT true WHERE EXISTS ("
        f"SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE "
        f"{schema_str} TABLE_NAME = '{table}'"
        f");"
    )
    return len(cursor.fetchall()) > 0


def _create_table(
    df: pd.DataFrame,
    cursor: pg8000.Cursor,
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
    postgresql_types: Dict[str, str] = _data_types.database_types_from_pandas(
        df=df,
        index=index,
        dtype=dtype,
        varchar_lengths_default="TEXT",
        varchar_lengths=varchar_lengths,
        converter_func=_data_types.pyarrow2postgresql,
    )
    cols_str: str = "".join([f'"{k}" {v},\n' for k, v in postgresql_types.items()])[:-2]
    sql = f'CREATE TABLE IF NOT EXISTS "{schema}"."{table}" (\n{cols_str})'
    _logger.debug("Create table query:\n%s", sql)
    cursor.execute(sql)


def connect(
    connection: Optional[str] = None,
    secret_id: Optional[str] = None,
    catalog_id: Optional[str] = None,
    dbname: Optional[str] = None,
    boto3_session: Optional[boto3.Session] = None,
    ssl_context: Optional[Union[bool, SSLContext]] = None,
    timeout: Optional[int] = None,
    tcp_keepalive: bool = True,
) -> pg8000.Connection:
    """Return a pg8000 connection from a Glue Catalog Connection.

    https://github.com/tlocke/pg8000

    Note
    ----
    You MUST pass a `connection` OR `secret_id`.
    Here is an example of the secret structure in Secrets Manager:
    {
    "host":"postgresql-instance-wrangler.dr8vkeyrb9m1.us-east-1.rds.amazonaws.com",
    "username":"test",
    "password":"test",
    "engine":"postgresql",
    "port":"3306",
    "dbname": "mydb" # Optional
    }

    Parameters
    ----------
    connection : Optional[str]
        Glue Catalog Connection name.
    secret_id: Optional[str]:
        Specifies the secret containing the connection details that you want to retrieve.
        You can specify either the Amazon Resource Name (ARN) or the friendly name of the secret.
    catalog_id : str, optional
        The ID of the Data Catalog.
        If none is provided, the AWS account ID is used by default.
    dbname: Optional[str]
        Optional database name to overwrite the stored one.
    boto3_session : boto3.Session(), optional
        Boto3 Session. The default boto3 session will be used if boto3_session receive None.
    ssl_context: Optional[Union[bool, SSLContext]]
        This governs SSL encryption for TCP/IP sockets.
        This parameter is forward to pg8000.
        https://github.com/tlocke/pg8000#functions
    timeout: Optional[int]
        This is the time in seconds before the connection to the server will time out.
        The default is None which means no timeout.
        This parameter is forward to pg8000.
        https://github.com/tlocke/pg8000#functions
    tcp_keepalive: bool
        If True then use TCP keepalive. The default is True.
        This parameter is forward to pg8000.
        https://github.com/tlocke/pg8000#functions

    Returns
    -------
    pg8000.Connection
        pg8000 connection.

    Examples
    --------
    >>> import awswrangler as wr
    >>> con = wr.postgresql.connect("MY_GLUE_CONNECTION")
    >>> with con.cursor() as cursor:
    >>>     cursor.execute("SELECT 1")
    >>>     print(cursor.fetchall())
    >>> con.close()

    """
    attrs: _db_utils.ConnectionAttributes = _db_utils.get_connection_attributes(
        connection=connection, secret_id=secret_id, catalog_id=catalog_id, dbname=dbname, boto3_session=boto3_session
    )
    if attrs.kind not in ("postgresql", "postgres"):
        raise exceptions.InvalidDatabaseType(
            f"Invalid connection type ({attrs.kind}. It must be a postgresql connection.)"
        )
    return pg8000.connect(
        user=attrs.user,
        database=attrs.database,
        password=attrs.password,
        port=attrs.port,
        host=attrs.host,
        ssl_context=ssl_context,
        timeout=timeout,
        tcp_keepalive=tcp_keepalive,
    )


def read_sql_query(
    sql: str,
    con: pg8000.Connection,
    index_col: Optional[Union[str, List[str]]] = None,
    params: Optional[Union[List[Any], Tuple[Any, ...], Dict[Any, Any]]] = None,
    chunksize: Optional[int] = None,
    dtype: Optional[Dict[str, pa.DataType]] = None,
    safe: bool = True,
    timestamp_as_object: bool = False,
) -> Union[pd.DataFrame, Iterator[pd.DataFrame]]:
    """Return a DataFrame corresponding to the result set of the query string.

    Parameters
    ----------
    sql : str
        SQL query.
    con : pg8000.Connection
        Use pg8000.connect() to use credentials directly or wr.postgresql.connect() to fetch it from the Glue Catalog.
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
    timestamp_as_object : bool
        Cast non-nanosecond timestamps (np.datetime64) to objects.

    Returns
    -------
    Union[pandas.DataFrame, Iterator[pandas.DataFrame]]
        Result as Pandas DataFrame(s).

    Examples
    --------
    Reading from PostgreSQL using a Glue Catalog Connections

    >>> import awswrangler as wr
    >>> con = wr.postgresql.connect("MY_GLUE_CONNECTION")
    >>> df = wr.postgresql.read_sql_query(
    ...     sql="SELECT * FROM public.my_table",
    ...     con=con
    ... )
    >>> con.close()

    """
    _validate_connection(con=con)
    return _db_utils.read_sql_query(
        sql=sql,
        con=con,
        index_col=index_col,
        params=params,
        chunksize=chunksize,
        dtype=dtype,
        safe=safe,
        timestamp_as_object=timestamp_as_object,
    )


def read_sql_table(
    table: str,
    con: pg8000.Connection,
    schema: Optional[str] = None,
    index_col: Optional[Union[str, List[str]]] = None,
    params: Optional[Union[List[Any], Tuple[Any, ...], Dict[Any, Any]]] = None,
    chunksize: Optional[int] = None,
    dtype: Optional[Dict[str, pa.DataType]] = None,
    safe: bool = True,
    timestamp_as_object: bool = False,
) -> Union[pd.DataFrame, Iterator[pd.DataFrame]]:
    """Return a DataFrame corresponding the table.

    Parameters
    ----------
    table : str
        Table name.
    con : pg8000.Connection
        Use pg8000.connect() to use credentials directly or wr.postgresql.connect() to fetch it from the Glue Catalog.
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
    timestamp_as_object : bool
        Cast non-nanosecond timestamps (np.datetime64) to objects.

    Returns
    -------
    Union[pandas.DataFrame, Iterator[pandas.DataFrame]]
        Result as Pandas DataFrame(s).

    Examples
    --------
    Reading from PostgreSQL using a Glue Catalog Connections

    >>> import awswrangler as wr
    >>> con = wr.postgresql.connect("MY_GLUE_CONNECTION")
    >>> df = wr.postgresql.read_sql_table(
    ...     table="my_table",
    ...     schema="public",
    ...     con=con
    ... )
    >>> con.close()

    """
    sql: str = f'SELECT * FROM "{table}"' if schema is None else f'SELECT * FROM "{schema}"."{table}"'
    return read_sql_query(
        sql=sql,
        con=con,
        index_col=index_col,
        params=params,
        chunksize=chunksize,
        dtype=dtype,
        safe=safe,
        timestamp_as_object=timestamp_as_object,
    )


@apply_configs
def to_sql(
    df: pd.DataFrame,
    con: pg8000.Connection,
    table: str,
    schema: str,
    mode: str = "append",
    index: bool = False,
    dtype: Optional[Dict[str, str]] = None,
    varchar_lengths: Optional[Dict[str, int]] = None,
    use_column_names: bool = False,
    chunksize: int = 200,
    upsert_conflict_columns: Optional[List[str]] = None,
    insert_conflict_columns: Optional[List[str]] = None,
) -> None:
    """Write records stored in a DataFrame into PostgreSQL.

    Parameters
    ----------
    df : pandas.DataFrame
        Pandas DataFrame https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html
    con : pg8000.Connection
        Use pg8000.connect() to use credentials directly or wr.postgresql.connect() to fetch it from the Glue Catalog.
    table : str
        Table name
    schema : str
        Schema name
    mode : str
        Append, overwrite or upsert.
            append: Inserts new records into table.
            overwrite: Drops table and recreates.
            upsert: Perform an upsert which checks for conflicts on columns given by `upsert_conflict_columns` and
            sets the new values on conflicts. Note that `upsert_conflict_columns` is required for this mode.
    index : bool
        True to store the DataFrame index as a column in the table,
        otherwise False to ignore it.
    dtype: Dict[str, str], optional
        Dictionary of columns names and PostgreSQL types to be casted.
        Useful when you have columns with undetermined or mixed data types.
        (e.g. {'col name': 'TEXT', 'col2 name': 'FLOAT'})
    varchar_lengths : Dict[str, int], optional
        Dict of VARCHAR length by columns. (e.g. {"col1": 10, "col5": 200}).
    use_column_names: bool
        If set to True, will use the column names of the DataFrame for generating the INSERT SQL Query.
        E.g. If the DataFrame has two columns `col1` and `col3` and `use_column_names` is True, data will only be
        inserted into the database columns `col1` and `col3`.
    chunksize: int
        Number of rows which are inserted with each SQL query. Defaults to inserting 200 rows per query.
    upsert_conflict_columns: List[str], optional
        This parameter is only supported if `mode` is set top `upsert`. In this case conflicts for the given columns are
        checked for evaluating the upsert.
    insert_conflict_columns: List[str], optional
        This parameter is only supported if `mode` is set top `append`. In this case conflicts for the given columns are
        checked for evaluating the insert 'ON CONFLICT DO NOTHING'.

    Returns
    -------
    None
        None.

    Examples
    --------
    Writing to PostgreSQL using a Glue Catalog Connections

    >>> import awswrangler as wr
    >>> con = wr.postgresql.connect("MY_GLUE_CONNECTION")
    >>> wr.postgresql.to_sql(
    ...     df=df,
    ...     table="my_table",
    ...     schema="public",
    ...     con=con
    ... )
    >>> con.close()

    """
    if df.empty is True:
        raise exceptions.EmptyDataFrame("DataFrame cannot be empty.")

    mode = mode.strip().lower()
    allowed_modes = ["append", "overwrite", "upsert"]
    _db_utils.validate_mode(mode=mode, allowed_modes=allowed_modes)
    if mode == "upsert" and not upsert_conflict_columns:
        raise exceptions.InvalidArgumentValue("<upsert_conflict_columns> needs to be set when using upsert mode.")
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
            column_placeholders: str = ", ".join(["%s"] * len(df.columns))
            column_names = [f'"{column}"' for column in df.columns]
            insertion_columns = ""
            upsert_str = ""
            if use_column_names:
                insertion_columns = f"({', '.join(column_names)})"
            if mode == "upsert":
                upsert_columns = ", ".join(f"{column}=EXCLUDED.{column}" for column in column_names)
                conflict_columns = ", ".join(upsert_conflict_columns)  # type: ignore
                upsert_str = f" ON CONFLICT ({conflict_columns}) DO UPDATE SET {upsert_columns}"
            if mode == "append" and insert_conflict_columns:
                conflict_columns = ", ".join(insert_conflict_columns)  # type: ignore
                upsert_str = f" ON CONFLICT ({conflict_columns}) DO NOTHING"
            placeholder_parameter_pair_generator = _db_utils.generate_placeholder_parameter_pairs(
                df=df, column_placeholders=column_placeholders, chunksize=chunksize
            )
            for placeholders, parameters in placeholder_parameter_pair_generator:
                sql: str = f'INSERT INTO "{schema}"."{table}" {insertion_columns} VALUES {placeholders}{upsert_str}'
                _logger.debug("sql: %s", sql)
                cursor.executemany(sql, (parameters,))
            con.commit()
    except Exception as ex:
        con.rollback()
        _logger.error(ex)
        raise
