"""Amazon MySQL Module."""

import logging
import uuid
from typing import Any, Dict, Iterator, List, Optional, Tuple, Type, Union

import boto3
import pandas as pd
import pyarrow as pa
import pymysql
from pymysql.cursors import Cursor

from awswrangler import _data_types
from awswrangler import _databases as _db_utils
from awswrangler import exceptions
from awswrangler._config import apply_configs

_logger: logging.Logger = logging.getLogger(__name__)


def _validate_connection(con: "pymysql.connections.Connection[Any]") -> None:
    if not isinstance(con, pymysql.connections.Connection):
        raise exceptions.InvalidConnection(
            "Invalid 'conn' argument, please pass a "
            "pymysql.connections.Connection object. Use pymysql.connect() to use "
            "credentials directly or wr.mysql.connect() to fetch it from the Glue Catalog."
        )


def _drop_table(cursor: Cursor, schema: Optional[str], table: str) -> None:
    schema_str = f"`{schema}`." if schema else ""
    sql = f"DROP TABLE IF EXISTS {schema_str}`{table}`"
    _logger.debug("Drop table query:\n%s", sql)
    cursor.execute(sql)


def _does_table_exist(cursor: Cursor, schema: Optional[str], table: str) -> bool:
    schema_str = f"TABLE_SCHEMA = '{schema}' AND" if schema else ""
    cursor.execute(f"SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE " f"{schema_str} TABLE_NAME = '{table}'")
    return len(cursor.fetchall()) > 0


def _create_table(
    df: pd.DataFrame,
    cursor: Cursor,
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
    mysql_types: Dict[str, str] = _data_types.database_types_from_pandas(
        df=df,
        index=index,
        dtype=dtype,
        varchar_lengths_default="TEXT",
        varchar_lengths=varchar_lengths,
        converter_func=_data_types.pyarrow2mysql,
    )
    cols_str: str = "".join([f"`{k}` {v},\n" for k, v in mysql_types.items()])[:-2]
    sql = f"CREATE TABLE IF NOT EXISTS `{schema}`.`{table}` (\n{cols_str})"
    _logger.debug("Create table query:\n%s", sql)
    cursor.execute(sql)


def connect(
    connection: Optional[str] = None,
    secret_id: Optional[str] = None,
    catalog_id: Optional[str] = None,
    dbname: Optional[str] = None,
    boto3_session: Optional[boto3.Session] = None,
    read_timeout: Optional[int] = None,
    write_timeout: Optional[int] = None,
    connect_timeout: int = 10,
    cursorclass: Type[Cursor] = Cursor,
) -> "pymysql.connections.Connection[Any]":
    """Return a pymysql connection from a Glue Catalog Connection or Secrets Manager.

    https://pymysql.readthedocs.io

    Note
    ----
    You MUST pass a `connection` OR `secret_id`.
    Here is an example of the secret structure in Secrets Manager:
    {
    "host":"mysql-instance-wrangler.dr8vkeyrb9m1.us-east-1.rds.amazonaws.com",
    "username":"test",
    "password":"test",
    "engine":"mysql",
    "port":"3306",
    "dbname": "mydb" # Optional
    }

    Note
    ----
    It is only possible to configure SSL using Glue Catalog Connection. More at:
    https://docs.aws.amazon.com/glue/latest/dg/connection-defining.html

    Note
    ----
    Consider using SSCursor `cursorclass` for queries that return a lot of data. More at:
    https://pymysql.readthedocs.io/en/latest/modules/cursors.html#pymysql.cursors.SSCursor

    Parameters
    ----------
    connection : str
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
    read_timeout: Optional[int]
        The timeout for reading from the connection in seconds (default: None - no timeout).
        This parameter is forward to pymysql.
        https://pymysql.readthedocs.io/en/latest/modules/connections.html
    write_timeout: Optional[int]
        The timeout for writing to the connection in seconds (default: None - no timeout)
        This parameter is forward to pymysql.
        https://pymysql.readthedocs.io/en/latest/modules/connections.html
    connect_timeout: int
        Timeout before throwing an exception when connecting.
        (default: 10, min: 1, max: 31536000)
        This parameter is forward to pymysql.
        https://pymysql.readthedocs.io/en/latest/modules/connections.html
    cursorclass : Cursor
        Cursor class to use, e.g. SSCursor; defaults to :class:`pymysql.cursors.Cursor`
        https://pymysql.readthedocs.io/en/latest/modules/cursors.html

    Returns
    -------
    pymysql.connections.Connection
        pymysql connection.

    Examples
    --------
    >>> import awswrangler as wr
    >>> con = wr.mysql.connect("MY_GLUE_CONNECTION")
    >>> with con.cursor() as cursor:
    >>>     cursor.execute("SELECT 1")
    >>>     print(cursor.fetchall())
    >>> con.close()

    """
    attrs: _db_utils.ConnectionAttributes = _db_utils.get_connection_attributes(
        connection=connection, secret_id=secret_id, catalog_id=catalog_id, dbname=dbname, boto3_session=boto3_session
    )
    if attrs.kind != "mysql":
        raise exceptions.InvalidDatabaseType(f"Invalid connection type ({attrs.kind}. It must be a MySQL connection.)")
    return pymysql.connect(
        user=attrs.user,
        database=attrs.database,
        password=attrs.password,
        port=attrs.port,
        host=attrs.host,
        ssl=attrs.ssl_context,  # type: ignore
        read_timeout=read_timeout,
        write_timeout=write_timeout,
        connect_timeout=connect_timeout,
        cursorclass=cursorclass,
    )


def read_sql_query(
    sql: str,
    con: "pymysql.connections.Connection[Any]",
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
    con : pymysql.connections.Connection
        Use pymysql.connect() to use credentials directly or wr.mysql.connect() to fetch it from the Glue Catalog.
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
    Reading from MySQL using a Glue Catalog Connections

    >>> import awswrangler as wr
    >>> con = wr.mysql.connect("MY_GLUE_CONNECTION")
    >>> df = wr.mysql.read_sql_query(
    ...     sql="SELECT * FROM test.my_table",
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
    con: "pymysql.connections.Connection[Any]",
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
    con : pymysql.connections.Connection
        Use pymysql.connect() to use credentials directly or wr.mysql.connect() to fetch it from the Glue Catalog.
    schema : str, optional
        Name of SQL schema in database to query.
        Uses default schema if None.
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
    Reading from MySQL using a Glue Catalog Connections

    >>> import awswrangler as wr
    >>> con = wr.mysql.connect("MY_GLUE_CONNECTION")
    >>> df = wr.mysql.read_sql_table(
    ...     table="my_table",
    ...     schema="test",
    ...     con=con
    ... )
    >>> con.close()

    """
    sql: str = f"SELECT * FROM `{table}`" if schema is None else f"SELECT * FROM `{schema}`.`{table}`"
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
    con: "pymysql.connections.Connection[Any]",
    table: str,
    schema: str,
    mode: str = "append",
    index: bool = False,
    dtype: Optional[Dict[str, str]] = None,
    varchar_lengths: Optional[Dict[str, int]] = None,
    use_column_names: bool = False,
    chunksize: int = 200,
    cursorclass: Type[Cursor] = Cursor,
) -> None:
    """Write records stored in a DataFrame into MySQL.

    Parameters
    ----------
    df : pandas.DataFrame
        Pandas DataFrame https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html
    con : pymysql.connections.Connection
        Use pymysql.connect() to use credentials directly or wr.mysql.connect() to fetch it from the Glue Catalog.
    table : str
        Table name
    schema : str
        Schema name
    mode : str
        Append, overwrite, upsert_duplicate_key, upsert_replace_into, upsert_distinct, ignore.
            append: Inserts new records into table.
            overwrite: Drops table and recreates.
            upsert_duplicate_key: Performs an upsert using `ON DUPLICATE KEY` clause. Requires table schema to have
            defined keys, otherwise duplicate records will be inserted.
            upsert_replace_into: Performs upsert using `REPLACE INTO` clause. Less efficient and still requires the
            table schema to have keys or else duplicate records will be inserted
            upsert_distinct: Inserts new records, including duplicates, then recreates the table and inserts `DISTINCT`
            records from old table. This is the least efficient approach but handles scenarios where there are no
            keys on table.
            ignore: Inserts new records into table using `INSERT IGNORE` clause.

    index : bool
        True to store the DataFrame index as a column in the table,
        otherwise False to ignore it.
    dtype: Dict[str, str], optional
        Dictionary of columns names and MySQL types to be casted.
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
    cursorclass : Cursor
        Cursor class to use, e.g. SSCrusor; defaults to :class:`pymysql.cursors.Cursor`
        https://pymysql.readthedocs.io/en/latest/modules/cursors.html

    Returns
    -------
    None
        None.

    Examples
    --------
    Writing to MySQL using a Glue Catalog Connections

    >>> import awswrangler as wr
    >>> con = wr.mysql.connect("MY_GLUE_CONNECTION")
    >>> wr.mysql.to_sql(
    ...     df=df,
    ...     table="my_table",
    ...     schema="test",
    ...     con=con
    ... )
    >>> con.close()

    """
    if df.empty is True:
        raise exceptions.EmptyDataFrame("DataFrame cannot be empty.")

    mode = mode.strip().lower()
    allowed_modes = [
        "append",
        "overwrite",
        "upsert_replace_into",
        "upsert_duplicate_key",
        "upsert_distinct",
        "ignore",
    ]
    _db_utils.validate_mode(mode=mode, allowed_modes=allowed_modes)
    _validate_connection(con=con)
    try:
        with con.cursor(cursor=cursorclass) as cursor:  # type: ignore
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
            insertion_columns = ""
            upsert_columns = ""
            upsert_str = ""
            ignore_str = " IGNORE" if mode == "ignore" else ""
            if use_column_names:
                insertion_columns = f"(`{'`, `'.join(df.columns)}`)"
            if mode == "upsert_duplicate_key":
                upsert_columns = ", ".join(df.columns.map(lambda column: f"`{column}`=VALUES(`{column}`)"))
                upsert_str = f" ON DUPLICATE KEY UPDATE {upsert_columns}"
            placeholder_parameter_pair_generator = _db_utils.generate_placeholder_parameter_pairs(
                df=df, column_placeholders=column_placeholders, chunksize=chunksize
            )
            sql: str
            for placeholders, parameters in placeholder_parameter_pair_generator:
                if mode == "upsert_replace_into":
                    sql = f"REPLACE INTO `{schema}`.`{table}` {insertion_columns} VALUES {placeholders}"
                else:
                    sql = f"""INSERT{ignore_str} INTO `{schema}`.`{table}` {insertion_columns}
VALUES {placeholders}{upsert_str}"""
                _logger.debug("sql: %s", sql)
                cursor.executemany(sql, (parameters,))
            con.commit()
            if mode == "upsert_distinct":
                temp_table = f"{table}_{uuid.uuid4().hex}"
                cursor.execute(f"CREATE TABLE `{schema}`.`{temp_table}` LIKE `{schema}`.`{table}`")
                cursor.execute(f"INSERT INTO `{schema}`.`{temp_table}` SELECT DISTINCT * FROM `{schema}`.`{table}`")
                cursor.execute(f"DROP TABLE IF EXISTS `{schema}`.`{table}`")
                cursor.execute(f"ALTER TABLE `{schema}`.`{temp_table}` RENAME TO `{table}`")
                con.commit()

    except Exception as ex:
        con.rollback()
        _logger.error(ex)
        raise
