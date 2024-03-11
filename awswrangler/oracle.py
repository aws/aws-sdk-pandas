# mypy: disable-error-code=name-defined
"""Amazon Oracle Database Module."""

from __future__ import annotations

import logging
from decimal import Decimal
from typing import (
    Any,
    Callable,
    Iterator,
    Literal,
    TypeVar,
    overload,
)

import boto3
import pyarrow as pa

import awswrangler.pandas as pd
from awswrangler import _data_types, _utils, exceptions
from awswrangler import _databases as _db_utils
from awswrangler._config import apply_configs
from awswrangler._sql_utils import identifier

__all__ = ["connect", "read_sql_query", "read_sql_table", "to_sql"]

oracledb = _utils.import_optional_dependency("oracledb")

_logger: logging.Logger = logging.getLogger(__name__)
FuncT = TypeVar("FuncT", bound=Callable[..., Any])


def _validate_connection(con: "oracledb.Connection") -> None:
    if not isinstance(con, oracledb.Connection):
        raise exceptions.InvalidConnection(
            "Invalid 'conn' argument, please pass a "
            "oracledb.Connection object. Use oracledb.connect() to use "
            "credentials directly or wr.oracle.connect() to fetch it from the Glue Catalog."
        )


def _get_table_identifier(schema: str | None, table: str) -> str:
    schema_str = f'{identifier(schema, sql_mode="ansi")}.' if schema else ""
    table_identifier = f'{schema_str}{identifier(table, sql_mode="ansi")}'
    return table_identifier


def _drop_table(cursor: "oracledb.Cursor", schema: str | None, table: str) -> None:
    table_identifier = _get_table_identifier(schema, table)
    sql = f"""
BEGIN
    EXECUTE IMMEDIATE 'DROP TABLE {table_identifier}';
EXCEPTION
    WHEN OTHERS THEN
        IF SQLCODE != -942 THEN
            RAISE;
        END IF;
END;
"""
    _logger.debug("Drop table query:\n%s", sql)
    cursor.execute(sql)


def _does_table_exist(cursor: "oracledb.Cursor", schema: str | None, table: str) -> bool:
    if schema:
        cursor.execute(
            "SELECT * FROM ALL_TABLES WHERE OWNER = :db_schema AND TABLE_NAME = :db_table",
            db_schema=schema,
            db_table=table,
        )
    else:
        cursor.execute("SELECT * FROM ALL_TABLES WHERE TABLE_NAME = :tbl", tbl=table)
    return len(cursor.fetchall()) > 0


def _create_table(
    df: pd.DataFrame,
    cursor: "oracledb.Cursor",
    table: str,
    schema: str,
    mode: str,
    index: bool,
    dtype: dict[str, str] | None,
    varchar_lengths: dict[str, int] | None,
    primary_keys: list[str] | None,
) -> None:
    if mode == "overwrite":
        _drop_table(cursor=cursor, schema=schema, table=table)
    elif _does_table_exist(cursor=cursor, schema=schema, table=table):
        return
    oracle_types: dict[str, str] = _data_types.database_types_from_pandas(
        df=df,
        index=index,
        dtype=dtype,
        varchar_lengths_default="CLOB",
        varchar_lengths=varchar_lengths,
        converter_func=_data_types.pyarrow2oracle,
    )
    cols_str: str = "".join([f'{identifier(k, sql_mode="ansi")} {v},\n' for k, v in oracle_types.items()])[:-2]

    if primary_keys:
        primary_keys_str = ", ".join([f'{identifier(k, sql_mode="ansi")}' for k in primary_keys])
    else:
        primary_keys_str = None

    table_identifier = _get_table_identifier(schema, table)
    create_table_params: str = f"\n{cols_str}"
    if primary_keys_str:
        create_table_params += f",\nPRIMARY KEY ({primary_keys_str})"

    sql = f"CREATE TABLE {table_identifier} ({create_table_params})"
    _logger.debug("Create table query:\n%s", sql)
    cursor.execute(sql)


@_utils.check_optional_dependency(oracledb, "oracledb")
def connect(
    connection: str | None = None,
    secret_id: str | None = None,
    catalog_id: str | None = None,
    dbname: str | None = None,
    boto3_session: boto3.Session | None = None,
    call_timeout: int | None = 0,
) -> "oracledb.Connection":
    """Return a oracledb connection from a Glue Catalog Connection.

    https://github.com/oracle/python-oracledb

    Note
    ----
    You MUST pass a `connection` OR `secret_id`.
    Here is an example of the secret structure in Secrets Manager:
    {
    "host":"oracle-instance-wrangler.cr4trrvge8rz.us-east-1.rds.amazonaws.com",
    "username":"test",
    "password":"test",
    "engine":"oracle",
    "port":"1521",
    "dbname": "mydb" # Optional
    }

    Parameters
    ----------
    connection: str, optional
        Glue Catalog Connection name.
    secret_id: str, optional
        Specifies the secret containing the connection details that you want to retrieve.
        You can specify either the Amazon Resource Name (ARN) or the friendly name of the secret.
    catalog_id: str, optional
        The ID of the Data Catalog.
        If none is provided, the AWS account ID is used by default.
    dbname: str, optional
        Optional database name to overwrite the stored one.
    boto3_session : boto3.Session(), optional
        Boto3 Session. The default boto3 session will be used if boto3_session receive None.
    call_timeout: int, optional
        This is the time in milliseconds that a single round-trip to the database may take before a timeout will occur.
        The default is None which means no timeout.
        This parameter is forwarded to oracledb.
        https://cx-oracle.readthedocs.io/en/latest/api_manual/connection.html#Connection.call_timeout

    Returns
    -------
    oracledb.Connection
        oracledb connection.

    Examples
    --------
    >>> import awswrangler as wr
    >>> con = wr.oracle.connect(connection="MY_GLUE_CONNECTION")
    >>> with con.cursor() as cursor:
    >>>     cursor.execute("SELECT 1 FROM DUAL")
    >>>     print(cursor.fetchall())
    >>> con.close()

    """
    attrs: _db_utils.ConnectionAttributes = _db_utils.get_connection_attributes(
        connection=connection, secret_id=secret_id, catalog_id=catalog_id, dbname=dbname, boto3_session=boto3_session
    )
    if attrs.kind != "oracle":
        raise exceptions.InvalidDatabaseType(
            f"Invalid connection type ({attrs.kind}. It must be an oracle connection.)"
        )

    connection_dsn = oracledb.makedsn(attrs.host, attrs.port, service_name=attrs.database)
    _logger.debug("DSN: %s", connection_dsn)
    oracle_connection = oracledb.connect(
        user=attrs.user,
        password=attrs.password,
        dsn=connection_dsn,
    )
    # oracledb.connect does not have a call_timeout attribute, it has to be set separatly
    oracle_connection.call_timeout = call_timeout
    return oracle_connection


@overload
def read_sql_query(
    sql: str,
    con: "oracledb.Connection",
    index_col: str | list[str] | None = ...,
    params: list[Any] | tuple[Any, ...] | dict[Any, Any] | None = ...,
    chunksize: None = ...,
    dtype: dict[str, pa.DataType] | None = ...,
    safe: bool = ...,
    timestamp_as_object: bool = ...,
    dtype_backend: Literal["numpy_nullable", "pyarrow"] = ...,
) -> pd.DataFrame: ...


@overload
def read_sql_query(
    sql: str,
    con: "oracledb.Connection",
    *,
    index_col: str | list[str] | None = ...,
    params: list[Any] | tuple[Any, ...] | dict[Any, Any] | None = ...,
    chunksize: int,
    dtype: dict[str, pa.DataType] | None = ...,
    safe: bool = ...,
    timestamp_as_object: bool = ...,
    dtype_backend: Literal["numpy_nullable", "pyarrow"] = ...,
) -> Iterator[pd.DataFrame]: ...


@overload
def read_sql_query(
    sql: str,
    con: "oracledb.Connection",
    *,
    index_col: str | list[str] | None = ...,
    params: list[Any] | tuple[Any, ...] | dict[Any, Any] | None = ...,
    chunksize: int | None,
    dtype: dict[str, pa.DataType] | None = ...,
    safe: bool = ...,
    timestamp_as_object: bool = ...,
    dtype_backend: Literal["numpy_nullable", "pyarrow"] = ...,
) -> pd.DataFrame | Iterator[pd.DataFrame]: ...


@_utils.check_optional_dependency(oracledb, "oracledb")
def read_sql_query(
    sql: str,
    con: "oracledb.Connection",
    index_col: str | list[str] | None = None,
    params: list[Any] | tuple[Any, ...] | dict[Any, Any] | None = None,
    chunksize: int | None = None,
    dtype: dict[str, pa.DataType] | None = None,
    safe: bool = True,
    timestamp_as_object: bool = False,
    dtype_backend: Literal["numpy_nullable", "pyarrow"] = "numpy_nullable",
) -> pd.DataFrame | Iterator[pd.DataFrame]:
    """Return a DataFrame corresponding to the result set of the query string.

    Parameters
    ----------
    sql : str
        SQL query.
    con : oracledb.Connection
        Use oracledb.connect() to use credentials directly or wr.oracle.connect() to fetch it from the Glue Catalog.
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
    dtype_backend: str, optional
        Which dtype_backend to use, e.g. whether a DataFrame should have NumPy arrays,
        nullable dtypes are used for all dtypes that have a nullable implementation when
        “numpy_nullable” is set, pyarrow is used for all dtypes if “pyarrow” is set.

        The dtype_backends are still experimential. The "pyarrow" backend is only supported with Pandas 2.0 or above.

    Returns
    -------
    Union[pandas.DataFrame, Iterator[pandas.DataFrame]]
        Result as Pandas DataFrame(s).

    Examples
    --------
    Reading from Oracle Database using a Glue Catalog Connections

    >>> import awswrangler as wr
    >>> con = wr.oracle.connect(connection="MY_GLUE_CONNECTION")
    >>> df = wr.oracle.read_sql_query(
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
        dtype_backend=dtype_backend,
    )


@overload
def read_sql_table(
    table: str,
    con: "oracledb.Connection",
    schema: str | None = ...,
    index_col: str | list[str] | None = ...,
    params: list[Any] | tuple[Any, ...] | dict[Any, Any] | None = ...,
    chunksize: None = ...,
    dtype: dict[str, pa.DataType] | None = ...,
    safe: bool = ...,
    timestamp_as_object: bool = ...,
    dtype_backend: Literal["numpy_nullable", "pyarrow"] = ...,
) -> pd.DataFrame: ...


@overload
def read_sql_table(
    table: str,
    con: "oracledb.Connection",
    *,
    schema: str | None = ...,
    index_col: str | list[str] | None = ...,
    params: list[Any] | tuple[Any, ...] | dict[Any, Any] | None = ...,
    chunksize: int,
    dtype: dict[str, pa.DataType] | None = ...,
    safe: bool = ...,
    timestamp_as_object: bool = ...,
    dtype_backend: Literal["numpy_nullable", "pyarrow"] = ...,
) -> Iterator[pd.DataFrame]: ...


@overload
def read_sql_table(
    table: str,
    con: "oracledb.Connection",
    *,
    schema: str | None = ...,
    index_col: str | list[str] | None = ...,
    params: list[Any] | tuple[Any, ...] | dict[Any, Any] | None = ...,
    chunksize: int | None,
    dtype: dict[str, pa.DataType] | None = ...,
    safe: bool = ...,
    timestamp_as_object: bool = ...,
    dtype_backend: Literal["numpy_nullable", "pyarrow"] = ...,
) -> pd.DataFrame | Iterator[pd.DataFrame]: ...


@_utils.check_optional_dependency(oracledb, "oracledb")
def read_sql_table(
    table: str,
    con: "oracledb.Connection",
    schema: str | None = None,
    index_col: str | list[str] | None = None,
    params: list[Any] | tuple[Any, ...] | dict[Any, Any] | None = None,
    chunksize: int | None = None,
    dtype: dict[str, pa.DataType] | None = None,
    safe: bool = True,
    timestamp_as_object: bool = False,
    dtype_backend: Literal["numpy_nullable", "pyarrow"] = "numpy_nullable",
) -> pd.DataFrame | Iterator[pd.DataFrame]:
    """Return a DataFrame corresponding the table.

    Parameters
    ----------
    table : str
        Table name.
    con : oracledb.Connection
        Use oracledb.connect() to use credentials directly or wr.oracle.connect() to fetch it from the Glue Catalog.
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
    dtype_backend: str, optional
        Which dtype_backend to use, e.g. whether a DataFrame should have NumPy arrays,
        nullable dtypes are used for all dtypes that have a nullable implementation when
        “numpy_nullable” is set, pyarrow is used for all dtypes if “pyarrow” is set.

        The dtype_backends are still experimential. The "pyarrow" backend is only supported with Pandas 2.0 or above.

    Returns
    -------
    Union[pandas.DataFrame, Iterator[pandas.DataFrame]]
        Result as Pandas DataFrame(s).

    Examples
    --------
    Reading from Oracle Database using a Glue Catalog Connections

    >>> import awswrangler as wr
    >>> con = wr.oracle.connect(connection="MY_GLUE_CONNECTION")
    >>> df = wr.oracle.read_sql_table(
    ...     table="my_table",
    ...     schema="test",
    ...     con=con
    ... )
    >>> con.close()
    """
    table_identifier = _get_table_identifier(schema, table)
    sql: str = f"SELECT * FROM {table_identifier}"
    return read_sql_query(
        sql=sql,
        con=con,
        index_col=index_col,
        params=params,
        chunksize=chunksize,
        dtype=dtype,
        safe=safe,
        timestamp_as_object=timestamp_as_object,
        dtype_backend=dtype_backend,
    )


def _generate_insert_statement(
    table_identifier: str,
    df: pd.DataFrame,
    use_column_names: bool,
) -> str:
    column_placeholders: str = f"({', '.join([':' + str(i + 1) for i in range(len(df.columns))])})"

    if use_column_names:
        insertion_columns = "(" + ", ".join(identifier(column, sql_mode="ansi") for column in df.columns) + ")"
    else:
        insertion_columns = ""

    return f"INSERT INTO {table_identifier} {insertion_columns} VALUES {column_placeholders}"


def _generate_upsert_statement(
    table_identifier: str,
    df: pd.DataFrame,
    use_column_names: bool,
    primary_keys: list[str] | None,
) -> str:
    if use_column_names is False:
        raise exceptions.InvalidArgumentCombination('`use_column_names` has to be True when `mode="upsert"`')
    if not primary_keys:
        raise exceptions.InvalidArgumentCombination('`primary_keys` need to be defined when `mode="upsert"`')

    non_primary_key_columns = [key for key in df.columns if key not in set(primary_keys)]

    primary_keys_str = ", ".join([f'{identifier(key, sql_mode="ansi")}' for key in primary_keys])
    columns_str = ", ".join([f'{identifier(key, sql_mode="ansi")}' for key in non_primary_key_columns])

    column_placeholders: str = f"({', '.join([':' + str(i + 1) for i in range(len(df.columns))])})"

    primary_key_condition_str = " AND ".join(
        [f'{identifier(key, sql_mode="ansi")} = :{i+1}' for i, key in enumerate(primary_keys)]
    )
    assignment_str = ", ".join(
        [
            f'{identifier(col, sql_mode="ansi")} = :{i + len(primary_keys) + 1}'
            for i, col in enumerate(non_primary_key_columns)
        ]
    )

    return f"""
    BEGIN
        INSERT INTO {table_identifier} ({primary_keys_str}, {columns_str})
            VALUES {column_placeholders};
        EXCEPTION
        WHEN dup_val_on_index THEN
            UPDATE {table_identifier}
            SET    {assignment_str}
            WHERE  {primary_key_condition_str};
    END;
    """


@_utils.check_optional_dependency(oracledb, "oracledb")
@apply_configs
def to_sql(
    df: pd.DataFrame,
    con: "oracledb.Connection",
    table: str,
    schema: str,
    mode: Literal["append", "overwrite", "upsert"] = "append",
    index: bool = False,
    dtype: dict[str, str] | None = None,
    varchar_lengths: dict[str, int] | None = None,
    use_column_names: bool = False,
    primary_keys: list[str] | None = None,
    chunksize: int = 200,
) -> None:
    """Write records stored in a DataFrame into Oracle Database.

    Parameters
    ----------
    df: pandas.DataFrame
        Pandas DataFrame https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html
    con: oracledb.Connection
        Use oracledb.connect() to use credentials directly or wr.oracle.connect() to fetch it from the Glue Catalog.
    table: str
        Table name
    schema: str
        Schema name
    mode: str
        Append, overwrite or upsert.
    index: bool
        True to store the DataFrame index as a column in the table,
        otherwise False to ignore it.
    dtype: Dict[str, str], optional
        Dictionary of columns names and Oracle types to be casted.
        Useful when you have columns with undetermined or mixed data types.
        (e.g. {'col name': 'TEXT', 'col2 name': 'FLOAT'})
    varchar_lengths: Dict[str, int], optional
        Dict of VARCHAR length by columns. (e.g. {"col1": 10, "col5": 200}).
    use_column_names: bool
        If set to True, will use the column names of the DataFrame for generating the INSERT SQL Query.
        E.g. If the DataFrame has two columns `col1` and `col3` and `use_column_names` is True, data will only be
        inserted into the database columns `col1` and `col3`.
    primary_keys : List[str], optional
        Primary keys.
    chunksize: int
        Number of rows which are inserted with each SQL query. Defaults to inserting 200 rows per query.

    Returns
    -------
    None
        None.

    Examples
    --------
    Writing to Oracle Database using a Glue Catalog Connections

    >>> import awswrangler as wr
    >>> con = wr.oracle.connect(connection="MY_GLUE_CONNECTION")
    >>> wr.oracle.to_sql(
    ...     df=df,
    ...     table="table",
    ...     schema="ORCL",
    ...     con=con
    ... )
    >>> con.close()

    """
    if df.empty is True:
        raise exceptions.EmptyDataFrame("DataFrame cannot be empty.")
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
                primary_keys=primary_keys,
            )
            if index:
                df.reset_index(level=df.index.names, inplace=True)

            column_placeholders: str = f"({', '.join([':' + str(i + 1) for i in range(len(df.columns))])})"
            table_identifier = _get_table_identifier(schema, table)

            if mode == "upsert":
                sql = _generate_upsert_statement(table_identifier, df, use_column_names, primary_keys)
            else:
                sql = _generate_insert_statement(table_identifier, df, use_column_names)

            placeholder_parameter_pair_generator = _db_utils.generate_placeholder_parameter_pairs(
                df=df, column_placeholders=column_placeholders, chunksize=chunksize
            )
            for _, parameters in placeholder_parameter_pair_generator:
                parameters = list(zip(*[iter(parameters)] * len(df.columns)))  # noqa: PLW2901
                _logger.debug("sql: %s", sql)
                cursor.executemany(sql, parameters)

            con.commit()
    except Exception as ex:
        con.rollback()
        _logger.error(ex)
        raise


def detect_oracle_decimal_datatype(cursor: Any) -> dict[str, pa.DataType]:
    """Determine if a given Oracle column is a decimal, not just a standard float value."""
    dtype = {}
    _logger.debug("cursor type: %s", type(cursor))
    if isinstance(cursor, oracledb.Cursor):
        # Oracle stores DECIMAL as the NUMBER type
        for row in cursor.description:
            if row[1] == oracledb.DB_TYPE_NUMBER and row[5] > 0:
                dtype[row[0]] = pa.decimal128(row[4], row[5])

    _logger.debug("decimal dtypes: %s", dtype)
    return dtype


def handle_oracle_objects(
    col_values: list[Any], col_name: str, dtype: dict[str, pa.DataType] | None = None
) -> list[Any]:
    """Retrieve Oracle LOB values which may be string or bytes, and convert float to decimal."""
    if any(isinstance(col_value, oracledb.LOB) for col_value in col_values):
        col_values = [
            col_value.read() if isinstance(col_value, oracledb.LOB) else col_value for col_value in col_values
        ]

    if dtype is not None:
        if isinstance(dtype[col_name], pa.Decimal128Type):
            _logger.debug("decimal_col_values:\n%s", col_values)
            col_values = [
                Decimal(repr(col_value)) if isinstance(col_value, float) else col_value for col_value in col_values
            ]

    return col_values
