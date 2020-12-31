"""Amazon Microsoft SQL Server Module."""


import logging
from typing import Any, Dict, Iterator, List, Optional, Tuple, Union

import boto3
import pandas as pd
import pyarrow as pa
import pymssql

from awswrangler import _data_types
from awswrangler import _databases as _db_utils
from awswrangler import exceptions

_logger: logging.Logger = logging.getLogger(__name__)


def _validate_connection(con: pymssql.Connection) -> None:
    if not isinstance(con, pymssql.Connection):
        raise exceptions.InvalidConnection(
            "Invalid 'conn' argument, please pass a "
            "pymssql.Connection object. Use pymssql.connect() to use "
            "credentials directly or wr.sqlserver.connect() to fetch it from the Glue Catalog."
        )


def _drop_table(cursor: pymssql.Cursor, schema: Optional[str], table: str) -> None:
    schema_str = f"{schema}." if schema else ""
    sql = f"IF OBJECT_ID(N'{schema_str}{table}', N'U') IS NOT NULL DROP TABLE {schema_str}{table}"
    _logger.debug("Drop table query:\n%s", sql)
    cursor.execute(sql)


def _does_table_exist(cursor: pymssql.Cursor, schema: Optional[str], table: str) -> bool:
    schema_str = f"TABLE_SCHEMA = '{schema}' AND" if schema else ""
    cursor.execute(f"SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE " f"{schema_str} TABLE_NAME = '{table}'")
    return len(cursor.fetchall()) > 0


def _create_table(
    df: pd.DataFrame,
    cursor: pymssql.Cursor,
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
    sql = f"IF OBJECT_ID(N'{schema}.{table}', N'U') IS NULL BEGIN CREATE TABLE {schema}.{table} (\n{cols_str}); END;"
    _logger.debug("Create table query:\n%s", sql)
    cursor.execute(sql)


def connect(
    connection: Optional[str] = None,
    secret_id: Optional[str] = None,
    catalog_id: Optional[str] = None,
    dbname: Optional[str] = None,
    boto3_session: Optional[boto3.Session] = None,
    timeout: Optional[int] = 0,
    login_timeout: Optional[int] = 60,
) -> pymssql.Connection:
    attrs: _db_utils.ConnectionAttributes = _db_utils.get_connection_attributes(
        connection=connection, secret_id=secret_id, catalog_id=catalog_id, dbname=dbname, boto3_session=boto3_session
    )
    if attrs.kind != "sqlserver":
        exceptions.InvalidDatabaseType(f"Invalid connection type ({attrs.kind}. It must be a sqlserver connection.)")
    return pymssql.connect(
        user=attrs.user,
        database=attrs.database,
        password=attrs.password,
        port=attrs.port,
        host=attrs.host,
        timeout=timeout,
        login_timeout=login_timeout,
    )


def read_sql_query(
    sql: str,
    con: pymssql.Connection,
    index_col: Optional[Union[str, List[str]]] = None,
    params: Optional[Union[List[Any], Tuple[Any, ...], Dict[Any, Any]]] = None,
    chunksize: Optional[int] = None,
    dtype: Optional[Dict[str, pa.DataType]] = None,
    safe: bool = True,
) -> Union[pd.DataFrame, Iterator[pd.DataFrame]]:
    _validate_connection(con=con)
    return _db_utils.read_sql_query(
        sql=sql, con=con, index_col=index_col, params=params, chunksize=chunksize, dtype=dtype, safe=safe
    )


def to_sql(
    df: pd.DataFrame,
    con: pymssql.Connection,
    table: str,
    schema: str,
    mode: str = "append",
    index: bool = False,
    dtype: Optional[Dict[str, str]] = None,
    varchar_lengths: Optional[Dict[str, int]] = None,
) -> None:
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
            placeholders: str = ", ".join(["%s"] * len(df.columns))
            sql: str = f'INSERT INTO "{schema}"."{table}" VALUES ({placeholders})'
            _logger.debug("sql: %s", sql)
            parameters: List[List[Any]] = _db_utils.extract_parameters(df=df)
            parameter_tuples: List[Tuple[Any]] = [tuple(parameter_set) for parameter_set in parameters]
            cursor.executemany(sql, parameter_tuples)
            con.commit()
    except Exception as ex:
        con.rollback()
        _logger.error(ex)
        raise
