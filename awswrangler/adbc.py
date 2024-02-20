# mypy: disable-error-code=name-defined
"""Amazon ADBC Module."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any, Iterator, Literal
from urllib.parse import urlencode

import boto3
import pyarrow as pa

import awswrangler.pandas as pd
from awswrangler import _databases as _db_utils
from awswrangler import _utils, exceptions

if TYPE_CHECKING:
    try:
        import adbc_driver_postgresql.dbapi as pg_dbapi
        from adbc_driver_manager import dbapi
    except ImportError:
        pass
else:
    pg_dbapi = _utils.import_optional_dependency("adbc_driver_postgresql.dbapi")
    db_api = _utils.import_optional_dependency("adbc_driver_manager.dbapi")


_logger: logging.Logger = logging.getLogger(__name__)


@_utils.check_optional_dependency(pg_dbapi, "pg_abapi")
def connect(
    connection: str | None = None,
    secret_id: str | None = None,
    catalog_id: str | None = None,
    dbname: str | None = None,
    boto3_session: boto3.Session | None = None,
    timeout: int | None = None,
) -> "dbapi.Connection":
    attrs: _db_utils.ConnectionAttributes = _db_utils.get_connection_attributes(
        connection=connection, secret_id=secret_id, catalog_id=catalog_id, dbname=dbname, boto3_session=boto3_session
    )
    if attrs.kind not in ("postgresql", "postgres"):
        raise exceptions.InvalidDatabaseType(
            f"Invalid connection type ({attrs.kind}. It must be a postgresql connection.)"
        )

    connection_arguments = {
        "host": attrs.host,
        "port": attrs.port,
        "user": attrs.user,
        "password": attrs.password,
    }
    if timeout:
        connection_arguments["connect_timeout"] = timeout

    return pg_dbapi.connect(uri=f"postgresql:///{attrs.database}?{urlencode(connection_arguments)}")


@_utils.check_optional_dependency(pg_dbapi, "pg_abapi")
def read_sql_query(
    sql: str,
    con: "dbapi.Connection",
    index_col: str | list[str] | None = None,
    params: list[Any] | tuple[Any, ...] | dict[Any, Any] | None = None,
    chunksize: int | None = None,
    dtype: dict[str, pa.DataType] | None = None,
    dtype_backend: Literal["numpy_nullable", "pyarrow"] = "numpy_nullable",
    **pandas_kwargs: Any,
) -> pd.DataFrame | Iterator[pd.DataFrame]:
    return pd.read_sql(
        sql,
        con,
        index_col=index_col,
        params=params,
        chunksize=chunksize,
        dtype=dtype,
        dtype_backend=dtype_backend,
        **pandas_kwargs,
    )


@_utils.check_optional_dependency(pg_dbapi, "pg_abapi")
def read_sql_table(
    table_name: str,
    con: "dbapi.Connection",
    index_col: str | list[str] | None = None,
    columns: list[str] | None = None,
    params: list[Any] | tuple[Any, ...] | dict[Any, Any] | None = None,
    chunksize: int | None = None,
    dtype: dict[str, pa.DataType] | None = None,
    dtype_backend: Literal["numpy_nullable", "pyarrow"] = "numpy_nullable",
    **pandas_kwargs: Any,
) -> pd.DataFrame | Iterator[pd.DataFrame]:
    return pd.read_sql_table(
        table_name,
        con,
        index_col=index_col,
        columns=columns,
        chunksize=chunksize,
        dtype=dtype,
        dtype_backend=dtype_backend,
        **pandas_kwargs,
    )


@_utils.check_optional_dependency(pg_dbapi, "pg_abapi")
def to_sql(
    df: pd.DataFrame,
    con: "dbapi.Connection",
    table: str,
    schema: str | None = False,
    if_exists: Literal["fail", "replace", "append"] = "fail",
    index: bool = False,
    index_label: str | list[str] | None = None,
    chunksize: int | None = None,
    dtype: dict[str, Any] | None = None,
    **pandas_kwargs: Any,
) -> None:
    df.to_sql(
        name=table,
        con=con,
        schema=schema,
        if_exists=if_exists,
        index=index,
        index_label=index_label,
        chunksize=chunksize,
        dtype=dtype,
        **pandas_kwargs,
    )
