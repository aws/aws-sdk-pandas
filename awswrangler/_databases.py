"""Databases Utilities."""

from __future__ import annotations

import importlib.util
import logging
import ssl
from typing import Any, Generator, Iterator, NamedTuple, cast, overload

import boto3
import pyarrow as pa
from typing_extensions import Literal

import awswrangler.pandas as pd
from awswrangler import _data_types, _utils, exceptions, oracle, secretsmanager
from awswrangler.catalog import get_connection

_oracledb_found = importlib.util.find_spec("oracledb")

_logger: logging.Logger = logging.getLogger(__name__)


class ConnectionAttributes(NamedTuple):
    """Connection Attributes."""

    kind: str
    user: str
    password: str
    host: str
    port: int
    database: str
    ssl_context: ssl.SSLContext | None


def _get_dbname(cluster_id: str, boto3_session: boto3.Session | None = None) -> str:
    client_redshift = _utils.client(service_name="redshift", session=boto3_session)
    res = client_redshift.describe_clusters(ClusterIdentifier=cluster_id)["Clusters"][0]
    return res["DBName"]


def _get_connection_attributes_from_catalog(
    connection: str, catalog_id: str | None, dbname: str | None, boto3_session: boto3.Session | None
) -> ConnectionAttributes:
    details: dict[str, Any] = get_connection(name=connection, catalog_id=catalog_id, boto3_session=boto3_session)[
        "ConnectionProperties"
    ]
    if ";databaseName=" in details["JDBC_CONNECTION_URL"]:
        database_sep = ";databaseName="
    else:
        database_sep = "/"
    port, database = details["JDBC_CONNECTION_URL"].split(":")[-1].split(database_sep)
    ssl_context: ssl.SSLContext | None = None
    if details.get("JDBC_ENFORCE_SSL") == "true":
        ssl_cert_path: str | None = details.get("CUSTOM_JDBC_CERT")
        ssl_cadata: str | None = None
        if ssl_cert_path:
            bucket_name, key_path = _utils.parse_path(ssl_cert_path)
            client_s3 = _utils.client(service_name="s3", session=boto3_session)
            try:
                ssl_cadata = client_s3.get_object(Bucket=bucket_name, Key=key_path)["Body"].read().decode("utf-8")
            except client_s3.exceptions.NoSuchKey:
                raise exceptions.NoFilesFound(  # pylint: disable=raise-missing-from
                    f"No CA certificate found at {ssl_cert_path}."
                )
        ssl_context = ssl.create_default_context(cadata=ssl_cadata)

    if "SECRET_ID" in details:
        secret_value: dict[str, Any] = secretsmanager.get_secret_json(
            name=details["SECRET_ID"], boto3_session=boto3_session
        )
        username = secret_value["username"]
        password = secret_value["password"]
    else:
        username = details["USERNAME"]
        password = details["PASSWORD"]

    return ConnectionAttributes(
        kind=details["JDBC_CONNECTION_URL"].split(":")[1].lower(),
        user=username,
        password=password,
        host=details["JDBC_CONNECTION_URL"].split(":")[-2].replace("/", "").replace("@", ""),
        port=int(port),
        database=dbname if dbname is not None else database,
        ssl_context=ssl_context,
    )


def _get_connection_attributes_from_secrets_manager(
    secret_id: str, dbname: str | None, boto3_session: boto3.Session | None
) -> ConnectionAttributes:
    secret_value: dict[str, Any] = secretsmanager.get_secret_json(name=secret_id, boto3_session=boto3_session)
    kind: str = secret_value["engine"]
    if dbname is not None:
        _dbname: str = dbname
    elif "dbname" in secret_value:
        _dbname = secret_value["dbname"]
    else:
        if kind != "redshift":
            raise exceptions.InvalidConnection(f"The secret {secret_id} MUST have a dbname property.")
        _dbname = _get_dbname(cluster_id=secret_value["dbClusterIdentifier"], boto3_session=boto3_session)
    return ConnectionAttributes(
        kind=kind,
        user=secret_value["username"],
        password=secret_value["password"],
        host=secret_value["host"],
        port=int(secret_value["port"]),
        database=_dbname,
        ssl_context=None,
    )


def get_connection_attributes(
    connection: str | None = None,
    secret_id: str | None = None,
    catalog_id: str | None = None,
    dbname: str | None = None,
    boto3_session: boto3.Session | None = None,
) -> ConnectionAttributes:
    """Get Connection Attributes."""
    if connection is None and secret_id is None:
        raise exceptions.InvalidArgumentCombination(
            "Failed attempt to connect. You MUST pass a connection name (Glue Catalog) OR a secret_id as argument."
        )
    if connection is not None:
        return _get_connection_attributes_from_catalog(
            connection=connection, catalog_id=catalog_id, dbname=dbname, boto3_session=boto3_session
        )
    return _get_connection_attributes_from_secrets_manager(
        secret_id=cast(str, secret_id), dbname=dbname, boto3_session=boto3_session
    )


def _convert_params(sql: str, params: list[Any] | tuple[Any, ...] | dict[Any, Any] | None) -> list[Any]:
    args: list[Any] = [sql]
    if params is not None:
        if hasattr(params, "keys"):
            return args + [params]
        return args + [list(params)]
    return args


def _should_handle_oracle_objects(dtype: pa.DataType) -> bool:
    return (
        dtype == pa.string()
        or dtype == pa.large_string()
        or isinstance(dtype, pa.Decimal128Type)
        or dtype == pa.binary()
        or dtype == pa.large_binary()
    )


def _records2df(
    records: list[tuple[Any]],
    cols_names: list[str],
    index: str | list[str] | None,
    safe: bool,
    dtype: dict[str, pa.DataType] | None,
    timestamp_as_object: bool,
    dtype_backend: Literal["numpy_nullable", "pyarrow"],
) -> pd.DataFrame:
    arrays: list[pa.Array] = []
    for col_values, col_name in zip(tuple(zip(*records)), cols_names):  # Transposing
        if (dtype is None) or (col_name not in dtype):
            if _oracledb_found:
                col_values = oracle.handle_oracle_objects(col_values, col_name)  # type: ignore[arg-type,assignment]  # noqa: PLW2901
            try:
                array: pa.Array = pa.array(obj=col_values, safe=safe)  # Creating Arrow array
            except pa.ArrowInvalid as ex:
                array = _data_types.process_not_inferred_array(ex, values=col_values)  # Creating Arrow array
        else:
            try:
                if _oracledb_found:
                    if _should_handle_oracle_objects(dtype[col_name]):
                        col_values = oracle.handle_oracle_objects(col_values, col_name, dtype)  # type: ignore[arg-type,assignment]  # noqa: PLW2901
                array = pa.array(obj=col_values, type=dtype[col_name], safe=safe)  # Creating Arrow array with dtype
            except (pa.ArrowInvalid, pa.ArrowTypeError):
                array = pa.array(obj=col_values, safe=safe)  # Creating Arrow array
                array = array.cast(target_type=dtype[col_name], safe=safe)  # Casting
        arrays.append(array)
    if not arrays:
        df = pd.DataFrame(columns=cols_names)
    else:
        table = pa.Table.from_arrays(arrays=arrays, names=cols_names)  # Creating arrow Table
        df = table.to_pandas(  # Creating Pandas DataFrame
            use_threads=True,
            split_blocks=True,
            self_destruct=True,
            integer_object_nulls=False,
            date_as_object=True,
            types_mapper=_data_types.get_pyarrow2pandas_type_mapper(dtype_backend=dtype_backend),
            safe=safe,
            timestamp_as_object=timestamp_as_object,
        )
    if index is not None:
        df.set_index(index, inplace=True)
    return df


def _get_cols_names(cursor_description: Any) -> list[str]:
    cols_names = [col[0].decode("utf-8") if isinstance(col[0], bytes) else col[0] for col in cursor_description]
    _logger.debug("cols_names: %s", cols_names)

    return cols_names


def _iterate_results(
    con: Any,
    cursor_args: list[Any],
    chunksize: int,
    index_col: str | list[str] | None,
    safe: bool,
    dtype: dict[str, pa.DataType] | None,
    timestamp_as_object: bool,
    dtype_backend: Literal["numpy_nullable", "pyarrow"],
) -> Iterator[pd.DataFrame]:
    with con.cursor() as cursor:
        cursor.execute(*cursor_args)
        if _oracledb_found:
            decimal_dtypes = oracle.detect_oracle_decimal_datatype(cursor)
            _logger.debug("steporig: %s", dtype)
            if decimal_dtypes and dtype is not None:
                dtype = dict(list(decimal_dtypes.items()) + list(dtype.items()))
            elif decimal_dtypes:
                dtype = decimal_dtypes

        cols_names = _get_cols_names(cursor.description)
        while True:
            records = cursor.fetchmany(chunksize)
            if not records:
                break
            yield _records2df(
                records=records,
                cols_names=cols_names,
                index=index_col,
                safe=safe,
                dtype=dtype,
                timestamp_as_object=timestamp_as_object,
                dtype_backend=dtype_backend,
            )


def _fetch_all_results(
    con: Any,
    cursor_args: list[Any],
    index_col: str | list[str] | None = None,
    dtype: dict[str, pa.DataType] | None = None,
    safe: bool = True,
    timestamp_as_object: bool = False,
    dtype_backend: Literal["numpy_nullable", "pyarrow"] = "pyarrow",
) -> pd.DataFrame:
    with con.cursor() as cursor:
        cursor.execute(*cursor_args)
        cols_names = _get_cols_names(cursor.description)
        if _oracledb_found:
            decimal_dtypes = oracle.detect_oracle_decimal_datatype(cursor)
            _logger.debug("steporig: %s", dtype)
            if decimal_dtypes and dtype is not None:
                dtype = dict(list(decimal_dtypes.items()) + list(dtype.items()))
            elif decimal_dtypes:
                dtype = decimal_dtypes

        return _records2df(
            records=cast(list[tuple[Any]], cursor.fetchall()),
            cols_names=cols_names,
            index=index_col,
            dtype=dtype,
            safe=safe,
            timestamp_as_object=timestamp_as_object,
            dtype_backend=dtype_backend,
        )


@overload
def read_sql_query(
    sql: str,
    con: Any,
    index_col: str | list[str] | None = ...,
    params: list[Any] | tuple[Any, ...] | dict[Any, Any] | None = ...,
    chunksize: None = ...,
    dtype: dict[str, pa.DataType] | None = ...,
    safe: bool = ...,
    timestamp_as_object: bool = ...,
    dtype_backend: Literal["numpy_nullable", "pyarrow"] = ...,
) -> pd.DataFrame:
    ...


@overload
def read_sql_query(
    sql: str,
    con: Any,
    *,
    index_col: str | list[str] | None = ...,
    params: list[Any] | tuple[Any, ...] | dict[Any, Any] | None = ...,
    chunksize: int,
    dtype: dict[str, pa.DataType] | None = ...,
    safe: bool = ...,
    timestamp_as_object: bool = ...,
    dtype_backend: Literal["numpy_nullable", "pyarrow"] = ...,
) -> Iterator[pd.DataFrame]:
    ...


@overload
def read_sql_query(
    sql: str,
    con: Any,
    *,
    index_col: str | list[str] | None = ...,
    params: list[Any] | tuple[Any, ...] | dict[Any, Any] | None = ...,
    chunksize: int | None,
    dtype: dict[str, pa.DataType] | None = ...,
    safe: bool = ...,
    timestamp_as_object: bool = ...,
    dtype_backend: Literal["numpy_nullable", "pyarrow"] = ...,
) -> pd.DataFrame | Iterator[pd.DataFrame]:
    ...


def read_sql_query(
    sql: str,
    con: Any,
    index_col: str | list[str] | None = None,
    params: list[Any] | tuple[Any, ...] | dict[Any, Any] | None = None,
    chunksize: int | None = None,
    dtype: dict[str, pa.DataType] | None = None,
    safe: bool = True,
    timestamp_as_object: bool = False,
    dtype_backend: Literal["numpy_nullable", "pyarrow"] = "numpy_nullable",
) -> pd.DataFrame | Iterator[pd.DataFrame]:
    """Read SQL Query (generic)."""
    args = _convert_params(sql, params)
    try:
        if chunksize is None:
            return _fetch_all_results(
                con=con,
                cursor_args=args,
                index_col=index_col,
                dtype=dtype,
                safe=safe,
                timestamp_as_object=timestamp_as_object,
                dtype_backend=dtype_backend,
            )

        return _iterate_results(
            con=con,
            cursor_args=args,
            chunksize=chunksize,
            index_col=index_col,
            dtype=dtype,
            safe=safe,
            timestamp_as_object=timestamp_as_object,
            dtype_backend=dtype_backend,
        )
    except Exception as ex:
        con.rollback()
        _logger.error(ex)
        raise


def generate_placeholder_parameter_pairs(
    df: pd.DataFrame, column_placeholders: str, chunksize: int
) -> Generator[tuple[str, list[Any]], None, None]:
    """Extract Placeholder and Parameter pairs."""

    def convert_value_to_native_python_type(value: Any) -> Any:
        if pd.isna(value):
            return None
        if hasattr(value, "to_pydatetime"):
            return value.to_pydatetime()

        return value

    parameters = df.values.tolist()
    for i in range(0, len(df.index), chunksize):
        parameters_chunk = parameters[i : i + chunksize]
        chunk_placeholders = ", ".join([f"({column_placeholders})" for _ in range(len(parameters_chunk))])
        flattened_chunk = [convert_value_to_native_python_type(value) for row in parameters_chunk for value in row]
        yield chunk_placeholders, flattened_chunk


def validate_mode(mode: str, allowed_modes: list[str]) -> None:
    """Check if mode is included in allowed_modes."""
    if mode not in allowed_modes:
        raise exceptions.InvalidArgumentValue(f"mode must be one of {', '.join(allowed_modes)}")
