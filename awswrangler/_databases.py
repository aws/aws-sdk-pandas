"""Databases Utilities."""

import logging
from typing import Any, Dict, Iterator, List, NamedTuple, Optional, Tuple, Union, cast

import boto3
import pandas as pd
import pyarrow as pa

from awswrangler import _data_types, _utils, exceptions, secretsmanager
from awswrangler.catalog import get_connection

_logger: logging.Logger = logging.getLogger(__name__)


class ConnectionAttributes(NamedTuple):
    """Connection Attributes."""

    kind: str
    user: str
    password: str
    host: str
    port: int
    database: str


def _get_dbname(cluster_id: str, boto3_session: Optional[boto3.Session] = None) -> str:
    client_redshift: boto3.client = _utils.client(service_name="redshift", session=boto3_session)
    res: Dict[str, Any] = client_redshift.describe_clusters(ClusterIdentifier=cluster_id)["Clusters"][0]
    return cast(str, res["DBName"])


def _get_connection_attributes_from_catalog(
    connection: str, catalog_id: Optional[str], dbname: Optional[str], boto3_session: Optional[boto3.Session]
) -> ConnectionAttributes:
    details: Dict[str, Any] = get_connection(name=connection, catalog_id=catalog_id, boto3_session=boto3_session)[
        "ConnectionProperties"
    ]
    if ";databaseName=" in details["JDBC_CONNECTION_URL"]:
        database_sep = ";databaseName="
    else:
        database_sep = "/"
    port, database = details["JDBC_CONNECTION_URL"].split(":")[3].split(database_sep)
    return ConnectionAttributes(
        kind=details["JDBC_CONNECTION_URL"].split(":")[1].lower(),
        user=details["USERNAME"],
        password=details["PASSWORD"],
        host=details["JDBC_CONNECTION_URL"].split(":")[2].replace("/", ""),
        port=int(port),
        database=dbname if dbname is not None else database,
    )


def _get_connection_attributes_from_secrets_manager(
    secret_id: str, dbname: Optional[str], boto3_session: Optional[boto3.Session]
) -> ConnectionAttributes:
    secret_value: Dict[str, Any] = secretsmanager.get_secret_json(name=secret_id, boto3_session=boto3_session)
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
        port=secret_value["port"],
        database=_dbname,
    )


def get_connection_attributes(
    connection: Optional[str] = None,
    secret_id: Optional[str] = None,
    catalog_id: Optional[str] = None,
    dbname: Optional[str] = None,
    boto3_session: Optional[boto3.Session] = None,
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


def _convert_params(sql: str, params: Optional[Union[List[Any], Tuple[Any, ...], Dict[Any, Any]]]) -> List[Any]:
    args: List[Any] = [sql]
    if params is not None:
        if hasattr(params, "keys"):
            return args + [params]
        return args + [list(params)]
    return args


def _records2df(
    records: List[Tuple[Any]],
    cols_names: List[str],
    index: Optional[Union[str, List[str]]],
    safe: bool,
    dtype: Optional[Dict[str, pa.DataType]],
) -> pd.DataFrame:
    arrays: List[pa.Array] = []
    for col_values, col_name in zip(tuple(zip(*records)), cols_names):  # Transposing
        if (dtype is None) or (col_name not in dtype):
            try:
                array: pa.Array = pa.array(obj=col_values, safe=safe)  # Creating Arrow array
            except pa.ArrowInvalid as ex:
                array = _data_types.process_not_inferred_array(ex, values=col_values)  # Creating Arrow array
        else:
            try:
                array = pa.array(obj=col_values, type=dtype[col_name], safe=safe)  # Creating Arrow array with dtype
            except pa.ArrowInvalid:
                array = pa.array(obj=col_values, safe=safe)  # Creating Arrow array
                array = array.cast(target_type=dtype[col_name], safe=safe)  # Casting
        arrays.append(array)
    table = pa.Table.from_arrays(arrays=arrays, names=cols_names)  # Creating arrow Table
    df: pd.DataFrame = table.to_pandas(  # Creating Pandas DataFrame
        use_threads=True,
        split_blocks=True,
        self_destruct=True,
        integer_object_nulls=False,
        date_as_object=True,
        types_mapper=_data_types.pyarrow2pandas_extension,
        safe=safe,
    )
    if index is not None:
        df.set_index(index, inplace=True)
    return df


def _get_cols_names(cursor_description: Any) -> List[str]:
    cols_names = [col[0].decode("utf-8") if isinstance(col[0], bytes) else col[0] for col in cursor_description]
    _logger.debug("cols_names: %s", cols_names)

    return cols_names


def _iterate_results(
    con: Any,
    cursor_args: List[Any],
    chunksize: int,
    index_col: Optional[Union[str, List[str]]],
    safe: bool,
    dtype: Optional[Dict[str, pa.DataType]],
) -> Iterator[pd.DataFrame]:
    with con.cursor() as cursor:
        cursor.execute(*cursor_args)
        cols_names = _get_cols_names(cursor.description)
        while True:
            records = cursor.fetchmany(chunksize)
            if not records:
                break
            yield _records2df(records=records, cols_names=cols_names, index=index_col, safe=safe, dtype=dtype)


def _fetch_all_results(
    con: Any,
    cursor_args: List[Any],
    index_col: Optional[Union[str, List[str]]] = None,
    dtype: Optional[Dict[str, pa.DataType]] = None,
    safe: bool = True,
) -> pd.DataFrame:
    with con.cursor() as cursor:
        cursor.execute(*cursor_args)
        cols_names = _get_cols_names(cursor.description)
        return _records2df(
            records=cast(List[Tuple[Any]], cursor.fetchall()),
            cols_names=cols_names,
            index=index_col,
            dtype=dtype,
            safe=safe,
        )


def read_sql_query(
    sql: str,
    con: Any,
    index_col: Optional[Union[str, List[str]]] = None,
    params: Optional[Union[List[Any], Tuple[Any, ...], Dict[Any, Any]]] = None,
    chunksize: Optional[int] = None,
    dtype: Optional[Dict[str, pa.DataType]] = None,
    safe: bool = True,
) -> Union[pd.DataFrame, Iterator[pd.DataFrame]]:
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
            )

        return _iterate_results(
            con=con,
            cursor_args=args,
            chunksize=chunksize,
            index_col=index_col,
            dtype=dtype,
            safe=safe,
        )
    except Exception as ex:
        con.rollback()
        _logger.error(ex)
        raise


def extract_parameters(df: pd.DataFrame) -> List[List[Any]]:
    """Extract Parameters."""
    parameters: List[List[Any]] = df.values.tolist()
    for i, row in enumerate(parameters):
        for j, value in enumerate(row):
            if pd.isna(value):
                parameters[i][j] = None
            elif hasattr(value, "to_pydatetime"):
                parameters[i][j] = value.to_pydatetime()
    return parameters
