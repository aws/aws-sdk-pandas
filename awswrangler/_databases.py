"""Databases Utilities."""

import logging
from typing import Any, Dict, Iterator, List, NamedTuple, Optional, Tuple, Union, cast
from urllib.parse import quote_plus

import boto3
import pandas as pd
import pyarrow as pa

from awswrangler import _data_types
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


def get_connection_attributes(
    connection: str,
    catalog_id: Optional[str] = None,
    boto3_session: Optional[boto3.Session] = None,
) -> ConnectionAttributes:
    """Get Connection Attributes."""
    details: Dict[str, Any] = get_connection(name=connection, catalog_id=catalog_id, boto3_session=boto3_session)[
        "ConnectionProperties"
    ]
    port, database = details["JDBC_CONNECTION_URL"].split(":")[3].split("/")
    return ConnectionAttributes(
        kind=details["JDBC_CONNECTION_URL"].split(":")[1].lower(),
        user=quote_plus(details["USERNAME"]),
        password=quote_plus(details["PASSWORD"]),
        host=details["JDBC_CONNECTION_URL"].split(":")[2].replace("/", ""),
        port=int(port),
        database=database,
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


def _iterate_cursor(
    cursor: Any,
    chunksize: int,
    cols_names: List[str],
    index: Optional[Union[str, List[str]]],
    safe: bool,
    dtype: Optional[Dict[str, pa.DataType]],
) -> Iterator[pd.DataFrame]:
    while True:
        records = cursor.fetchmany(chunksize)
        if not records:
            break
        yield _records2df(records=records, cols_names=cols_names, index=index, safe=safe, dtype=dtype)


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
        with con.cursor() as cursor:
            cursor.execute(*args)
            cols_names: List[str] = [
                col[0].decode("utf-8") if isinstance(col[0], bytes) else col[0] for col in cursor.description
            ]
            _logger.debug("cols_names: %s", cols_names)
            if chunksize is None:
                return _records2df(
                    records=cast(List[Tuple[Any]], cursor.fetchall()),
                    cols_names=cols_names,
                    index=index_col,
                    dtype=dtype,
                    safe=safe,
                )
            return _iterate_cursor(
                cursor=cursor, chunksize=chunksize, cols_names=cols_names, index=index_col, dtype=dtype, safe=safe
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
