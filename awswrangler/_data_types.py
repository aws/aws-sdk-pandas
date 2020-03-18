"""Internal (private) Data Types Module."""

import logging
from typing import Dict, List, Optional, Tuple

import pandas as pd  # type: ignore
import pyarrow as pa  # type: ignore

from awswrangler import exceptions

logger: logging.Logger = logging.getLogger(__name__)


def pyarrow2athena(dtype: pa.DataType) -> str:  # pylint: disable=too-many-branches,too-many-return-statements
    """Pyarrow to Athena data types conversion."""
    if pa.types.is_int8(dtype):
        return "tinyint"
    if pa.types.is_int16(dtype):
        return "smallint"
    if pa.types.is_int32(dtype):
        return "int"
    if pa.types.is_int64(dtype):
        return "bigint"
    if pa.types.is_float32(dtype):
        return "float"
    if pa.types.is_float64(dtype):
        return "double"
    if pa.types.is_boolean(dtype):
        return "boolean"
    if pa.types.is_string(dtype):
        return "string"
    if pa.types.is_timestamp(dtype):
        return "timestamp"
    if pa.types.is_date(dtype):
        return "date"
    if pa.types.is_binary(dtype):
        return "binary"
    if pa.types.is_dictionary(dtype):
        return pyarrow2athena(dtype=dtype.value_type)
    if pa.types.is_decimal(dtype):
        return f"decimal({dtype.precision},{dtype.scale})"
    if pa.types.is_list(dtype):
        return f"array<{pyarrow2athena(dtype=dtype.value_type)}>"
    if pa.types.is_struct(dtype):
        return f"struct<{', '.join([f'{f.name}: {pyarrow2athena(dtype=f.type)}' for f in dtype])}>"
    if dtype == pa.null():
        raise exceptions.UndetectedType("We can't infer the data type from an entire null object column")
    raise exceptions.UnsupportedType(f"Unsupported Pyarrow type: {dtype}")  # pragma: no cover


def pyarrow_types_from_pandas(
    df: pd.DataFrame, index: bool, ignore_cols: Optional[List[str]] = None
) -> Dict[str, pa.DataType]:
    """Extract the related Pyarrow data types from any Pandas DataFrame."""
    columns_types: Dict[str, pa.DataType] = {}

    # Handle exception data types (e.g. Int64, Int32, string)
    ignore_cols = [] if ignore_cols is None else ignore_cols
    cols: List[str] = []
    cols_dtypes: Dict[str, pa.DataType] = {}
    for name, dtype in df.dtypes.to_dict().items():
        dtype = str(dtype)
        if name in ignore_cols:
            pass
        elif dtype == "Int32":
            cols_dtypes[name] = pa.int32()
        elif dtype == "Int64":
            cols_dtypes[name] = pa.int64()
        elif dtype == "string":
            cols_dtypes[name] = pa.string()
        else:
            cols.append(name)

    # Filling cols_dtypes and indexes
    indexes: List[str] = []
    for field in pa.Schema.from_pandas(df=df[cols], preserve_index=index):
        name = str(field.name)
        cols_dtypes[name] = field.type
        if (name not in df.columns) and (index is True):
            indexes.append(name)

    # Filling schema
    columns_types = {n: cols_dtypes[n] for n in list(df.columns) + indexes if n not in ignore_cols}  # add cols + idxs
    logger.debug(f"columns_types: {columns_types}")
    return columns_types


def athena_types_from_pandas(df: pd.DataFrame, index: bool, ignore_cols: Optional[List[str]] = None) -> Dict[str, str]:
    """Extract the related Athena data types from any Pandas DataFrame."""
    pa_columns_types: Dict[str, Optional[pa.DataType]] = pyarrow_types_from_pandas(
        df=df, index=index, ignore_cols=ignore_cols
    )
    athena_columns_types: Dict[str, str] = {k: pyarrow2athena(dtype=v) for k, v in pa_columns_types.items()}
    logger.debug(f"athena_columns_types: {athena_columns_types}")
    return athena_columns_types


def athena_types_from_pandas_partitioned(
    df: pd.DataFrame, index: bool, ignore_cols: Optional[List[str]] = None, partition_cols: Optional[List[str]] = None
) -> Tuple[Dict[str, str], Dict[str, str]]:
    """Extract the related Athena data types from any Pandas DataFrame considering possible partitions."""
    partitions: List[str] = partition_cols if partition_cols else []
    athena_columns_types: Dict[str, str] = athena_types_from_pandas(df=df, index=index, ignore_cols=ignore_cols)
    columns_types: Dict[str, str] = {}
    partitions_types: Dict[str, str] = {}
    for k, v in athena_columns_types.items():
        if k in partitions:
            partitions_types[k] = v
        else:
            columns_types[k] = v
    return columns_types, partitions_types


def pyarrow_schema_from_pandas(df: pd.DataFrame, index: bool, ignore_cols: Optional[List[str]] = None) -> pa.Schema:
    """Extract the related Pyarrow Schema from any Pandas DataFrame."""
    columns_types: Dict[str, Optional[pa.DataType]] = pyarrow_types_from_pandas(
        df=df, index=index, ignore_cols=ignore_cols
    )
    return pa.schema(fields=columns_types)
