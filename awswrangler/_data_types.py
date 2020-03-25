"""Internal (private) Data Types Module."""

import logging
from typing import Dict, List, Optional, Tuple

import pandas as pd  # type: ignore
import pyarrow as pa  # type: ignore
import pyarrow.parquet  # type: ignore

from awswrangler import exceptions

_logger: logging.Logger = logging.getLogger(__name__)


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


def pyarrow2pandas_extension(  # pylint: disable=too-many-branches,too-many-return-statements
    dtype: pa.DataType
) -> Optional[pd.api.extensions.ExtensionDtype]:
    """Pyarrow to Pandas data types conversion."""
    if pa.types.is_int8(dtype):  # pragma: no cover
        return pd.Int8Dtype()
    if pa.types.is_int16(dtype):  # pragma: no cover
        return pd.Int16Dtype()
    if pa.types.is_int32(dtype):
        return pd.Int32Dtype()
    if pa.types.is_int64(dtype):
        return pd.Int64Dtype()
    if pa.types.is_boolean(dtype):
        return pd.BooleanDtype()
    if pa.types.is_string(dtype):
        return pd.StringDtype()
    return None


def athena2pandas(dtype: str) -> str:  # pylint: disable=too-many-branches,too-many-return-statements
    """Athena to Pandas data types conversion."""
    dtype = dtype.lower()
    if dtype == "tinyint":
        return "Int8"
    if dtype == "smallint":
        return "Int16"
    if dtype in ("int", "integer"):
        return "Int32"
    if dtype == "bigint":
        return "Int64"
    if dtype == "float":
        return "float32"
    if dtype == "double":
        return "float64"
    if dtype in ("bool", "boolean"):
        return "boolean"
    if dtype in ("string", "char", "varchar"):
        return "string"
    if dtype in ("timestamp", "timestamp with time zone"):
        return "datetime64"
    if dtype == "date":
        return "date"
    if dtype == "decimal":
        return "decimal"
    if dtype == "varbinary":
        return "bytes"
    if dtype == "array":  # pragma: no cover
        return "list"
    raise exceptions.UnsupportedType(f"Unsupported Athena type: {dtype}")  # pragma: no cover


def pyarrow_types_from_pandas(
    df: pd.DataFrame, index: bool, ignore_cols: Optional[List[str]] = None
) -> Dict[str, pa.DataType]:
    """Extract the related Pyarrow data types from any Pandas DataFrame."""
    # Handle exception data types (e.g. Int64, Int32, string)
    ignore_cols = [] if ignore_cols is None else ignore_cols
    cols: List[str] = []
    cols_dtypes: Dict[str, pa.DataType] = {}
    for name, dtype in df.dtypes.to_dict().items():
        dtype = str(dtype)
        if name in ignore_cols:
            pass
        elif dtype == "Int8":
            cols_dtypes[name] = pa.int8()
        elif dtype == "Int16":
            cols_dtypes[name] = pa.int16()
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
    columns_types: Dict[str, pa.DataType]
    columns_types = {n: cols_dtypes[n] for n in list(df.columns) + indexes if n not in ignore_cols}  # add cols + idxs
    _logger.debug(f"columns_types: {columns_types}")
    return columns_types


def athena_types_from_pandas(df: pd.DataFrame, index: bool, ignore_cols: Optional[List[str]] = None) -> Dict[str, str]:
    """Extract the related Athena data types from any Pandas DataFrame."""
    pa_columns_types: Dict[str, Optional[pa.DataType]] = pyarrow_types_from_pandas(
        df=df, index=index, ignore_cols=ignore_cols
    )
    athena_columns_types: Dict[str, str] = {k: pyarrow2athena(dtype=v) for k, v in pa_columns_types.items()}
    _logger.debug(f"athena_columns_types: {athena_columns_types}")
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


def athena_types_from_pyarrow_schema(
    schema: pa.Schema, partitions: pyarrow.parquet.ParquetPartitions
) -> Tuple[Dict[str, str], Optional[Dict[str, str]]]:
    """Extract the related Athena data types from any PyArrow Schema considering possible partitions."""
    columns_types: Dict[str, str] = {str(f.name): pyarrow2athena(dtype=f.type) for f in schema}
    _logger.debug(f"columns_types: {columns_types}")
    partitions_types: Dict[str, str] = {p.name: pyarrow2athena(p.dictionary.type) for p in partitions}
    _logger.debug(f"partitions_types: {partitions_types}")
    return columns_types, partitions_types


def athena_partitions_from_pyarrow_partitions(
    path: str, partitions: pyarrow.parquet.ParquetPartitions
) -> Dict[str, List[str]]:
    """Extract the related Athena partitions values from any PyArrow Partitions."""
    path = path if path[-1] == "/" else f"{path}/"
    partitions_values: Dict[str, List[str]] = {}
    names: List[str] = [p.name for p in partitions]
    for values in zip(*[p.keys for p in partitions]):
        suffix: str = "/".join([f"{n}={v}" for n, v in zip(names, values)])
        suffix = suffix if suffix[-1] == "/" else f"{suffix}/"
        partitions_values[f"{path}{suffix}"] = list(values)
    return partitions_values
