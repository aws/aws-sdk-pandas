"""Internal (private) Data Types Module."""

from __future__ import annotations

import datetime
import logging
import re
import warnings
from decimal import Decimal
from typing import Any, Callable, Iterator, Match, Sequence

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet

from awswrangler import _arrow, exceptions
from awswrangler._distributed import engine

_logger: logging.Logger = logging.getLogger(__name__)


def pyarrow2athena(  # noqa: PLR0911,PLR0912
    dtype: pa.DataType, ignore_null: bool = False
) -> str:
    """Pyarrow to Athena data types conversion."""
    if pa.types.is_int8(dtype):
        return "tinyint"
    if pa.types.is_int16(dtype) or pa.types.is_uint8(dtype):
        return "smallint"
    if pa.types.is_int32(dtype) or pa.types.is_uint16(dtype):
        return "int"
    if pa.types.is_int64(dtype) or pa.types.is_uint32(dtype):
        return "bigint"
    if pa.types.is_uint64(dtype):
        raise exceptions.UnsupportedType("There is no support for uint64, please consider int64 or uint32.")
    if pa.types.is_float32(dtype):
        return "float"
    if pa.types.is_float64(dtype):
        return "double"
    if pa.types.is_boolean(dtype):
        return "boolean"
    if pa.types.is_string(dtype) or pa.types.is_large_string(dtype):
        return "string"
    if pa.types.is_timestamp(dtype):
        return "timestamp"
    if pa.types.is_date(dtype):
        return "date"
    if pa.types.is_binary(dtype) or pa.types.is_fixed_size_binary(dtype):
        return "binary"
    if pa.types.is_dictionary(dtype):
        return pyarrow2athena(dtype=dtype.value_type, ignore_null=ignore_null)
    if pa.types.is_decimal(dtype):
        return f"decimal({dtype.precision},{dtype.scale})"
    if pa.types.is_list(dtype):
        return f"array<{pyarrow2athena(dtype=dtype.value_type, ignore_null=ignore_null)}>"
    if pa.types.is_struct(dtype):
        return (
            f"struct<{','.join([f'{f.name}:{pyarrow2athena(dtype=f.type, ignore_null=ignore_null)}' for f in dtype])}>"
        )
    if pa.types.is_map(dtype):
        return f"map<{pyarrow2athena(dtype=dtype.key_type, ignore_null=ignore_null)},{pyarrow2athena(dtype=dtype.item_type, ignore_null=ignore_null)}>"
    if dtype == pa.null():
        if ignore_null:
            return ""
        raise exceptions.UndetectedType("We can not infer the data type from an entire null object column")
    raise exceptions.UnsupportedType(f"Unsupported Pyarrow type: {dtype}")


def pyarrow2redshift(  # noqa: PLR0911,PLR0912
    dtype: pa.DataType, string_type: str
) -> str:
    """Pyarrow to Redshift data types conversion."""
    if pa.types.is_int8(dtype):
        return "SMALLINT"
    if pa.types.is_int16(dtype) or pa.types.is_uint8(dtype):
        return "SMALLINT"
    if pa.types.is_int32(dtype) or pa.types.is_uint16(dtype):
        return "INTEGER"
    if pa.types.is_int64(dtype) or pa.types.is_uint32(dtype):
        return "BIGINT"
    if pa.types.is_uint64(dtype):
        raise exceptions.UnsupportedType("There is no support for uint64, please consider int64 or uint32.")
    if pa.types.is_float32(dtype):
        return "FLOAT4"
    if pa.types.is_float64(dtype):
        return "FLOAT8"
    if pa.types.is_boolean(dtype):
        return "BOOL"
    if pa.types.is_string(dtype) or pa.types.is_large_string(dtype):
        return string_type
    if pa.types.is_timestamp(dtype):
        return "TIMESTAMP"
    if pa.types.is_date(dtype):
        return "DATE"
    if pa.types.is_time(dtype):
        return "TIME"
    if pa.types.is_binary(dtype):
        return "VARBYTE"
    if pa.types.is_decimal(dtype):
        return f"DECIMAL({dtype.precision},{dtype.scale})"
    if pa.types.is_dictionary(dtype):
        return pyarrow2redshift(dtype=dtype.value_type, string_type=string_type)
    if pa.types.is_list(dtype) or pa.types.is_struct(dtype) or pa.types.is_map(dtype):
        return "SUPER"
    raise exceptions.UnsupportedType(f"Unsupported Redshift type: {dtype}")


def pyarrow2mysql(  # noqa: PLR0911,PLR0912
    dtype: pa.DataType, string_type: str
) -> str:
    """Pyarrow to MySQL data types conversion."""
    if pa.types.is_int8(dtype):
        return "TINYINT"
    if pa.types.is_uint8(dtype):
        return "UNSIGNED TINYINT"
    if pa.types.is_int16(dtype):
        return "SMALLINT"
    if pa.types.is_uint16(dtype):
        return "UNSIGNED SMALLINT"
    if pa.types.is_int32(dtype):
        return "INTEGER"
    if pa.types.is_uint32(dtype):
        return "UNSIGNED INTEGER"
    if pa.types.is_int64(dtype):
        return "BIGINT"
    if pa.types.is_uint64(dtype):
        return "UNSIGNED BIGINT"
    if pa.types.is_float32(dtype):
        return "FLOAT"
    if pa.types.is_float64(dtype):
        return "DOUBLE PRECISION"
    if pa.types.is_boolean(dtype):
        return "BOOLEAN"
    if pa.types.is_string(dtype) or pa.types.is_large_string(dtype):
        return string_type
    if pa.types.is_timestamp(dtype):
        return "TIMESTAMP"
    if pa.types.is_date(dtype):
        return "DATE"
    if pa.types.is_decimal(dtype):
        return f"DECIMAL({dtype.precision},{dtype.scale})"
    if pa.types.is_dictionary(dtype):
        return pyarrow2mysql(dtype=dtype.value_type, string_type=string_type)
    if pa.types.is_binary(dtype):
        return "BLOB"
    raise exceptions.UnsupportedType(f"Unsupported MySQL type: {dtype}")


def pyarrow2oracle(  # noqa: PLR0911
    dtype: pa.DataType, string_type: str
) -> str:
    """Pyarrow to Oracle Database data types conversion."""
    if pa.types.is_int8(dtype):
        return "NUMBER(3)"
    if pa.types.is_int16(dtype) or pa.types.is_uint8(dtype):
        return "NUMBER(5)"
    if pa.types.is_int32(dtype) or pa.types.is_uint16(dtype):
        return "NUMBER(10)"
    if pa.types.is_int64(dtype) or pa.types.is_uint32(dtype):
        return "NUMBER(19)"
    if pa.types.is_uint64(dtype):
        raise exceptions.UnsupportedType("There is no support for uint64, please consider int64 or uint32.")
    if pa.types.is_float32(dtype):
        return "BINARY_FLOAT"
    if pa.types.is_float64(dtype):
        return "BINARY_DOUBLE"
    if pa.types.is_boolean(dtype):
        return "NUMBER(3)"
    if pa.types.is_string(dtype) or pa.types.is_large_string(dtype):
        return string_type
    if pa.types.is_timestamp(dtype):
        return "TIMESTAMP"
    if pa.types.is_date(dtype):
        return "DATE"
    if pa.types.is_decimal(dtype):
        return f"NUMBER({dtype.precision},{dtype.scale})"
    if pa.types.is_dictionary(dtype):
        return pyarrow2oracle(dtype=dtype.value_type, string_type=string_type)
    if pa.types.is_binary(dtype):
        return "BLOB"
    raise exceptions.UnsupportedType(f"Unsupported Oracle type: {dtype}")


def pyarrow2postgresql(  # noqa: PLR0911
    dtype: pa.DataType, string_type: str
) -> str:
    """Pyarrow to PostgreSQL data types conversion."""
    if pa.types.is_int8(dtype):
        return "SMALLINT"
    if pa.types.is_int16(dtype) or pa.types.is_uint8(dtype):
        return "SMALLINT"
    if pa.types.is_int32(dtype) or pa.types.is_uint16(dtype):
        return "INTEGER"
    if pa.types.is_int64(dtype) or pa.types.is_uint32(dtype):
        return "BIGINT"
    if pa.types.is_uint64(dtype):
        raise exceptions.UnsupportedType("There is no support for uint64, please consider int64 or uint32.")
    if pa.types.is_float32(dtype):
        return "FLOAT"
    if pa.types.is_float64(dtype):
        return "FLOAT8"
    if pa.types.is_boolean(dtype):
        return "BOOL"
    if pa.types.is_string(dtype) or pa.types.is_large_string(dtype):
        return string_type
    if pa.types.is_timestamp(dtype):
        return "TIMESTAMP"
    if pa.types.is_date(dtype):
        return "DATE"
    if pa.types.is_decimal(dtype):
        return f"DECIMAL({dtype.precision},{dtype.scale})"
    if pa.types.is_dictionary(dtype):
        return pyarrow2postgresql(dtype=dtype.value_type, string_type=string_type)
    if pa.types.is_binary(dtype):
        return "BYTEA"
    raise exceptions.UnsupportedType(f"Unsupported PostgreSQL type: {dtype}")


def pyarrow2sqlserver(  # noqa: PLR0911
    dtype: pa.DataType, string_type: str
) -> str:
    """Pyarrow to Microsoft SQL Server data types conversion."""
    if pa.types.is_int8(dtype):
        return "SMALLINT"
    if pa.types.is_int16(dtype) or pa.types.is_uint8(dtype):
        return "SMALLINT"
    if pa.types.is_int32(dtype) or pa.types.is_uint16(dtype):
        return "INT"
    if pa.types.is_int64(dtype) or pa.types.is_uint32(dtype):
        return "BIGINT"
    if pa.types.is_uint64(dtype):
        raise exceptions.UnsupportedType("There is no support for uint64, please consider int64 or uint32.")
    if pa.types.is_float32(dtype):
        return "FLOAT(24)"
    if pa.types.is_float64(dtype):
        return "FLOAT"
    if pa.types.is_boolean(dtype):
        return "BIT"
    if pa.types.is_string(dtype) or pa.types.is_large_string(dtype):
        return string_type
    if pa.types.is_timestamp(dtype):
        return "DATETIME2"
    if pa.types.is_date(dtype):
        return "DATE"
    if pa.types.is_decimal(dtype):
        return f"DECIMAL({dtype.precision},{dtype.scale})"
    if pa.types.is_dictionary(dtype):
        return pyarrow2sqlserver(dtype=dtype.value_type, string_type=string_type)
    if pa.types.is_binary(dtype):
        return "VARBINARY"
    raise exceptions.UnsupportedType(f"Unsupported SQL Server type: {dtype}")


def pyarrow2timestream(dtype: pa.DataType) -> str:  # noqa: PLR0911
    """Pyarrow to Amazon Timestream data types conversion."""
    if pa.types.is_int8(dtype):
        return "BIGINT"
    if pa.types.is_int16(dtype) or pa.types.is_uint8(dtype):
        return "BIGINT"
    if pa.types.is_int32(dtype) or pa.types.is_uint16(dtype):
        return "BIGINT"
    if pa.types.is_int64(dtype) or pa.types.is_uint32(dtype):
        return "BIGINT"
    if pa.types.is_uint64(dtype):
        return "BIGINT"
    if pa.types.is_float32(dtype):
        return "DOUBLE"
    if pa.types.is_float64(dtype):
        return "DOUBLE"
    if pa.types.is_boolean(dtype):
        return "BOOLEAN"
    if pa.types.is_string(dtype) or pa.types.is_large_string(dtype):
        return "VARCHAR"
    if pa.types.is_date(dtype):
        return "DATE"
    if pa.types.is_time(dtype):
        return "TIME"
    if pa.types.is_timestamp(dtype):
        return "TIMESTAMP"
    raise exceptions.UnsupportedType(f"Unsupported Amazon Timestream measure type: {dtype}")


def _split_fields(s: str) -> Iterator[str]:
    counter: int = 0
    last: int = 0
    for i, x in enumerate(s):
        if x in ("<", "("):
            counter += 1
        elif x in (">", ")"):
            counter -= 1
        elif x == "," and counter == 0:
            yield s[last:i]
            last = i + 1
    yield s[last:]


def _split_struct(s: str) -> list[str]:
    return list(_split_fields(s=s))


def _split_map(s: str) -> list[str]:
    parts: list[str] = list(_split_fields(s=s))
    if len(parts) != 2:
        raise RuntimeError(f"Invalid map fields: {s}")
    return parts


def athena2pyarrow(dtype: str) -> pa.DataType:  # noqa: PLR0911,PLR0912
    """Athena to PyArrow data types conversion."""
    dtype = dtype.strip()
    if dtype.startswith(("array", "struct", "map")):
        orig_dtype: str = dtype
    dtype = dtype.lower().replace(" ", "")
    if dtype == "tinyint":
        return pa.int8()
    if dtype == "smallint":
        return pa.int16()
    if dtype in ("int", "integer"):
        return pa.int32()
    if dtype == "bigint":
        return pa.int64()
    if dtype in ("float", "real"):
        return pa.float32()
    if dtype == "double":
        return pa.float64()
    if dtype == "boolean":
        return pa.bool_()
    if (dtype in ("string", "uuid")) or dtype.startswith("char") or dtype.startswith("varchar"):
        return pa.string()
    if dtype == "timestamp":
        return pa.timestamp(unit="ns")
    if dtype == "date":
        return pa.date32()
    if dtype in ("binary" or "varbinary"):
        return pa.binary()
    if dtype.startswith("decimal") is True:
        precision, scale = dtype.replace("decimal(", "").replace(")", "").split(sep=",")
        return pa.decimal128(precision=int(precision), scale=int(scale))
    if dtype.startswith("array") is True:
        return pa.list_(value_type=athena2pyarrow(dtype=orig_dtype[6:-1]), list_size=-1)
    if dtype.startswith("struct") is True:
        return pa.struct(
            [(f.split(":", 1)[0].strip(), athena2pyarrow(f.split(":", 1)[1])) for f in _split_struct(orig_dtype[7:-1])]
        )
    if dtype.startswith("map") is True:
        parts: list[str] = _split_map(s=orig_dtype[4:-1])
        return pa.map_(athena2pyarrow(parts[0]), athena2pyarrow(parts[1]))
    raise exceptions.UnsupportedType(f"Unsupported Athena type: {dtype}")


def athena2pandas(dtype: str, dtype_backend: str | None = None) -> str:  # noqa: PLR0911
    """Athena to Pandas data types conversion."""
    dtype = dtype.lower()
    if dtype == "tinyint":
        return "Int8" if dtype_backend != "pyarrow" else "int8[pyarrow]"
    if dtype == "smallint":
        return "Int16" if dtype_backend != "pyarrow" else "int16[pyarrow]"
    if dtype in ("int", "integer"):
        return "Int32" if dtype_backend != "pyarrow" else "int32[pyarrow]"
    if dtype == "bigint":
        return "Int64" if dtype_backend != "pyarrow" else "int64[pyarrow]"
    if dtype in ("float", "real"):
        return "float32" if dtype_backend != "pyarrow" else "double[pyarrow]"
    if dtype == "double":
        return "float64" if dtype_backend != "pyarrow" else "double[pyarrow]"
    if dtype == "boolean":
        return "boolean" if dtype_backend != "pyarrow" else "bool[pyarrow]"
    if (dtype == "string") or dtype.startswith("char") or dtype.startswith("varchar"):
        return "string" if dtype_backend != "pyarrow" else "string[pyarrow]"
    if dtype in ("timestamp", "timestamp with time zone"):
        return "datetime64" if dtype_backend != "pyarrow" else "date64[pyarrow]"
    if dtype == "date":
        return "date" if dtype_backend != "pyarrow" else "date32[pyarrow]"
    if dtype.startswith("decimal"):
        return "decimal" if dtype_backend != "pyarrow" else "double[pyarrow]"
    if dtype in ("binary", "varbinary"):
        return "bytes" if dtype_backend != "pyarrow" else "binary[pyarrow]"
    if any(dtype.startswith(t) for t in ["array", "row", "map", "struct"]):
        return "object"
    if dtype == "geometry":
        return "string"
    raise exceptions.UnsupportedType(f"Unsupported Athena type: {dtype}")


def athena2quicksight(dtype: str) -> str:  # noqa: PLR0911
    """Athena to Quicksight data types conversion."""
    dtype = dtype.lower()
    if dtype == "tinyint":
        return "INTEGER"
    if dtype == "smallint":
        return "INTEGER"
    if dtype in ("int", "integer"):
        return "INTEGER"
    if dtype == "bigint":
        return "INTEGER"
    if dtype in ("float", "real"):
        return "DECIMAL"
    if dtype == "double":
        return "DECIMAL"
    if dtype in ("boolean", "bool"):
        return "BOOLEAN"
    if dtype.startswith(("char", "varchar")):
        return "STRING"
    if dtype == "string":
        return "STRING"
    if dtype == "timestamp":
        return "DATETIME"
    if dtype == "date":
        return "DATETIME"
    if dtype.startswith("decimal"):
        return "DECIMAL"
    if dtype == "binary":
        return "BIT"
    raise exceptions.UnsupportedType(f"Unsupported Athena type: {dtype}")


def athena2redshift(  # noqa: PLR0911
    dtype: str, varchar_length: int = 256
) -> str:
    """Athena to Redshift data types conversion."""
    dtype = dtype.lower()
    if dtype == "tinyint":
        return "SMALLINT"
    if dtype == "smallint":
        return "SMALLINT"
    if dtype in ("int", "integer"):
        return "INTEGER"
    if dtype == "bigint":
        return "BIGINT"
    if dtype in ("float", "real"):
        return "FLOAT4"
    if dtype == "double":
        return "FLOAT8"
    if dtype in ("boolean", "bool"):
        return "BOOL"
    if dtype in ("string", "char", "varchar"):
        return f"VARCHAR({varchar_length})"
    if dtype == "timestamp":
        return "TIMESTAMP"
    if dtype == "date":
        return "DATE"
    if dtype == "binary":
        return "VARBYTE"
    if dtype.startswith("decimal"):
        return dtype.upper()
    if dtype.startswith("array") or dtype.startswith("struct"):
        return "SUPER"
    raise exceptions.UnsupportedType(f"Unsupported Redshift type: {dtype}")


def pyarrow2pandas_extension(  # noqa: PLR0911
    dtype: pa.DataType,
) -> pd.api.extensions.ExtensionDtype | None:
    """Pyarrow to Pandas data types conversion."""
    if pa.types.is_int8(dtype):
        return pd.Int8Dtype()
    if pa.types.is_int16(dtype):
        return pd.Int16Dtype()
    if pa.types.is_int32(dtype):
        return pd.Int32Dtype()
    if pa.types.is_int64(dtype):
        return pd.Int64Dtype()
    if pa.types.is_uint8(dtype):
        return pd.UInt8Dtype()
    if pa.types.is_uint16(dtype):
        return pd.UInt16Dtype()
    if pa.types.is_uint32(dtype):
        return pd.UInt32Dtype()
    if pa.types.is_uint64(dtype):
        return pd.UInt64Dtype()
    if pa.types.is_boolean(dtype):
        return pd.BooleanDtype()
    if pa.types.is_string(dtype) or pa.types.is_large_string(dtype):
        return pd.StringDtype()
    return None


def pyarrow2pyarrow_backed_pandas_extension(
    dtype: pa.DataType,
) -> pd.api.extensions.ExtensionDtype | None:
    """Pyarrow to Pandas PyArrow-backed data types conversion."""
    return pd.ArrowDtype(dtype)


def get_pyarrow2pandas_type_mapper(
    dtype_backend: str | None = None,
) -> Callable[[pa.DataType], pd.api.extensions.ExtensionDtype | None]:
    if dtype_backend == "pyarrow":
        return pyarrow2pyarrow_backed_pandas_extension

    return pyarrow2pandas_extension


@engine.dispatch_on_engine
def pyarrow_types_from_pandas(  # noqa: PLR0912,PLR0915
    df: pd.DataFrame, index: bool, ignore_cols: list[str] | None = None, index_left: bool = False
) -> dict[str, pa.DataType]:
    """Extract the related Pyarrow data types from any Pandas DataFrame."""
    # Handle exception data types (e.g. Int64, Int32, string)
    ignore_cols = [] if ignore_cols is None else ignore_cols
    cols: list[str] = []
    cols_dtypes: dict[str, pa.DataType | None] = {}
    for name, dtype in df.dtypes.to_dict().items():
        dtype_str = str(dtype)
        if name in ignore_cols:
            cols_dtypes[name] = None
        elif dtype_str == "Int8":
            cols_dtypes[name] = pa.int8()
        elif dtype_str == "Int16":
            cols_dtypes[name] = pa.int16()
        elif dtype_str == "Int32":
            cols_dtypes[name] = pa.int32()
        elif dtype_str == "Int64":
            cols_dtypes[name] = pa.int64()
        elif dtype_str == "float32":
            cols_dtypes[name] = pa.float32()
        elif dtype_str == "float64":
            cols_dtypes[name] = pa.float64()
        elif dtype_str == "string":
            cols_dtypes[name] = pa.string()
        elif dtype_str == "boolean":
            cols_dtypes[name] = pa.bool_()
        else:
            cols.append(name)

    # Filling cols_dtypes
    for col in cols:
        _logger.debug("Inferring PyArrow type from column: %s", col)
        try:
            schema: pa.Schema = pa.Schema.from_pandas(df=df[[col]], preserve_index=False)
        except pa.ArrowInvalid as ex:
            cols_dtypes[col] = process_not_inferred_dtype(ex)
        except TypeError as ex:
            msg = str(ex)
            if " is required (got type " in msg:
                raise TypeError(
                    f"The {col} columns has a too generic data type ({df[col].dtype}) and seems "
                    f"to have mixed data types ({msg}). "
                    "Please, cast this columns with a more deterministic data type "
                    f"(e.g. df['{col}'] = df['{col}'].astype('string')) or "
                    "pass the column schema as argument"
                    f"(e.g. dtype={{'{col}': 'string'}}"
                ) from ex
            raise
        else:
            cols_dtypes[col] = schema.field(col).type

    # Filling indexes
    indexes: list[str] = []
    if index is True:
        # Get index columns
        try:
            fields = pa.Schema.from_pandas(df=df[[]], preserve_index=True)
        except AttributeError as ae:
            if "'Index' object has no attribute 'head'" not in str(ae):
                raise ae
            # Get index fields from a new df with only index columns
            # Adding indexes as columns via .reset_index() because
            # pa.Schema.from_pandas(.., preserve_index=True) fails with
            # "'Index' object has no attribute 'head'" if using extension
            # dtypes on pandas 1.4.x
            fields = pa.Schema.from_pandas(df=df.reset_index().drop(columns=cols), preserve_index=False)
        for field in fields:
            name = str(field.name)
            # Check if any of the index columns must be ignored
            if name in ignore_cols:
                cols_dtypes[name] = None
            else:
                _logger.debug("Inferring PyArrow type from index: %s", name)
                cols_dtypes[name] = field.type
            indexes.append(name)

    # Merging Index
    sorted_cols: list[str] = indexes + list(df.columns) if index_left is True else list(df.columns) + indexes

    # Filling schema
    columns_types: dict[str, pa.DataType]
    columns_types = {n: cols_dtypes[n] for n in sorted_cols}
    _logger.debug("columns_types: %s", columns_types)
    return columns_types


def pyarrow2pandas_defaults(
    use_threads: bool | int, kwargs: dict[str, Any] | None = None, dtype_backend: str | None = None
) -> dict[str, Any]:
    """Return Pyarrow to Pandas default dictionary arguments."""
    default_kwargs = {
        "use_threads": use_threads,
        "split_blocks": True,
        "self_destruct": True,
        "ignore_metadata": False,
        "types_mapper": get_pyarrow2pandas_type_mapper(dtype_backend),
    }
    if kwargs:
        default_kwargs.update(kwargs)
    return default_kwargs


def process_not_inferred_dtype(ex: pa.ArrowInvalid) -> pa.DataType:
    """Infer data type from PyArrow inference exception."""
    ex_str = str(ex)
    _logger.debug("PyArrow was not able to infer data type:\n%s", ex_str)
    match: Match[str] | None = re.search(
        pattern="Could not convert (.*) with type (.*): did not recognize "
        "Python value type when inferring an Arrow data type",
        string=ex_str,
    )
    if match is None:
        raise ex
    groups: Sequence[str] | None = match.groups()
    if groups is None:
        raise ex
    if len(groups) != 2:
        raise ex
    _logger.debug("groups: %s", groups)
    type_str: str = groups[1]
    if type_str == "UUID":
        return pa.string()
    raise ex


def process_not_inferred_array(ex: pa.ArrowInvalid, values: Any) -> pa.Array:
    """Infer `pyarrow.array` from PyArrow inference exception."""
    dtype = process_not_inferred_dtype(ex=ex)
    if dtype == pa.string():
        array: pa.Array = pa.array(obj=[str(x) if x is not None else None for x in values], type=dtype, safe=True)
    else:
        raise ex
    return array


def athena_types_from_pandas(
    df: pd.DataFrame, index: bool, dtype: dict[str, str] | None = None, index_left: bool = False
) -> dict[str, str]:
    """Extract the related Athena data types from any Pandas DataFrame."""
    casts: dict[str, str] = dtype if dtype else {}
    pa_columns_types: dict[str, pa.DataType | None] = pyarrow_types_from_pandas(
        df=df, index=index, ignore_cols=list(casts.keys()), index_left=index_left
    )
    athena_columns_types: dict[str, str] = {}
    for k, v in pa_columns_types.items():
        if v is None:
            athena_columns_types[k] = casts[k].replace(" ", "")
        else:
            try:
                athena_columns_types[k] = pyarrow2athena(dtype=v)
            except exceptions.UndetectedType as ex:
                raise exceptions.UndetectedType(
                    "Impossible to infer the equivalent Athena data type "
                    f"for the {k} column. "
                    "It is completely empty (only null values) "
                    f"and has a too generic data type ({df[k].dtype}). "
                    "Please, cast this columns with a more deterministic data type "
                    f"(e.g. df['{k}'] = df['{k}'].astype('string')) or "
                    "pass the column schema as argument"
                    f"(e.g. dtype={{'{k}': 'string'}}"
                ) from ex
            except exceptions.UnsupportedType as ex:
                raise exceptions.UnsupportedType(f"Unsupported Pyarrow type: {v} for column {k}") from ex
    _logger.debug("athena_columns_types: %s", athena_columns_types)
    return athena_columns_types


def athena_types_from_pandas_partitioned(
    df: pd.DataFrame,
    index: bool,
    partition_cols: list[str] | None = None,
    dtype: dict[str, str] | None = None,
    index_left: bool = False,
) -> tuple[dict[str, str], dict[str, str]]:
    """Extract the related Athena data types from any Pandas DataFrame considering possible partitions."""
    partitions: list[str] = partition_cols if partition_cols else []
    athena_columns_types: dict[str, str] = athena_types_from_pandas(
        df=df, index=index, dtype=dtype, index_left=index_left
    )
    columns_types: dict[str, str] = {}
    for col, typ in athena_columns_types.items():
        if col not in partitions:
            columns_types[col] = typ
    partitions_types: dict[str, str] = {}
    for par in partitions:
        partitions_types[par] = athena_columns_types[par]
    return columns_types, partitions_types


def pyarrow_schema_from_pandas(
    df: pd.DataFrame, index: bool, ignore_cols: list[str] | None = None, dtype: dict[str, str] | None = None
) -> pa.Schema:
    """Extract the related Pyarrow Schema from any Pandas DataFrame."""
    casts: dict[str, str] = {} if dtype is None else dtype
    _logger.debug("casts: %s", casts)
    ignore: list[str] = [] if ignore_cols is None else ignore_cols
    ignore_plus = ignore + list(casts.keys())
    columns_types: dict[str, pa.DataType | None] = pyarrow_types_from_pandas(
        df=df, index=index, ignore_cols=ignore_plus
    )
    for k, v in casts.items():
        if (k not in ignore) and (k in df.columns or _is_index_name(k, df.index)):
            columns_types[k] = athena2pyarrow(dtype=v)
    columns_types = {k: v for k, v in columns_types.items() if v is not None}
    _logger.debug("columns_types: %s", columns_types)
    return pa.schema(fields=columns_types)


def _is_index_name(name: str, index: pd.Index) -> bool:
    if name in index.names:
        # named index level
        return True

    if (match := re.match(r"__index_level_(?P<level>\d+)__", name)) is not None:
        # unnamed index level
        if len(index.names) > (level := int(match.group("level"))):
            return index.names[level] is None

    return False


def athena_types_from_pyarrow_schema(
    schema: pa.Schema,
    ignore_null: bool = False,
) -> dict[str, str]:
    """Extract the related Athena data types from any PyArrow Schema considering possible partitions."""
    columns_types: dict[str, str] = {str(f.name): pyarrow2athena(dtype=f.type, ignore_null=ignore_null) for f in schema}
    _logger.debug("columns_types: %s", columns_types)
    return columns_types


def cast_pandas_with_athena_types(
    df: pd.DataFrame, dtype: dict[str, str], dtype_backend: str | None = None
) -> pd.DataFrame:
    """Cast columns in a Pandas DataFrame."""
    mutability_ensured: bool = False
    for col, athena_type in dtype.items():
        if (
            (col in df.columns)
            and (athena_type.startswith("array") is False)
            and (athena_type.startswith("struct") is False)
            and (athena_type.startswith("map") is False)
        ):
            desired_type: str = athena2pandas(dtype=athena_type, dtype_backend=dtype_backend)
            current_type: str = _normalize_pandas_dtype_name(dtype=str(df[col].dtypes))
            if desired_type != current_type:  # Needs conversion
                _logger.debug("current_type: %s -> desired_type: %s", current_type, desired_type)
                if mutability_ensured is False:
                    df = _arrow.ensure_df_is_mutable(df=df)
                    mutability_ensured = True
                _cast_pandas_column(df=df, col=col, current_type=current_type, desired_type=desired_type)
    return df


def _normalize_pandas_dtype_name(dtype: str) -> str:
    if dtype.startswith("datetime64") is True:
        return "datetime64"
    if dtype.startswith("decimal") is True:
        return "decimal"
    return dtype


def _cast2date(value: Any) -> Any:
    if isinstance(value, float) and (np.isnan(value) or np.isinf(value)):
        return None
    if pd.isna(value) or value is None:
        return None
    if isinstance(value, datetime.date):
        return value
    return pd.to_datetime(value).date()


def _cast_pandas_column(df: pd.DataFrame, col: str, current_type: str, desired_type: str) -> pd.DataFrame:
    if desired_type == "datetime64":
        df[col] = pd.to_datetime(df[col])
    elif desired_type == "date":
        df[col] = df[col].apply(lambda x: _cast2date(value=x)).replace(to_replace={pd.NaT: None})
    elif desired_type == "bytes":
        df[col] = df[col].astype("string").str.encode(encoding="utf-8").replace(to_replace={pd.NA: None})
    elif desired_type == "decimal":
        # First cast to string
        df = _cast_pandas_column(df=df, col=col, current_type=current_type, desired_type="string")
        # Then cast to decimal
        df[col] = df[col].apply(lambda x: Decimal(str(x)) if str(x) not in ("", "none", "None", " ", "<NA>") else None)
    else:
        try:
            df[col] = df[col].astype(desired_type)
        except TypeError as ex:
            _logger.debug("Column: %s", col)
            if "object cannot be converted to an IntegerDtype" not in str(ex):
                raise ex
            warnings.warn(
                "Object cannot be converted to an IntegerDtype. Integer columns in Python cannot contain "
                "missing values. If your input data contains missing values, it will be encoded as floats"
                "which may cause precision loss.",
                UserWarning,
            )
            df[col] = (
                df[col]
                .apply(lambda x: int(x) if str(x) not in ("", "none", "None", " ", "<NA>") else None)
                .astype(desired_type)
            )
    return df


def database_types_from_pandas(
    df: pd.DataFrame,
    index: bool,
    dtype: dict[str, str] | None,
    varchar_lengths_default: int | str,
    varchar_lengths: dict[str, int] | None,
    converter_func: Callable[[pa.DataType, str], str],
) -> dict[str, str]:
    """Extract database data types from a Pandas DataFrame."""
    _dtype: dict[str, str] = dtype if dtype else {}
    _varchar_lengths: dict[str, int] = varchar_lengths if varchar_lengths else {}
    pyarrow_types: dict[str, pa.DataType | None] = pyarrow_types_from_pandas(
        df=df, index=index, ignore_cols=list(_dtype.keys()), index_left=True
    )
    database_types: dict[str, str] = {}
    for col_name, col_dtype in pyarrow_types.items():
        if col_name in _dtype:
            database_types[col_name] = _dtype[col_name]
        else:
            if col_name in _varchar_lengths:
                string_type: str = f"VARCHAR({_varchar_lengths[col_name]})"
            elif isinstance(varchar_lengths_default, str):
                string_type = varchar_lengths_default
            else:
                string_type = f"VARCHAR({varchar_lengths_default})"
            database_types[col_name] = converter_func(col_dtype, string_type)
    _logger.debug("database_types: %s", database_types)
    return database_types


def timestream_type_from_pandas(df: pd.DataFrame) -> list[str]:
    """Extract Amazon Timestream types from a Pandas DataFrame."""
    return [
        pyarrow2timestream(pyarrow_type)
        for pyarrow_type in pyarrow_types_from_pandas(df=df, index=False, ignore_cols=[]).values()
    ]


def get_arrow_timestamp_unit(data_type: pa.lib.DataType) -> Any:
    """Return unit of pyarrow timestamp. If the pyarrow type is not timestamp then None is returned."""
    if isinstance(data_type, pa.lib.TimestampType):
        return data_type.unit
    return None
