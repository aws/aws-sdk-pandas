"""Internal (private) Data Types Module."""

import datetime
import logging
import re
from decimal import Decimal
from typing import Any, Callable, Dict, Iterator, List, Match, Optional, Sequence, Tuple, Union

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet

from awswrangler import _utils, exceptions

_logger: logging.Logger = logging.getLogger(__name__)


def pyarrow2athena(dtype: pa.DataType) -> str:  # pylint: disable=too-many-branches,too-many-return-statements
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
        return f"struct<{','.join([f'{f.name}:{pyarrow2athena(dtype=f.type)}' for f in dtype])}>"
    if pa.types.is_map(dtype):
        return f"map<{pyarrow2athena(dtype=dtype.key_type)}, {pyarrow2athena(dtype=dtype.item_type)}>"
    if dtype == pa.null():
        raise exceptions.UndetectedType("We can not infer the data type from an entire null object column")
    raise exceptions.UnsupportedType(f"Unsupported Pyarrow type: {dtype}")


def pyarrow2redshift(  # pylint: disable=too-many-branches,too-many-return-statements
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
    if pa.types.is_string(dtype):
        return string_type
    if pa.types.is_timestamp(dtype):
        return "TIMESTAMP"
    if pa.types.is_date(dtype):
        return "DATE"
    if pa.types.is_decimal(dtype):
        return f"DECIMAL({dtype.precision},{dtype.scale})"
    if pa.types.is_dictionary(dtype):
        return pyarrow2redshift(dtype=dtype.value_type, string_type=string_type)
    if pa.types.is_list(dtype) or pa.types.is_struct(dtype):
        return "SUPER"
    raise exceptions.UnsupportedType(f"Unsupported Redshift type: {dtype}")


def pyarrow2mysql(  # pylint: disable=too-many-branches,too-many-return-statements
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
    if pa.types.is_string(dtype):
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


def pyarrow2postgresql(  # pylint: disable=too-many-branches,too-many-return-statements
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
    if pa.types.is_string(dtype):
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


def pyarrow2sqlserver(  # pylint: disable=too-many-branches,too-many-return-statements
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
    if pa.types.is_string(dtype):
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
    raise exceptions.UnsupportedType(f"Unsupported PostgreSQL type: {dtype}")


def pyarrow2timestream(dtype: pa.DataType) -> str:  # pylint: disable=too-many-branches,too-many-return-statements
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
    if pa.types.is_string(dtype):
        return "VARCHAR"
    raise exceptions.UnsupportedType(f"Unsupported Amazon Timestream measure type: {dtype}")


def _split_fields(s: str) -> Iterator[str]:
    counter: int = 0
    last: int = 0
    for i, x in enumerate(s):
        if x == "<":
            counter += 1
        elif x == ">":
            counter -= 1
        elif x == "," and counter == 0:
            yield s[last:i]
            last = i + 1
    yield s[last:]


def _split_struct(s: str) -> List[str]:
    return list(_split_fields(s=s))


def _split_map(s: str) -> List[str]:
    parts: List[str] = list(_split_fields(s=s))
    if len(parts) != 2:
        raise RuntimeError(f"Invalid map fields: {s}")
    return parts


def athena2pyarrow(dtype: str) -> pa.DataType:  # pylint: disable=too-many-return-statements,too-many-branches
    """Athena to PyArrow data types conversion."""
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
    if (dtype == "string") or dtype.startswith("char") or dtype.startswith("varchar"):
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
            [(f.split(":", 1)[0], athena2pyarrow(f.split(":", 1)[1])) for f in _split_struct(orig_dtype[7:-1])]
        )
    if dtype.startswith("map") is True:
        parts: List[str] = _split_map(s=orig_dtype[4:-1])
        return pa.map_(athena2pyarrow(parts[0]), athena2pyarrow(parts[1]))
    raise exceptions.UnsupportedType(f"Unsupported Athena type: {dtype}")


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
    if dtype in ("float", "real"):
        return "float32"
    if dtype == "double":
        return "float64"
    if dtype == "boolean":
        return "boolean"
    if (dtype == "string") or dtype.startswith("char") or dtype.startswith("varchar"):
        return "string"
    if dtype in ("timestamp", "timestamp with time zone"):
        return "datetime64"
    if dtype == "date":
        return "date"
    if dtype.startswith("decimal"):
        return "decimal"
    if dtype in ("binary", "varbinary"):
        return "bytes"
    raise exceptions.UnsupportedType(f"Unsupported Athena type: {dtype}")


def athena2quicksight(dtype: str) -> str:  # pylint: disable=too-many-branches,too-many-return-statements
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
    if dtype in ("string", "char", "varchar"):
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


def athena2redshift(  # pylint: disable=too-many-branches,too-many-return-statements
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
    if dtype.startswith("decimal"):
        return dtype.upper()
    if dtype.startswith("array") or dtype.startswith("struct"):
        return "SUPER"
    raise exceptions.UnsupportedType(f"Unsupported Redshift type: {dtype}")


def pyarrow2pandas_extension(  # pylint: disable=too-many-branches,too-many-return-statements
    dtype: pa.DataType,
) -> Optional[pd.api.extensions.ExtensionDtype]:
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
    if pa.types.is_string(dtype):
        return pd.StringDtype()
    return None


def pyarrow_types_from_pandas(
    df: pd.DataFrame, index: bool, ignore_cols: Optional[List[str]] = None, index_left: bool = False
) -> Dict[str, pa.DataType]:
    """Extract the related Pyarrow data types from any Pandas DataFrame."""
    # Handle exception data types (e.g. Int64, Int32, string)
    ignore_cols = [] if ignore_cols is None else ignore_cols
    cols: List[str] = []
    cols_dtypes: Dict[str, Optional[pa.DataType]] = {}
    for name, dtype in df.dtypes.to_dict().items():
        dtype = str(dtype)
        if name in ignore_cols:
            cols_dtypes[name] = None
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
                    "pass the column schema as argument for AWS Data Wrangler "
                    f"(e.g. dtype={{'{col}': 'string'}}"
                ) from ex
            raise
        else:
            cols_dtypes[col] = schema.field(col).type

    # Filling indexes
    indexes: List[str] = []
    if index is True:
        for field in pa.Schema.from_pandas(df=df[[]], preserve_index=True):
            name = str(field.name)
            _logger.debug("Inferring PyArrow type from index: %s", name)
            cols_dtypes[name] = field.type
            indexes.append(name)

    # Merging Index
    sorted_cols: List[str] = indexes + list(df.columns) if index_left is True else list(df.columns) + indexes

    # Filling schema
    columns_types: Dict[str, pa.DataType]
    columns_types = {n: cols_dtypes[n] for n in sorted_cols}
    _logger.debug("columns_types: %s", columns_types)
    return columns_types


def process_not_inferred_dtype(ex: pa.ArrowInvalid) -> pa.DataType:
    """Infer data type from PyArrow inference exception."""
    ex_str = str(ex)
    _logger.debug("PyArrow was not able to infer data type:\n%s", ex_str)
    match: Optional[Match[str]] = re.search(
        pattern="Could not convert (.*) with type (.*): did not recognize "
        "Python value type when inferring an Arrow data type",
        string=ex_str,
    )
    if match is None:
        raise ex
    groups: Optional[Sequence[str]] = match.groups()
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
        array: pa.Array = pa.array(obj=[str(x) for x in values], type=dtype, safe=True)
    else:
        raise ex
    return array


def athena_types_from_pandas(
    df: pd.DataFrame, index: bool, dtype: Optional[Dict[str, str]] = None, index_left: bool = False
) -> Dict[str, str]:
    """Extract the related Athena data types from any Pandas DataFrame."""
    casts: Dict[str, str] = dtype if dtype else {}
    pa_columns_types: Dict[str, Optional[pa.DataType]] = pyarrow_types_from_pandas(
        df=df, index=index, ignore_cols=list(casts.keys()), index_left=index_left
    )
    athena_columns_types: Dict[str, str] = {}
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
                    "pass the column schema as argument for AWS Data Wrangler "
                    f"(e.g. dtype={{'{k}': 'string'}}"
                ) from ex
    _logger.debug("athena_columns_types: %s", athena_columns_types)
    return athena_columns_types


def athena_types_from_pandas_partitioned(
    df: pd.DataFrame,
    index: bool,
    partition_cols: Optional[List[str]] = None,
    dtype: Optional[Dict[str, str]] = None,
    index_left: bool = False,
) -> Tuple[Dict[str, str], Dict[str, str]]:
    """Extract the related Athena data types from any Pandas DataFrame considering possible partitions."""
    partitions: List[str] = partition_cols if partition_cols else []
    athena_columns_types: Dict[str, str] = athena_types_from_pandas(
        df=df, index=index, dtype=dtype, index_left=index_left
    )
    columns_types: Dict[str, str] = {}
    for col, typ in athena_columns_types.items():
        if col not in partitions:
            columns_types[col] = typ
    partitions_types: Dict[str, str] = {}
    for par in partitions:
        partitions_types[par] = athena_columns_types[par]
    return columns_types, partitions_types


def pyarrow_schema_from_pandas(
    df: pd.DataFrame, index: bool, ignore_cols: Optional[List[str]] = None, dtype: Optional[Dict[str, str]] = None
) -> pa.Schema:
    """Extract the related Pyarrow Schema from any Pandas DataFrame."""
    casts: Dict[str, str] = {} if dtype is None else dtype
    _logger.debug("casts: %s", casts)
    ignore: List[str] = [] if ignore_cols is None else ignore_cols
    ignore_plus = ignore + list(casts.keys())
    columns_types: Dict[str, Optional[pa.DataType]] = pyarrow_types_from_pandas(
        df=df, index=index, ignore_cols=ignore_plus
    )
    for k, v in casts.items():
        if (k in df.columns) and (k not in ignore):
            columns_types[k] = athena2pyarrow(dtype=v)
    columns_types = {k: v for k, v in columns_types.items() if v is not None}
    _logger.debug("columns_types: %s", columns_types)
    return pa.schema(fields=columns_types)


def athena_types_from_pyarrow_schema(
    schema: pa.Schema, partitions: Optional[pyarrow.parquet.ParquetPartitions]
) -> Tuple[Dict[str, str], Optional[Dict[str, str]]]:
    """Extract the related Athena data types from any PyArrow Schema considering possible partitions."""
    columns_types: Dict[str, str] = {str(f.name): pyarrow2athena(dtype=f.type) for f in schema}
    _logger.debug("columns_types: %s", columns_types)
    partitions_types: Optional[Dict[str, str]] = None
    if partitions is not None:
        partitions_types = {p.name: pyarrow2athena(p.dictionary.type) for p in partitions}
    _logger.debug("partitions_types: %s", partitions_types)
    return columns_types, partitions_types


def cast_pandas_with_athena_types(df: pd.DataFrame, dtype: Dict[str, str]) -> pd.DataFrame:
    """Cast columns in a Pandas DataFrame."""
    mutability_ensured: bool = False
    for col, athena_type in dtype.items():
        if (
            (col in df.columns)
            and (athena_type.startswith("array") is False)
            and (athena_type.startswith("struct") is False)
            and (athena_type.startswith("map") is False)
        ):
            desired_type: str = athena2pandas(dtype=athena_type)
            current_type: str = _normalize_pandas_dtype_name(dtype=str(df[col].dtypes))
            if desired_type != current_type:  # Needs conversion
                _logger.debug("current_type: %s -> desired_type: %s", current_type, desired_type)
                if mutability_ensured is False:
                    df = _utils.ensure_df_is_mutable(df=df)
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
            if "object cannot be converted to an IntegerDtype" not in str(ex):
                raise ex
            df[col] = (
                df[col]
                .apply(lambda x: int(x) if str(x) not in ("", "none", "None", " ", "<NA>") else None)
                .astype(desired_type)
            )
    return df


def database_types_from_pandas(
    df: pd.DataFrame,
    index: bool,
    dtype: Optional[Dict[str, str]],
    varchar_lengths_default: Union[int, str],
    varchar_lengths: Optional[Dict[str, int]],
    converter_func: Callable[[pa.DataType, str], str],
) -> Dict[str, str]:
    """Extract database data types from a Pandas DataFrame."""
    _dtype: Dict[str, str] = dtype if dtype else {}
    _varchar_lengths: Dict[str, int] = varchar_lengths if varchar_lengths else {}
    pyarrow_types: Dict[str, Optional[pa.DataType]] = pyarrow_types_from_pandas(
        df=df, index=index, ignore_cols=list(_dtype.keys()), index_left=True
    )
    database_types: Dict[str, str] = {}
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


def timestream_type_from_pandas(df: pd.DataFrame) -> str:
    """Extract Amazon Timestream types from a Pandas DataFrame."""
    pyarrow_types: Dict[str, Optional[pa.DataType]] = pyarrow_types_from_pandas(df=df, index=False, ignore_cols=[])
    if len(pyarrow_types) != 1 or list(pyarrow_types.values())[0] is None:
        raise RuntimeError(f"Invalid pyarrow_types: {pyarrow_types}")
    pyarrow_type: pa.DataType = list(pyarrow_types.values())[0]
    _logger.debug("pyarrow_type: %s", pyarrow_type)
    return pyarrow2timestream(dtype=pyarrow_type)
