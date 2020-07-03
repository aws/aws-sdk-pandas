"""Internal (private) Data Types Module."""

import logging
import re
from decimal import Decimal
from typing import Any, Dict, List, Match, Optional, Sequence, Tuple

import pandas as pd  # type: ignore
import pyarrow as pa  # type: ignore
import pyarrow.parquet  # type: ignore
import sqlalchemy  # type: ignore
import sqlalchemy.dialects.mysql  # type: ignore
import sqlalchemy.dialects.postgresql  # type: ignore
import sqlalchemy_redshift.dialect  # type: ignore
from sqlalchemy.sql.visitors import VisitableType  # type: ignore

from awswrangler import _utils, exceptions

_logger: logging.Logger = logging.getLogger(__name__)


def athena2pyarrow(dtype: str) -> pa.DataType:  # pylint: disable=too-many-return-statements
    """Athena to PyArrow data types conversion."""
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
        return pa.list_(value_type=athena2pyarrow(dtype=dtype[6:-1]), list_size=-1)
    if dtype.startswith("struct") is True:
        return pa.struct([(f.split(":", 1)[0], athena2pyarrow(f.split(":", 1)[1])) for f in dtype[7:-1].split(",")])
    if dtype.startswith("map") is True:  # pragma: no cover
        return pa.map_(athena2pyarrow(dtype[4:-1].split(",", 1)[0]), athena2pyarrow(dtype[4:-1].split(",", 1)[1]))
    raise exceptions.UnsupportedType(f"Unsupported Athena type: {dtype}")  # pragma: no cover


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
    raise exceptions.UnsupportedType(f"Unsupported Athena type: {dtype}")  # pragma: no cover


def athena2redshift(  # pylint: disable=too-many-branches,too-many-return-statements
    dtype: str, varchar_length: int = 256
) -> str:
    """Athena to Redshift data types conversion."""
    dtype = dtype.lower()
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
    raise exceptions.UnsupportedType(f"Unsupported Athena type: {dtype}")  # pragma: no cover


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
    if dtype == "binary":  # pragma: no cover
        return "BIT"
    raise exceptions.UnsupportedType(f"Unsupported Athena type: {dtype}")  # pragma: no cover


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
    if pa.types.is_map(dtype):  # pragma: no cover
        return f"map<{pyarrow2athena(dtype=dtype.key_type)}, {pyarrow2athena(dtype=dtype.item_type)}>"
    if dtype == pa.null():
        raise exceptions.UndetectedType("We can not infer the data type from an entire null object column")
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


def pyarrow2sqlalchemy(  # pylint: disable=too-many-branches,too-many-return-statements
    dtype: pa.DataType, db_type: str
) -> Optional[VisitableType]:
    """Pyarrow to Athena data types conversion."""
    if pa.types.is_int8(dtype):
        return sqlalchemy.types.SmallInteger
    if pa.types.is_int16(dtype):
        return sqlalchemy.types.SmallInteger
    if pa.types.is_int32(dtype):
        return sqlalchemy.types.Integer
    if pa.types.is_int64(dtype):
        return sqlalchemy.types.BigInteger
    if pa.types.is_float32(dtype):
        return sqlalchemy.types.Float
    if pa.types.is_float64(dtype):
        if db_type == "mysql":
            return sqlalchemy.dialects.mysql.DOUBLE
        if db_type == "postgresql":
            return sqlalchemy.dialects.postgresql.DOUBLE_PRECISION
        if db_type == "redshift":
            return sqlalchemy_redshift.dialect.DOUBLE_PRECISION
        raise exceptions.InvalidDatabaseType(
            f"{db_type} is a invalid database type, please choose between postgresql, mysql and redshift."
        )  # pragma: no cover
    if pa.types.is_boolean(dtype):
        return sqlalchemy.types.Boolean
    if pa.types.is_string(dtype):
        if db_type == "mysql":
            return sqlalchemy.types.Text
        if db_type == "postgresql":
            return sqlalchemy.types.Text
        if db_type == "redshift":
            return sqlalchemy.types.VARCHAR(length=256)
        raise exceptions.InvalidDatabaseType(
            f"{db_type} is a invalid database type. " f"Please choose between postgresql, mysql and redshift."
        )  # pragma: no cover
    if pa.types.is_timestamp(dtype):
        return sqlalchemy.types.DateTime
    if pa.types.is_date(dtype):
        return sqlalchemy.types.Date
    if pa.types.is_binary(dtype):
        if db_type == "redshift":
            raise exceptions.UnsupportedType("Binary columns are not supported for Redshift.")  # pragma: no cover
        return sqlalchemy.types.Binary
    if pa.types.is_decimal(dtype):
        return sqlalchemy.types.Numeric(precision=dtype.precision, scale=dtype.scale)
    if pa.types.is_dictionary(dtype):
        return pyarrow2sqlalchemy(dtype=dtype.value_type, db_type=db_type)
    if dtype == pa.null():  # pragma: no cover
        return None
    raise exceptions.UnsupportedType(f"Unsupported Pyarrow type: {dtype}")  # pragma: no cover


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
        except pa.ArrowInvalid as ex:  # pragma: no cover
            cols_dtypes[col] = process_not_inferred_dtype(ex)
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
    match: Optional[Match] = re.search(
        pattern="Could not convert (.*) with type (.*): did not recognize "
        "Python value type when inferring an Arrow data type",
        string=ex_str,
    )
    if match is None:
        raise ex  # pragma: no cover
    groups: Optional[Sequence[str]] = match.groups()
    if groups is None:
        raise ex  # pragma: no cover
    if len(groups) != 2:
        raise ex  # pragma: no cover
    _logger.debug("groups: %s", groups)
    type_str: str = groups[1]
    if type_str == "UUID":
        return pa.string()
    raise ex  # pragma: no cover


def process_not_inferred_array(ex: pa.ArrowInvalid, values: Any) -> pa.Array:
    """Infer `pyarrow.array` from PyArrow inference exception."""
    dtype = process_not_inferred_dtype(ex=ex)
    if dtype == pa.string():
        array: pa.Array = pa.array(obj=[str(x) for x in values], type=dtype, safe=True)
    else:
        raise ex  # pragma: no cover
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
            athena_columns_types[k] = pyarrow2athena(dtype=v)
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
        partitions_types = {p.name: pyarrow2athena(p.dictionary.type) for p in partitions}  # pragma: no cover
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
        return "decimal"  # pragma: no cover
    return dtype


def _cast_pandas_column(df: pd.DataFrame, col: str, current_type: str, desired_type: str) -> pd.DataFrame:
    if desired_type == "datetime64":
        df[col] = pd.to_datetime(df[col])
    elif desired_type == "date":
        df[col] = pd.to_datetime(df[col]).dt.date.replace(to_replace={pd.NaT: None})
    elif desired_type == "bytes":
        df[col] = df[col].astype("string").str.encode(encoding="utf-8").replace(to_replace={pd.NA: None})
    elif desired_type == "decimal":
        # First cast to string
        df = _cast_pandas_column(df=df, col=col, current_type=current_type, desired_type="string")
        # Then cast to decimal
        df[col] = df[col].apply(lambda x: Decimal(str(x)) if str(x) not in ("", "none", "None", " ", "<NA>") else None)
    elif desired_type == "string":
        if current_type.lower().startswith("int") is True:
            df[col] = df[col].astype(str).astype("string")
        elif current_type.startswith("float") is True:
            df[col] = df[col].astype(str).astype("string")
        elif current_type in ("object", "category"):
            df[col] = df[col].astype(str).astype("string")
        else:
            df[col] = df[col].astype("string")
    else:
        try:
            df[col] = df[col].astype(desired_type)
        except TypeError as ex:
            if "object cannot be converted to an IntegerDtype" not in str(ex):
                raise ex  # pragma: no cover
            df[col] = (
                df[col]
                .apply(lambda x: int(x) if str(x) not in ("", "none", "None", " ", "<NA>") else None)
                .astype(desired_type)
            )
    return df


def sqlalchemy_types_from_pandas(
    df: pd.DataFrame, db_type: str, dtype: Optional[Dict[str, VisitableType]] = None
) -> Dict[str, VisitableType]:
    """Extract the related SQLAlchemy data types from any Pandas DataFrame."""
    casts: Dict[str, VisitableType] = dtype if dtype is not None else {}
    pa_columns_types: Dict[str, Optional[pa.DataType]] = pyarrow_types_from_pandas(
        df=df, index=False, ignore_cols=list(casts.keys())
    )
    sqlalchemy_columns_types: Dict[str, VisitableType] = {}
    for k, v in pa_columns_types.items():
        if v is None:
            sqlalchemy_columns_types[k] = casts[k]
        else:
            sqlalchemy_columns_types[k] = pyarrow2sqlalchemy(dtype=v, db_type=db_type)
    _logger.debug("sqlalchemy_columns_types: %s", sqlalchemy_columns_types)
    return sqlalchemy_columns_types
