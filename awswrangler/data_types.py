import logging
from datetime import datetime, date

import pyarrow

from awswrangler.exceptions import UnsupportedType

logger = logging.getLogger(__name__)


def athena2pandas(dtype):
    dtype = dtype.lower()
    if dtype in ["int", "integer", "bigint", "smallint", "tinyint"]:
        return "Int64"
    elif dtype in ["float", "double", "real"]:
        return "float64"
    elif dtype == "boolean":
        return "bool"
    elif dtype in ["string", "char", "varchar"]:
        return "str"
    elif dtype == "timestamp":
        return "datetime64"
    elif dtype == "date":
        return "date"
    elif dtype == "array":
        return "literal_eval"
    else:
        raise UnsupportedType(f"Unsupported Athena type: {dtype}")


def athena2pyarrow(dtype):
    dtype = dtype.lower()
    if dtype == "tinyint":
        return "int8"
    if dtype == "smallint":
        return "int16"
    elif dtype in ["int", "integer"]:
        return "int32"
    elif dtype == "bigint":
        return "int64"
    elif dtype == "float":
        return "float32"
    elif dtype == "double":
        return "float64"
    elif dtype in ["boolean", "bool"]:
        return "bool"
    elif dtype in ["string", "char", "varchar", "array", "row", "map"]:
        return "string"
    elif dtype == "timestamp":
        return "timestamp[ns]"
    elif dtype == "date":
        return "date32"
    else:
        raise UnsupportedType(f"Unsupported Athena type: {dtype}")


def athena2python(dtype):
    dtype = dtype.lower()
    if dtype in ["int", "integer", "bigint", "smallint", "tinyint"]:
        return int
    elif dtype in ["float", "double", "real"]:
        return float
    elif dtype == "boolean":
        return bool
    elif dtype in ["string", "char", "varchar", "array", "row", "map"]:
        return str
    elif dtype == "timestamp":
        return datetime
    elif dtype == "date":
        return date
    else:
        raise UnsupportedType(f"Unsupported Athena type: {dtype}")


def athena2redshift(dtype):
    dtype = dtype.lower()
    if dtype == "smallint":
        return "SMALLINT"
    elif dtype in ["int", "integer"]:
        return "INTEGER"
    elif dtype == "bigint":
        return "BIGINT"
    elif dtype == "float":
        return "FLOAT4"
    elif dtype == "double":
        return "FLOAT8"
    elif dtype in ["boolean", "bool"]:
        return "BOOL"
    elif dtype in ["string", "char", "varchar", "array", "row", "map"]:
        return "VARCHAR(256)"
    elif dtype == "timestamp":
        return "TIMESTAMP"
    elif dtype == "date":
        return "DATE"
    else:
        raise UnsupportedType(f"Unsupported Athena type: {dtype}")


def pandas2athena(dtype):
    dtype = dtype.lower()
    if dtype == "int32":
        return "int"
    elif dtype in ["int64", "Int64"]:
        return "bigint"
    elif dtype == "float32":
        return "float"
    elif dtype == "float64":
        return "double"
    elif dtype == "bool":
        return "boolean"
    elif dtype == "object":
        return "string"
    elif dtype.startswith("datetime64"):
        return "timestamp"
    else:
        raise UnsupportedType(f"Unsupported Pandas type: {dtype}")


def pandas2redshift(dtype):
    dtype = dtype.lower()
    if dtype == "int32":
        return "INTEGER"
    elif dtype == "int64":
        return "BIGINT"
    elif dtype == "float32":
        return "FLOAT4"
    elif dtype == "float64":
        return "FLOAT8"
    elif dtype == "bool":
        return "BOOLEAN"
    elif dtype == "object" and isinstance(dtype, str):
        return "VARCHAR(256)"
    elif dtype[:10] == "datetime64":
        return "TIMESTAMP"
    else:
        raise UnsupportedType("Unsupported Pandas type: " + dtype)


def pyarrow2athena(dtype):
    dtype_str = str(dtype).lower()
    if dtype_str == "int8":
        return "tinyint"
    elif dtype_str == "int16":
        return "smallint"
    elif dtype_str == "int32":
        return "int"
    elif dtype_str == "int64":
        return "bigint"
    elif dtype_str == "float":
        return "float"
    elif dtype_str == "double":
        return "double"
    elif dtype_str == "bool":
        return "boolean"
    elif dtype_str == "string":
        return "string"
    elif dtype_str.startswith("timestamp"):
        return "timestamp"
    elif dtype_str.startswith("date"):
        return "date"
    elif dtype_str.startswith("list"):
        return f"array<{pyarrow2athena(dtype.value_type)}>"
    else:
        raise UnsupportedType(f"Unsupported Pyarrow type: {dtype}")


def pyarrow2redshift(dtype):
    dtype_str = str(dtype).lower()
    if dtype_str == "int16":
        return "SMALLINT"
    elif dtype_str == "int32":
        return "INT"
    elif dtype_str == "int64":
        return "BIGINT"
    elif dtype_str == "float":
        return "FLOAT4"
    elif dtype_str == "double":
        return "FLOAT8"
    elif dtype_str == "bool":
        return "BOOLEAN"
    elif dtype_str == "string":
        return "VARCHAR(256)"
    elif dtype_str.startswith("timestamp"):
        return "TIMESTAMP"
    elif dtype_str.startswith("date"):
        return "DATE"
    else:
        raise UnsupportedType(f"Unsupported Pyarrow type: {dtype}")


def python2athena(python_type):
    python_type = str(python_type)
    if python_type == "<class 'int'>":
        return "bigint"
    elif python_type == "<class 'float'>":
        return "double"
    elif python_type == "<class 'boll'>":
        return "boolean"
    elif python_type == "<class 'str'>":
        return "string"
    elif python_type == "<class 'datetime.datetime'>":
        return "timestamp"
    elif python_type == "<class 'datetime.date'>":
        return "date"
    else:
        raise UnsupportedType(f"Unsupported Python type: {python_type}")


def redshift2athena(dtype):
    dtype_str = str(dtype)
    if dtype_str in ["SMALLINT", "INT2"]:
        return "smallint"
    elif dtype_str in ["INTEGER", "INT", "INT4"]:
        return "int"
    elif dtype_str in ["BIGINT", "INT8"]:
        return "bigint"
    elif dtype_str in ["REAL", "FLOAT4"]:
        return "float"
    elif dtype_str in ["DOUBLE PRECISION", "FLOAT8", "FLOAT"]:
        return "double"
    elif dtype_str in ["BOOLEAN", "BOOL"]:
        return "boolean"
    elif dtype_str in ["VARCHAR", "CHARACTER VARYING", "NVARCHAR", "TEXT"]:
        return "string"
    elif dtype_str == "DATE":
        return "date"
    elif dtype_str == "TIMESTAMP":
        return "timestamp"
    else:
        raise UnsupportedType(f"Unsupported Redshift type: {dtype_str}")


def redshift2pyarrow(dtype):
    dtype_str = str(dtype)
    if dtype_str in ["SMALLINT", "INT2"]:
        return "int16"
    elif dtype_str in ["INTEGER", "INT", "INT4"]:
        return "int32"
    elif dtype_str in ["BIGINT", "INT8"]:
        return "int64"
    elif dtype_str in ["REAL", "FLOAT4"]:
        return "float32"
    elif dtype_str in ["DOUBLE PRECISION", "FLOAT8", "FLOAT"]:
        return "float64"
    elif dtype_str in ["BOOLEAN", "BOOL"]:
        return "bool"
    elif dtype_str in ["VARCHAR", "CHARACTER VARYING", "NVARCHAR", "TEXT"]:
        return "string"
    elif dtype_str == "DATE":
        return "date32"
    elif dtype_str == "TIMESTAMP":
        return "timestamp[ns]"
    else:
        raise UnsupportedType(f"Unsupported Redshift type: {dtype_str}")


def spark2redshift(dtype):
    dtype = dtype.lower()
    if dtype == "smallint":
        return "SMALLINT"
    elif dtype == "int":
        return "INT"
    elif dtype == "bigint":
        return "BIGINT"
    elif dtype == "float":
        return "FLOAT4"
    elif dtype == "double":
        return "FLOAT8"
    elif dtype == "bool":
        return "BOOLEAN"
    elif dtype == "timestamp":
        return "TIMESTAMP"
    elif dtype == "date":
        return "DATE"
    elif dtype == "string":
        return "VARCHAR(256)"
    else:
        raise UnsupportedType("Unsupported Spark type: " + dtype)


def convert_schema(func, schema):
    """
    Convert schema in the format of {"col name": "bigint", "col2 name": "int"}
    applying some data types conversion function (e.g. spark2redshift)

    :param func: Conversion Function (e.g. spark2redshift)
    :param schema: Schema (e.g. {"col name": "bigint", "col2 name": "int"})
    :return: Converted schema
    """
    return {name: func(dtype) for name, dtype in schema}


def extract_pyarrow_schema_from_pandas(dataframe,
                                       preserve_index,
                                       indexes_position="right"):
    """
    Extract the related Pyarrow schema from any Pandas DataFrame

    :param dataframe: Pandas Dataframe
    :param preserve_index: True or False
    :param indexes_position: "right" or "left"
    :return: Pyarrow schema (e.g. {"col name": "bigint", "col2 name": "int"})
    """
    cols = []
    cols_dtypes = {}
    if indexes_position not in ["right", "left"]:
        raise ValueError(f"indexes_position must be \"right\" or \"left\"")

    # Handle exception data types (e.g. Int64)
    for name, dtype in dataframe.dtypes.to_dict().items():
        dtype = str(dtype)
        if dtype == "Int64":
            cols_dtypes[name] = "int64"
        else:
            cols.append(name)

    # Filling cols_dtypes and indexes
    indexes = []
    for field in pyarrow.Schema.from_pandas(df=dataframe[cols],
                                            preserve_index=preserve_index):
        name = str(field.name)
        dtype = field.type
        cols_dtypes[name] = dtype
        if name not in dataframe.columns:
            indexes.append(name)

    # Filling schema
    if indexes_position == "right":
        schema = [(name, cols_dtypes[name])
                  for name in dataframe.columns]  # adding columns
        schema += [(name, cols_dtypes[name])
                   for name in indexes]  # adding indexes
    elif indexes_position == "left":
        schema = [(name, cols_dtypes[name])
                  for name in indexes]  # adding indexes
        schema += [(name, cols_dtypes[name])
                   for name in dataframe.columns]  # adding columns
    else:
        raise ValueError(f"indexes_position must be \"right\" or \"left\"")

    logger.debug(f"schema: {schema}")
    return schema
