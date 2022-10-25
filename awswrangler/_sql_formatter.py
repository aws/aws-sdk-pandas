"""Formatting logic for SQL parameters."""
import datetime
import decimal
import re
from enum import Enum
from typing import Any, Dict, Generic, Optional, Sequence, Type, TypeVar


class _EngineType(Enum):
    PRESTO = "presto"
    HIVE = "hive"
    PARTIQL = "partiql"

    def __str__(self) -> str:
        return self.value


_NoneType = type(None)
_PythonType = TypeVar("_PythonType")
_PythonTypeMapValue = TypeVar("_PythonTypeMapValue")


class _AbstractType(Generic[_PythonType]):
    def __init__(self, data: _PythonType, engine: _EngineType):
        self.data: _PythonType = data
        self.engine: _EngineType = engine

    def __str__(self) -> str:
        raise NotImplementedError(f"{type(self)} not implemented for engine={self.engine}.")


class _NullType(_AbstractType[_NoneType]):
    def __str__(self) -> str:
        if self.engine == _EngineType.PARTIQL:
            return "null"

        return "NULL"


class _StringType(_AbstractType[str]):
    supported_formats = {"s", "i"}

    def __str__(self) -> str:
        if self.engine in [_EngineType.PRESTO, _EngineType.PARTIQL]:
            return f"""'{self.data.replace("'", "''")}'"""

        if self.engine == _EngineType.HIVE:
            return "'{}'".format(
                self.data.replace("\\", "\\\\")
                .replace("'", "\\'")
                .replace("\r", "\\r")
                .replace("\n", "\\n")
                .replace("\t", "\\t")
            )

        return super().__str__()


class _BooleanType(_AbstractType[bool]):
    def __str__(self) -> str:
        if self.engine == _EngineType.PARTIQL:
            return "1" if self.data else "0"

        return str(self.data).upper()


class _IntegerType(_AbstractType[int]):
    def __str__(self) -> str:
        return str(self.data)


class _FloatType(_AbstractType[float]):
    def __str__(self) -> str:
        return f"{self.data:f}"


class _DecimalType(_AbstractType[decimal.Decimal]):
    def __str__(self) -> str:
        if self.engine == _EngineType.PARTIQL:
            return f"'{self.data}'"

        return f"DECIMAL '{self.data:f}'"


class _TimestampType(_AbstractType[datetime.datetime]):
    def __str__(self) -> str:
        if self.data.tzinfo is not None:
            raise TypeError(f"Supports only timezone aware datatype, got {self.data}.")

        if self.engine == _EngineType.PARTIQL:
            return f"'{self.data.isoformat()}'"

        return f"TIMESTAMP '{self.data.isoformat(sep=' ', timespec='milliseconds')}'"


class _DateType(_AbstractType[datetime.date]):
    def __str__(self) -> str:
        if self.engine == _EngineType.PARTIQL:
            return f"'{self.data.isoformat()}'"

        return f"DATE '{self.data.isoformat()}'"


class _ArrayType(_AbstractType[Sequence[_PythonType]]):
    def __str__(self) -> str:
        if self.engine == _EngineType.PARTIQL:
            super().__str__()

        return f"ARRAY [{', '.join(map(str, self.data))}]"


class _MapType(_AbstractType[Dict[_PythonType, _PythonTypeMapValue]]):
    def __str__(self) -> str:
        if self.engine == _EngineType.PARTIQL:
            super().__str__()

        if not self.data:
            return "MAP()"

        map_keys = list(self.data.keys())
        key_type = type(map_keys[0])
        for key in map_keys:
            if isinstance(key, _NullType):
                raise TypeError("Map key cannot be null.")
            if not isinstance(key, key_type):
                raise TypeError("All Map key elements must be the same type.")

        map_values = list(self.data.values())
        return f"MAP(ARRAY [{', '.join(map(str, map_keys))}], ARRAY [{', '.join(map(str, map_values))}])"


_FORMATS: Dict[Type[Any], Type[_AbstractType[_PythonType]]] = {  # type: ignore
    bool: _BooleanType,
    str: _StringType,
    int: _IntegerType,
    datetime.datetime: _TimestampType,
    datetime.date: _DateType,
    decimal.Decimal: _DecimalType,
    float: _FloatType,
}

_ARRAY_FORMATS: Dict[Type[Any], Type[_AbstractType[_PythonType]]] = {  # type: ignore
    list: _ArrayType,
    tuple: _ArrayType,
    set: _ArrayType,
}

_MAP_FORMATS: Dict[Type[Any], Type[_AbstractType[_PythonType]]] = {  # type: ignore
    dict: _MapType,
}


def _create_abstract_type(
    data: _PythonType,
    engine: _EngineType,
) -> _AbstractType[_PythonType]:
    if data is None:
        return _NullType(data=data, engine=engine)

    for python_type, format_type in _FORMATS.items():
        if isinstance(data, python_type):
            return format_type(data=data, engine=engine)

    for python_type, format_type in _ARRAY_FORMATS.items():
        if isinstance(data, python_type):
            return format_type(
                [_create_abstract_type(item, engine=engine) for item in data],
                engine=engine,
            )

    for python_type, format_type in _MAP_FORMATS.items():
        if isinstance(data, python_type):
            return format_type(
                data={
                    _create_abstract_type(mk, engine=engine): _create_abstract_type(mv, engine=engine)
                    for mk, mv in data.items()
                },
                engine=engine,
            )

    raise TypeError(f"Unsupported type {type(data)} in parameter.")


def _format_parameters(params: Dict[str, Any], engine: _EngineType) -> Dict[str, Any]:
    processed_params = {}

    for k, v in params.items():
        abs_type = _create_abstract_type(data=v, engine=engine)
        processed_params[k] = str(abs_type)

    return processed_params


_PATTERN = re.compile(r":([A-Za-z0-9_]+)(?![A-Za-z0-9_])")


def _process_sql_params(sql: str, params: Optional[Dict[str, Any]], engine: _EngineType = _EngineType.PRESTO) -> str:
    if params is None:
        params = {}

    processed_params = _format_parameters(params, engine=engine)

    def replace(match: re.Match) -> str:  # type: ignore
        key = match.group(1)

        if key not in processed_params:
            # do not replace anything if the parameter is not provided
            return str(match.group(0))

        return str(processed_params[key])

    sql = _PATTERN.sub(replace, sql)

    return sql
