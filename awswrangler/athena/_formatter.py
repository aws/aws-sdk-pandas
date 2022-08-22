import datetime
import decimal
from enum import Enum
from typing import Dict, Any, TypeVar, Generic, Sequence, Type


class EngineType(Enum):
    PRESTO = "presto"
    HIVE = "hive"

    def __str__(self) -> str:
        return self.value


_NoneType = type(None)
_PythonType = TypeVar("_PythonType")
_PythonTypeMapValue = TypeVar("_PythonTypeMapValue")


class _AbstractType(Generic[_PythonType]):
    def __init__(self, data: _PythonType, engine: EngineType):
        self.data: _PythonType = data
        self.engine: EngineType = engine

    def __str__(self) -> str:
        raise NotImplementedError(
            f"{type(self)} not implemented for engine={self.engine}."
        )


class NullType(_AbstractType[_NoneType]):
    def __str__(self) -> str:
        return "NULL"


class StringType(_AbstractType[str]):
    supported_formats = {"s", "i"}

    def __str__(self) -> str:
        if self.engine == EngineType.PRESTO:
            return f"""'{self.data.replace("'", "''")}'"""
        elif self.engine == EngineType.HIVE:
            return "'{}'".format(
                self.data
                .replace('\\', '\\\\')
                .replace("'", "\\'")
                .replace('\r', '\\r')
                .replace('\n', '\\n')
                .replace('\t', '\\t')
            )

        return super().__str__()


class BooleanType(_AbstractType[bool]):
    def __str__(self) -> str:
        return str(self.data).upper()


class IntegerType(_AbstractType[int]):
    def __str__(self) -> str:
        return str(self.data)


class FloatType(_AbstractType[float]):
    def __str__(self) -> str:
        return f"{self.data:f}"


class DecimalType(_AbstractType[decimal.Decimal]):
    def __str__(self) -> str:
        return f"DECIMAL '{self.data:f}'"


class TimestampType(_AbstractType[datetime.datetime]):
    def __str__(self) -> str:
        if self.data.tzinfo is not None:
            raise TypeError(f"Supports only timezone aware datatype, got {self.data}.")
        return f"TIMESTAMP '{self.data.isoformat(sep=' ', timespec='milliseconds')}'"


class DateType(_AbstractType[datetime.date]):
    def __str__(self) -> str:
        return f"DATE '{self.data.isoformat()}'"


class ArrayType(_AbstractType[Sequence[_PythonType]]):
    def __str__(self) -> str:
        return f"ARRAY [{', '.join(map(str, self.data))}]"


class MapType(_AbstractType[Dict[_PythonType, _PythonTypeMapValue]]):
    def __str__(self) -> str:
        if not len(self.data):
            return "MAP()"

        map_keys = list(self.data.keys())
        key_type = type(map_keys[0])
        for key in map_keys:
            if isinstance(key, NullType):
                raise TypeError("Map key cannot be null.")
            if not isinstance(key, key_type):
                raise TypeError("All Map key elements must be the same type.")

        map_values = list(self.data.values())
        return f"MAP(ARRAY [{', '.join(map(str, map_keys))}], ARRAY [{', '.join(map(str, map_values))}])"


_FORMATS: Dict[Type[Any], Type[_AbstractType[_PythonType]]] = {  # type: ignore
    bool: BooleanType,
    str: StringType,
    int: IntegerType,
    datetime.datetime: TimestampType,
    datetime.date: DateType,
    decimal.Decimal: DecimalType,
    float: FloatType,
}

_ARRAY_FORMATS: Dict[Type[Any], Type[_AbstractType[_PythonType]]] = {  # type: ignore
    list: ArrayType,
    tuple: ArrayType,
    set: ArrayType,
}

_MAP_FORMATS: Dict[Type[Any], Type[_AbstractType[_PythonType]]] = {  # type: ignore
    dict: MapType,
}


def _create_abstract_type(
    data: _PythonType,
    engine: EngineType,
) -> _AbstractType[_PythonType]:
    if data is None:
        return NullType(data=data, engine=engine)

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
                    _create_abstract_type(mk, engine=engine):
                        _create_abstract_type(mv, engine=engine)
                    for mk, mv
                    in data.items()
                },
                engine=engine,
            )

    raise TypeError(f"Unsupported type {type(data)} in parameter.")


def _format_parameters(params: Dict[str, Any], engine: EngineType) -> Dict[str, Any]:
    processed_params = {}

    for k, v in params.items():
        abs_type = _create_abstract_type(data=v, engine=engine)
        processed_params[k] = str(abs_type)

    return processed_params
