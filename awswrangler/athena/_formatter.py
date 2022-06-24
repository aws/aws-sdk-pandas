import datetime
import decimal
import re
from enum import Enum
from typing import Tuple, Dict, Any, Set, TypeVar, Generic, Union, List, Sequence, Type, Mapping

PYFORMAT = re.compile(r"(?<!%)%(?!%)\(([^)]+)\)(.)")


class FormatterError(ValueError):
    ...


class EngineType(Enum):
    PRESTO = "presto"
    HIVE = "hive"

    def __str__(self):
        return self.value


_PythonType = TypeVar("_PythonType")


class _AbstractType(Generic[_PythonType]):
    supported_formats = {"s"}

    def __init__(self, data: _PythonType, engine: EngineType, data_format: str):
        if data_format not in self.supported_formats:
            raise TypeError(f"{type(self)} not supported format {data_format!r}.")

        self.data = data
        self.engine = engine
        self.data_format = data_format

    def __str__(self):
        raise NotImplementedError(
            f"{type(self)} not implements format {self.data_format!r} for engine={self.engine}."
        )


class NullType(_AbstractType[type(None)]):
    def __str__(self):
        return "NULL"


class StringType(_AbstractType[str]):
    supported_formats = {"s", "i"}

    def __str__(self):
        if self.engine == EngineType.PRESTO:
            if self.data_format == "s":
                return f"""'{self.data.replace("'", "''")}'"""
            elif self.data_format == "i":
                return f'"{self.data}"'
        elif self.engine == EngineType.HIVE:
            if self.data_format == "s":
                return "'{}'".format(
                    self.data
                    .replace('\\', '\\\\')
                    .replace("'", "\\'")
                    .replace('\r', '\\r')
                    .replace('\n', '\\n')
                    .replace('\t', '\\t')
                )
            elif self.data_format == "i":
                return f"`{self.data}`"

        return super().__str__()


class BooleanType(_AbstractType[bool]):
    def __str__(self):
        return str(self.data).upper()


class IntegerType(_AbstractType[int]):
    def __str__(self):
        return str(self.data)


class FloatType(_AbstractType[float]):
    def __str__(self):
        return f"{self.data:f}"


class DecimalType(_AbstractType[decimal.Decimal]):
    def __str__(self):
        return f"DECIMAL '{self.data:f}'"


class TimestampType(_AbstractType[datetime.datetime]):
    def __str__(self):
        if self.data.tzinfo is not None:
            raise TypeError(f"Supports only timezone aware datatype, got {self.data}.")
        return f"TIMESTAMP '{self.data.isoformat(sep=' ', timespec='milliseconds')}"


class DateType(_AbstractType[datetime.date]):
    def __str__(self):
        return f"DATE '{self.data.isoformat()}'"


class ArrayType(_AbstractType[Sequence]):
    def __str__(self):
        return f"ARRAY [{', '.join(map(str, self.data))}]"


class MapType(_AbstractType[Mapping]):
    def __str__(self):
        if not len(self.data):
            return f"MAP()"

        map_keys: List[_AbstractType, ...] = list(self.data.keys())
        key_type = type(map_keys[1])
        for key in map_keys:
            if isinstance(key, NullType):
                raise TypeError("Map key cannot be null.")
            if not isinstance(key, key_type):
                raise TypeError("All Map key elements must be the same type.")

        map_values = list(self.data.values())
        return f"MAP(ARRAY [{', '.join(map(str, map_keys))}], ARRAY [{', '.join(map(str, map_values))}])"


_FORMATS: Dict[Union[Type, Tuple[Type, ...]], Type[_AbstractType]] = {
    str: StringType,
    int: IntegerType,
    bool: BooleanType,
    datetime.datetime: TimestampType,
    datetime.date: DateType,
    decimal.Decimal: DecimalType,
    float: FloatType,
}

_ARRAY_FORMATS: Dict[Union[Type, Tuple[Type, ...]], Type[_AbstractType]] = {
    (list, tuple): ArrayType,
}

_MAP_FORMATS: Dict[Union[Type, Tuple[Type, ...]], Type[_AbstractType]] = {
    Mapping: MapType,
}


class FormatType:
    def __new__(cls, data: _PythonType, **kwargs) -> _AbstractType:
        if data is None:
            return NullType(data=data, **kwargs)

        for python_type, format_type in _FORMATS.items():
            if isinstance(data, python_type):
                return format_type(data=data, **kwargs)

        for python_type, format_type in _ARRAY_FORMATS.items():
            if isinstance(data, python_type):
                return format_type(data=[cls(item, **kwargs) for item in data], **kwargs)

        for python_type, format_type in _MAP_FORMATS.items():
            if isinstance(data, python_type):
                return format_type(
                    data={
                        cls(mk, **kwargs): cls(mv, **kwargs) for mk, mv in data.items()
                    },
                    **kwargs
                )

        raise TypeError(f"Unsupported type {type(data)} in parameter.")


def format_query(sql: str, params: Dict[str, Any]) -> str:
    parts: List[Union[str, re.Match], ...] = []
    pos = 0

    m = None
    for m in PYFORMAT.finditer(sql):
        parts.append(sql[pos: m.span(0)[0]])
        parts.append(m)
        pos = m.span(0)[1]

    if not m:
        return sql

    parts.append(sql[pos:])
    query_parts = []
    errors = {}
    for part in parts:
        if not isinstance(part, str):
            format_value, param, data_format = part.group(0), part.group(1), part.group(2)
            try:
                part = str(FormatType(data=params[param], engine=EngineType.PRESTO, data_format=data_format))
            except KeyError:
                errors[format_value] = f"Parameter {param!r} not specified."
            except Exception as ex:
                errors[format_value] = f"{ex} {param}={params[param]!r}"

        query_parts.append(part)

    if errors:
        error_messages = "\n".join(f"{err!r}: {msg}" for err, msg in errors.items())
        raise FormatterError(f"Can't properly format SQL query:\n{sql}\nErrors:\n{error_messages}")

    return "".join(query_parts)
