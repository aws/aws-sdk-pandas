"""Formatting logic for SQL parameters."""

from __future__ import annotations

import datetime
import decimal
import re
from abc import ABC, abstractmethod
from typing import Any, Callable, Sequence

from typing_extensions import Literal

from awswrangler import exceptions

_EngineTypeLiteral = Literal["presto", "hive", "partiql"]


class _Engine(ABC):
    def __init__(self, engine_name: _EngineTypeLiteral) -> None:
        self.engine_name = engine_name

    def format_null(self, value: None = None) -> str:
        return "NULL"

    @abstractmethod
    def format_string(self, value: str) -> str:
        pass

    def format_bool(self, value: bool) -> str:
        return str(value).upper()

    def format_integer(self, value: int) -> str:
        return str(value)

    def format_float(self, value: float) -> str:
        return f"{value:f}"

    def format_decimal(self, value: decimal.Decimal) -> str:
        return f"DECIMAL '{value:f}'"

    def format_timestamp(self, value: datetime.datetime) -> str:
        if value.tzinfo is not None:
            raise TypeError(f"Supports only timezone aware datatype, got {value}.")

        return f"TIMESTAMP '{value.isoformat(sep=' ', timespec='milliseconds')}'"

    def format_date(self, value: datetime.date) -> str:
        return f"DATE '{value.isoformat()}'"

    def format_array(self, value: Sequence[Any]) -> str:
        return f"ARRAY [{', '.join(map(self.format, value))}]"

    def format_dict(self, value: dict[Any, Any]) -> str:
        if not value:
            return "MAP()"

        map_keys = list(value.keys())
        key_type = type(map_keys[0])
        for key in map_keys:
            if key is None:
                raise TypeError("Map key cannot be null.")
            if not isinstance(key, key_type):
                raise TypeError("All Map key elements must be the same type.")

        map_values = list(value.values())
        return (
            f"MAP(ARRAY [{', '.join(map(self.format, map_keys))}], ARRAY [{', '.join(map(self.format, map_values))}])"
        )

    def format(self, data: Any) -> str:
        formats_dict: dict[type[Any], Callable[[Any], str]] = {
            bool: self.format_bool,
            str: self.format_string,
            int: self.format_integer,
            datetime.datetime: self.format_timestamp,
            datetime.date: self.format_date,
            decimal.Decimal: self.format_decimal,
            float: self.format_float,
            list: self.format_array,
            tuple: self.format_array,
            set: self.format_array,
            dict: self.format_dict,
        }

        if data is None:
            return self.format_null()

        for python_type, format_func in formats_dict.items():
            if isinstance(data, python_type):
                return format_func(data)

        raise TypeError(f"Unsupported type {type(data)} in parameter.")


class _PrestoEngine(_Engine):
    def __init__(self) -> None:
        super().__init__("presto")

    def format_string(self, value: str) -> str:
        return f"""'{value.replace("'", "''")}'"""


class _HiveEngine(_Engine):
    def __init__(self) -> None:
        super().__init__("hive")

    def format_string(self, value: str) -> str:
        return "'{}'".format(
            value.replace("\\", "\\\\")
            .replace("'", "\\'")
            .replace("\r", "\\r")
            .replace("\n", "\\n")
            .replace("\t", "\\t")
        )


class _PartiQLEngine(_Engine):
    def __init__(self) -> None:
        super().__init__("partiql")

    def format_null(self, value: None = None) -> str:
        return "null"

    def format_string(self, value: str) -> str:
        return f"""'{value.replace("'", "''")}'"""

    def format_bool(self, value: bool) -> str:
        return "1" if value else "0"

    def format_decimal(self, value: decimal.Decimal) -> str:
        return f"'{value}'"

    def format_timestamp(self, value: datetime.datetime) -> str:
        if value.tzinfo is not None:
            raise TypeError(f"Supports only timezone aware datatype, got {value}.")

        return f"'{value.isoformat()}'"

    def format_date(self, value: datetime.date) -> str:
        return f"'{value.isoformat()}'"

    def format_array(self, value: Sequence[Any]) -> str:
        raise NotImplementedError(f"format_array not implemented for engine={self.engine_name}.")

    def format_dict(self, value: dict[Any, Any]) -> str:
        raise NotImplementedError(f"format_dict not implemented for engine={self.engine_name}.")


def _format_parameters(params: dict[str, Any], engine: _Engine) -> dict[str, Any]:
    processed_params = {}

    for k, v in params.items():
        processed_params[k] = engine.format(data=v)

    return processed_params


_PATTERN = re.compile(r":([A-Za-z0-9_]+)(?![A-Za-z0-9_])")


def _create_engine(engine_type: _EngineTypeLiteral) -> _Engine:
    if engine_type == "hive":
        return _HiveEngine()

    if engine_type == "presto":
        return _PrestoEngine()

    if engine_type == "partiql":
        return _PartiQLEngine()

    raise exceptions.InvalidArgumentValue(f"Unknown engine type: {engine_type}")


def _process_sql_params(sql: str, params: dict[str, Any] | None, engine_type: _EngineTypeLiteral = "presto") -> str:
    if params is None:
        params = {}

    engine = _create_engine(engine_type)
    processed_params = _format_parameters(params, engine=engine)

    def replace(match: re.Match) -> str:  # type: ignore[type-arg]
        key = match.group(1)

        if key not in processed_params:
            # do not replace anything if the parameter is not provided
            return str(match.group(0))

        return str(processed_params[key])

    sql = _PATTERN.sub(replace, sql)

    return sql
