import datetime as dt
import decimal
from dataclasses import dataclass

import pytest

from awswrangler._sql_formatter import (
    _Engine,
    _format_parameters,
    _HiveEngine,
    _PrestoEngine,
    _process_sql_params,
)

_hive_engine_param = pytest.param(_HiveEngine(), id="hive")
_presto_engine_param = pytest.param(_PrestoEngine(), id="presto")


@pytest.mark.parametrize("engine", [_hive_engine_param, _presto_engine_param])
def test_parameter_formatting(engine: _Engine) -> None:
    actual_params = _format_parameters(
        {
            "string": "hello",
            "int": 12,
            "float": 13.0,
            "null": None,
            "datetime": dt.datetime(2022, 8, 22, 13, 2, 36, 123000),
            "date": dt.date(2022, 8, 22),
            "boolean": True,
            "decimal": decimal.Decimal("12.03"),
            "list": [decimal.Decimal("33.33"), 1, None, False],
            "tuple": (decimal.Decimal("33.33"), 1, None, False),
            "map": {"int": 4, "date": dt.date(2022, 8, 22)},
        },
        engine=engine,
    )

    expected_params = {
        "string": "'hello'",
        "int": "12",
        "float": "13.000000",
        "null": "NULL",
        "datetime": "TIMESTAMP '2022-08-22 13:02:36.123'",
        "date": "DATE '2022-08-22'",
        "boolean": "TRUE",
        "decimal": "DECIMAL '12.03'",
        "list": "ARRAY [DECIMAL '33.33', 1, NULL, FALSE]",
        "tuple": "ARRAY [DECIMAL '33.33', 1, NULL, FALSE]",
        "map": "MAP(ARRAY ['int', 'date'], ARRAY [4, DATE '2022-08-22'])",
    }

    assert actual_params == expected_params


@pytest.mark.parametrize("engine", [_hive_engine_param, _presto_engine_param])
def test_set_formatting(engine: _Engine) -> None:
    actual_params = _format_parameters(
        {"set": {decimal.Decimal("33.33"), 1, None, False}},
        engine=engine,
    )

    assert len(actual_params) == 1
    assert "set" in actual_params

    assert "DECIMAL '33.33'" in actual_params["set"]
    assert "1" in actual_params["set"]
    assert "NULL" in actual_params["set"]
    assert "FALSE" in actual_params["set"]


def test_escaped_string_formatting_for_presto() -> None:
    actual_params = _format_parameters(
        {"string": "Driver's License"},
        engine=_PrestoEngine(),
    )

    expected_params = {
        "string": "'Driver''s License'",
    }

    assert actual_params == expected_params


def test_escaped_string_formatting_for_hive() -> None:
    actual_params = _format_parameters(
        {"string": "Driver's License"},
        engine=_HiveEngine(),
    )

    expected_params = {
        "string": r"'Driver\'s License'",
    }

    assert actual_params == expected_params


@pytest.mark.parametrize("engine", [_hive_engine_param, _presto_engine_param])
def test_map_key_cannot_be_null(engine: _Engine) -> None:
    with pytest.raises(TypeError, match=r".*Map key cannot be null.*"):
        _format_parameters(
            {"map": {None: 4}},
            engine=engine,
        )


@pytest.mark.parametrize("engine", [_hive_engine_param, _presto_engine_param])
def test_map_keys_cannot_have_different_types(engine: _Engine) -> None:
    with pytest.raises(TypeError, match=r".*All Map key elements must be the same type\..*"):
        _format_parameters(
            {"map": {"hello": 3, 77: 10}},
            engine=engine,
        )


@pytest.mark.parametrize("engine", [_hive_engine_param, _presto_engine_param])
def test_invalid_parameter_type(engine: _Engine) -> None:
    @dataclass
    class Point:
        x: int
        y: int

    with pytest.raises(TypeError, match=r".*Unsupported type.*Point.*"):
        _format_parameters(
            {"point": Point(7, 1)},
            engine=engine,
        )


def test_process_sql_params_double_colon_cast() -> None:
    sql = "SELECT col::text, col::timestamp FROM table WHERE id = :id AND status = :text"
    params = {"id": 1, "text": "active"}
    processed_sql = _process_sql_params(sql, params)
    expected_sql = "SELECT col::text, col::timestamp FROM table WHERE id = 1 AND status = 'active'"
    assert processed_sql == expected_sql


@pytest.mark.parametrize("engine", [_hive_engine_param, _presto_engine_param])
def test_numpy_parameter_formatting(engine: _Engine) -> None:
    import numpy as np

    actual_params = _format_parameters(
        {
            "np_int64": np.int64(42),
            "np_int32": np.int32(10),
            "np_float64": np.float64(3.14),
            "np_float32": np.float32(2.5),
            "np_bool_true": np.bool_(True),
            "np_bool_false": np.bool_(False),
            "np_str": np.str_("hello"),
            "np_list": [np.int64(1), np.int32(2)],
        },
        engine=engine,
    )

    assert actual_params["np_int64"] == "42"
    assert actual_params["np_int32"] == "10"
    assert actual_params["np_float64"] == "3.140000"
    assert actual_params["np_float32"] == "2.500000"
    assert actual_params["np_bool_true"] == "TRUE"
    assert actual_params["np_bool_false"] == "FALSE"
    assert actual_params["np_str"] == "'hello'"
    assert actual_params["np_list"] == "ARRAY [1, 2]"
