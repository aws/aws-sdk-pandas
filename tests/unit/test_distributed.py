import importlib.util
import logging
from enum import Enum

import pytest

import awswrangler as wr
from awswrangler._distributed import EngineEnum, MemoryFormatEnum
from awswrangler.s3._write_parquet import _to_parquet

logging.getLogger("awswrangler").setLevel(logging.DEBUG)

pytestmark = pytest.mark.distributed


@pytest.mark.parametrize(
    "engine_enum",
    [
        pytest.param(
            EngineEnum.RAY,
            marks=pytest.mark.skip("ray not available") if not importlib.util.find_spec("ray") else [],
        ),
    ],
)
def test_engine(engine_enum: Enum) -> None:
    assert wr.engine.get_installed() == engine_enum
    assert wr.engine.get() == engine_enum
    assert wr.engine._registry
    assert wr.engine.dispatch_func(_to_parquet).__name__.endswith("distributed")
    assert not wr.engine.dispatch_func(_to_parquet, "python").__name__.endswith("distributed")

    wr.engine.register("python")
    assert wr.engine.get_installed() == engine_enum
    assert wr.engine.get() == EngineEnum.PYTHON
    assert not wr.engine._registry
    assert not wr.engine.dispatch_func(_to_parquet).__name__.endswith("distributed")


@pytest.mark.parametrize(
    "memory_format_enum",
    [
        pytest.param(
            MemoryFormatEnum.MODIN,
            marks=pytest.mark.skip("modin not available") if not importlib.util.find_spec("modin") else [],
        ),
    ],
)
def test_memory_format(memory_format_enum: Enum) -> None:
    assert wr.memory_format.get_installed() == memory_format_enum
    assert wr.memory_format.get() == memory_format_enum

    wr.memory_format.set("pandas")
    assert wr.memory_format.get_installed() == memory_format_enum
    assert wr.memory_format.get() == MemoryFormatEnum.PANDAS
