import importlib.util
import logging
from enum import Enum

import pytest

import awswrangler as wr
from awswrangler._distributed import EngineEnum
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
