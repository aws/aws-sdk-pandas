import logging
from enum import Enum

import pytest

import awswrangler as wr
from awswrangler._distributed import EngineEnum
from awswrangler.s3._write_parquet import _to_parquet

from .._utils import is_ray_modin

logging.getLogger("awswrangler").setLevel(logging.DEBUG)

pytestmark = pytest.mark.distributed


def test_engine_lazy_initialization(path: str) -> None:
    assert not wr.engine._registry

    # any function which dispatches based on engine will
    # initialize the engine
    wr.s3.wait_objects_not_exist([path])

    assert wr.engine._registry


def test_engine_explicit_eager_initialization(path: str) -> None:
    wr.engine.register(EngineEnum.RAY.value)

    assert wr.engine._registry


@pytest.mark.parametrize(
    "engine_enum",
    [
        pytest.param(
            EngineEnum.RAY,
            marks=pytest.mark.skip("ray not available") if not is_ray_modin else [],
        ),
    ],
)
def test_engine(engine_enum: Enum) -> None:
    assert wr.engine.get_installed() == engine_enum
    assert wr.engine.get() == engine_enum

    wr.engine.register(engine_enum.value)

    assert wr.engine._registry
    assert wr.engine.dispatch_func(_to_parquet).__name__.endswith("distributed")
    assert not wr.engine.dispatch_func(_to_parquet, "python").__name__.endswith("distributed")
