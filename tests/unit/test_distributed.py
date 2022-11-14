import logging

import pytest

import awswrangler as wr
from awswrangler._distributed import EngineEnum
from awswrangler.s3._write_parquet import _to_parquet

from .._utils import is_ray_modin

logging.getLogger("awswrangler").setLevel(logging.DEBUG)

pytestmark = pytest.mark.distributed


@pytest.mark.skipif(condition=not is_ray_modin, reason="ray not available")
def test_engine_lazy_initialization(path: str) -> None:
    assert not wr.engine._registry

    # any function which dispatches based on engine will
    # initialize the engine
    wr.s3.wait_objects_not_exist([path])

    assert wr.engine._registry


@pytest.mark.skipif(condition=not is_ray_modin, reason="ray not available")
def test_engine_explicit_eager_initialization(path: str) -> None:
    wr.engine.initialize()

    assert wr.engine._registry


@pytest.mark.skipif(condition=not is_ray_modin, reason="ray not available")
def test_engine_ray() -> None:
    assert wr.engine.get_installed() == EngineEnum.RAY
    assert wr.engine.get() == EngineEnum.RAY

    wr.engine.initialize(EngineEnum.RAY.value)

    assert wr.engine._registry
    assert wr.engine.dispatch_func(_to_parquet).__name__.endswith("distributed")
    assert not wr.engine.dispatch_func(_to_parquet, "python").__name__.endswith("distributed")


def test_engine_python() -> None:
    wr.engine.initialize(EngineEnum.PYTHON.value)

    assert wr.engine.get() == EngineEnum.PYTHON

    assert not wr.engine._registry
    assert not wr.engine.dispatch_func(_to_parquet).__name__.endswith("distributed")
