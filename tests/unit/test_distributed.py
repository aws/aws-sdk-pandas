import logging
from importlib import reload
from types import ModuleType
from typing import Iterator

import pytest

from .._utils import is_ray_modin

logging.getLogger("awswrangler").setLevel(logging.DEBUG)

pytestmark = pytest.mark.distributed


@pytest.fixture(scope="function")
def wr() -> Iterator[ModuleType]:
    import awswrangler

    awswrangler.engine.__class__._engine = None
    awswrangler.engine.__class__._initialized_engine = None
    awswrangler.engine.__class__._registry.clear()

    yield reload(awswrangler)

    # Reset for future tests
    awswrangler.engine.initialize()


@pytest.mark.skipif(condition=not is_ray_modin, reason="ray not available")
def test_engine_initialization(wr: ModuleType, path: str) -> None:
    assert wr.engine.is_initialized()


@pytest.mark.skipif(condition=not is_ray_modin, reason="ray not available")
def test_engine_python(wr: ModuleType) -> None:
    from awswrangler._distributed import EngineEnum
    from awswrangler.s3._write_parquet import _to_parquet

    assert wr.engine.get_installed() == EngineEnum.RAY
    assert wr.engine.get() == EngineEnum.RAY

    wr.engine.initialize(EngineEnum.PYTHON.value)

    assert wr.engine.get() == EngineEnum.PYTHON

    assert not wr.engine.dispatch_func(_to_parquet).__name__.endswith("distributed")


@pytest.mark.skipif(condition=not is_ray_modin, reason="ray not available")
def test_engine_ray(wr: ModuleType) -> None:
    from awswrangler._distributed import EngineEnum
    from awswrangler.s3._write_parquet import _to_parquet

    assert wr.engine.get_installed() == EngineEnum.RAY
    assert wr.engine.get() == EngineEnum.RAY

    assert wr.engine._registry
    assert wr.engine.dispatch_func(_to_parquet).__name__.endswith("distributed")
    assert not wr.engine.dispatch_func(_to_parquet, "python").__name__.endswith("distributed")


@pytest.mark.skipif(condition=is_ray_modin, reason="ray is installed")
def test_engine_python_without_ray_installed(wr: ModuleType) -> None:
    from awswrangler._distributed import EngineEnum
    from awswrangler.s3._write_parquet import _to_parquet

    assert wr.engine.get_installed() == EngineEnum.PYTHON
    assert wr.engine.get() == EngineEnum.PYTHON

    assert not wr.engine.dispatch_func(_to_parquet).__name__.endswith("distributed")
