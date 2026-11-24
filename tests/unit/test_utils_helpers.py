"""Offline edge-case tests for small helpers in ``awswrangler._utils``."""

import os
import types

import pytest

from awswrangler import _utils
from awswrangler._utils import ensure_cpu_count, get_even_chunks_sizes, import_optional_dependency


@pytest.mark.parametrize(
    "total_size,chunk_size,upper_bound,expected",
    [
        (5, 10, False, (5,)),
        (1, 10, False, (1,)),
        (0, 10, False, (0,)),
        (0, 10, True, (0,)),
        (5, 10, True, (5,)),
    ],
)
def test_get_even_chunks_sizes_smaller_than_one_chunk(total_size, chunk_size, upper_bound, expected):
    assert get_even_chunks_sizes(total_size, chunk_size, upper_bound) == expected


def test_ensure_cpu_count_defaults_to_threads_enabled(monkeypatch):
    monkeypatch.setattr(os, "cpu_count", lambda: 8)
    assert ensure_cpu_count() == 8
    assert ensure_cpu_count(use_threads=True) == 8
    assert ensure_cpu_count(use_threads=False) == 1


@pytest.mark.parametrize("cpu_count,expected", [(None, 1), (0, 1), (1, 1), (2, 2)])
def test_ensure_cpu_count_never_returns_less_than_one(monkeypatch, cpu_count, expected):
    monkeypatch.setattr(os, "cpu_count", lambda: cpu_count)
    assert ensure_cpu_count(use_threads=True) == expected


def test_import_optional_dependency_returns_module():
    module = import_optional_dependency("json")
    assert isinstance(module, types.ModuleType)
    assert module.__name__ == "json"


def test_import_optional_dependency_returns_none_when_missing():
    assert import_optional_dependency("this_module_does_not_exist_for_awswrangler") is None


def test_check_optional_dependency_blocks_calls_when_module_is_missing():
    @_utils.check_optional_dependency(None, "pyiceberg")
    def f() -> str:
        return "ok"

    with pytest.raises(ModuleNotFoundError, match=r"Missing optional dependency 'pyiceberg'"):
        f()


def test_check_optional_dependency_passes_through_when_module_is_present():
    @_utils.check_optional_dependency(types, "types")
    def f(a: int, b: int = 0) -> int:
        """Docstring."""
        return a + b

    assert f(1, b=2) == 3
    assert f.__name__ == "f"
    assert f.__doc__ == "Docstring."
