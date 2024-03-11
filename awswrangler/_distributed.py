"""Distributed engine and memory format configuration."""

from __future__ import annotations

import importlib.util
import os
import threading
from collections import defaultdict
from enum import Enum, unique
from functools import wraps
from importlib import reload
from typing import Any, Callable, Literal, TypeVar, cast

EngineLiteral = Literal["python", "ray"]
MemoryFormatLiteral = Literal["pandas", "modin"]

FunctionType = TypeVar("FunctionType", bound=Callable[..., Any])


WR_ENGINE: EngineLiteral | None = os.getenv("WR_ENGINE")  # type: ignore[assignment]
WR_MEMORY_FORMAT: MemoryFormatLiteral | None = os.getenv("WR_MEMORY_FORMAT")  # type: ignore[assignment]


@unique
class EngineEnum(Enum):
    """Execution engine enum."""

    RAY = "ray"
    PYTHON = "python"


@unique
class MemoryFormatEnum(Enum):
    """Memory format enum."""

    MODIN = "modin"
    PANDAS = "pandas"


class Engine:
    """Execution engine configuration class."""

    _engine: EngineEnum | None = EngineEnum[WR_ENGINE.upper()] if WR_ENGINE else None
    _initialized_engine: EngineEnum | None = None
    _registry: dict[EngineLiteral, dict[str, Callable[..., Any]]] = defaultdict(dict)
    _lock: threading.RLock = threading.RLock()

    @classmethod
    def get_installed(cls) -> EngineEnum:
        """Get the installed distribution engine.

        This is the engine that can be imported.

        Returns
        -------
        EngineEnum
            The distribution engine installed.
        """
        if importlib.util.find_spec("ray"):
            return EngineEnum.RAY
        return EngineEnum.PYTHON

    @classmethod
    def get(cls) -> EngineEnum:
        """Get the configured distribution engine.

        This is the engine currently configured. If None, the installed engine is returned.

        Returns
        -------
        str
            The distribution engine configured.
        """
        with cls._lock:
            return cls._engine if cls._engine else cls.get_installed()

    @classmethod
    def set(cls, name: EngineLiteral) -> None:
        """Set the distribution engine."""
        with cls._lock:
            cls._engine = EngineEnum[name.upper()]

    @classmethod
    def dispatch_func(cls, source_func: FunctionType, value: EngineLiteral | None = None) -> FunctionType:
        """Dispatch a func based on value or the distribution engine and the source function."""
        try:
            with cls._lock:
                return cls._registry[value or cls.get().value][source_func.__name__]  # type: ignore[return-value]
        except KeyError:
            return getattr(source_func, "_source_func", source_func)

    @classmethod
    def register_func(cls, source_func: FunctionType, destination_func: FunctionType) -> None:
        """Register a func based on the distribution engine and source function."""
        with cls._lock:
            cls._registry[cls.get().value][source_func.__name__] = destination_func

    @classmethod
    def dispatch_on_engine(cls, func: FunctionType) -> FunctionType:
        """Dispatch on engine function decorator."""

        @wraps(func)
        def wrapper(*args: Any, **kw: dict[str, Any]) -> Any:
            cls.initialize(name=cls.get().value)
            return cls.dispatch_func(func)(*args, **kw)

        # Save the original function
        wrapper._source_func = func  # type: ignore[attr-defined]
        return wrapper  # type: ignore[return-value]

    @classmethod
    def register(cls, name: EngineLiteral | None = None) -> None:
        """Register the distribution engine dispatch methods."""
        with cls._lock:
            engine_name = cast(EngineLiteral, name or cls.get().value)
            cls.set(engine_name)
            cls._registry.clear()

            if engine_name == EngineEnum.RAY.value:
                from awswrangler.distributed.ray._register import register_ray

                register_ray()

    @classmethod
    def initialize(cls, name: EngineLiteral | None = None) -> None:
        """Initialize the distribution engine."""
        with cls._lock:
            engine_name = cast(EngineLiteral, name or cls.get_installed().value)
            if engine_name == EngineEnum.RAY.value:
                from awswrangler.distributed.ray import initialize_ray

                initialize_ray()
            cls._initialized_engine = EngineEnum[engine_name.upper()]

    @classmethod
    def is_initialized(cls, name: EngineLiteral | None = None) -> bool:
        """Check if the distribution engine is initialized."""
        with cls._lock:
            engine_name = cast(EngineLiteral, name or cls.get_installed().value)

            return False if not cls._initialized_engine else cls._initialized_engine.value == engine_name


class MemoryFormat:
    """Memory format configuration class."""

    _enum: MemoryFormatEnum | None = MemoryFormatEnum[WR_MEMORY_FORMAT.upper()] if WR_MEMORY_FORMAT else None
    _lock: threading.RLock = threading.RLock()

    @classmethod
    def get_installed(cls) -> MemoryFormatEnum:
        """Get the installed memory format.

        This is the format that can be imported.

        Returns
        -------
        Enum
            The memory format installed.
        """
        if importlib.util.find_spec("modin"):
            return MemoryFormatEnum.MODIN
        return MemoryFormatEnum.PANDAS

    @classmethod
    def get(cls) -> MemoryFormatEnum:
        """Get the configured memory format.

        This is the memory format currently configured. If None, the installed memory format is returned.

        Returns
        -------
        Enum
            The memory format configured.
        """
        with cls._lock:
            return cls._enum if cls._enum else cls.get_installed()

    @classmethod
    def set(cls, name: EngineLiteral) -> None:
        """Set the memory format."""
        with cls._lock:
            cls._enum = MemoryFormatEnum[name.upper()]

            _reload()


def _reload() -> None:
    """Reload Pandas proxy module."""
    import awswrangler.pandas

    reload(awswrangler.pandas)


engine: Engine = Engine()
memory_format: MemoryFormat = MemoryFormat()
