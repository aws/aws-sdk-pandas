"""Distributed engine and memory format configuration."""

# pylint: disable=import-outside-toplevel

import importlib.util
from collections import defaultdict
from enum import Enum, unique
from functools import wraps
from typing import Any, Callable, Dict, Optional


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

    _enum: Optional[EngineEnum] = None
    _initialized_engine: Optional[EngineEnum] = None
    _registry: Dict[str, Dict[str, Callable[..., Any]]] = defaultdict(dict)

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
        return cls._enum if cls._enum else cls.get_installed()

    @classmethod
    def set(cls, name: str) -> None:
        """Set the distribution engine."""
        cls._enum = EngineEnum._member_map_[name.upper()]  # type: ignore  # pylint: disable=protected-access,no-member

    @classmethod
    def dispatch_func(cls, source_func: Callable[..., Any], value: Optional[Any] = None) -> Callable[..., Any]:
        """Dispatch a func based on value or the distribution engine and the source function."""
        if not cls.is_initialized(cls.get().value):
            cls.initialize(cls.get().value)

        try:
            return cls._registry[value or cls.get().value][source_func.__name__]
        except KeyError:
            return getattr(source_func, "_source_func", source_func)

    @classmethod
    def register_func(cls, source_func: Callable[..., Any], destination_func: Callable[..., Any]) -> Callable[..., Any]:
        """Register a func based on the distribution engine and source function."""
        cls._registry[cls.get().value][source_func.__name__] = destination_func
        return destination_func

    @classmethod
    def dispatch_on_engine(cls, func: Callable[..., Any]) -> Callable[..., Any]:
        """Dispatch on engine function decorator."""

        @wraps(func)
        def wrapper(*args: Any, **kw: Dict[str, Any]) -> Any:
            if not cls.is_initialized(cls.get().value):
                cls.initialize(cls.get().value)

            return cls.dispatch_func(func)(*args, **kw)

        # Save the original function
        wrapper._source_func = func  # type: ignore  # pylint: disable=protected-access
        return wrapper

    @classmethod
    def register(cls, name: Optional[str] = None) -> None:
        """Register the distribution engine dispatch methods."""
        engine_name = cls.get_installed().value if not name else name
        cls.set(engine_name)
        cls._registry.clear()

        if engine_name == EngineEnum.RAY.value:
            from awswrangler.distributed.ray._register import register_ray

            register_ray()

    @classmethod
    def initialize(cls, name: Optional[str] = None) -> None:
        """Initialize the distribution engine."""
        engine_name = cls.get_installed().value if not name else name
        if engine_name == EngineEnum.RAY.value:
            from awswrangler.distributed.ray import initialize_ray

            initialize_ray()
        cls.register(engine_name)

    @classmethod
    def is_initialized(cls, name: Optional[str] = None) -> bool:
        """Check if the distribution engine is initialized."""
        engine_name = cls.get_installed().value if not name else name
        if cls._initialized_engine is None:
            return False

        return cls._initialized_engine.value == engine_name


class MemoryFormat:
    """Memory format configuration class."""

    _enum: Optional[Enum] = None

    @classmethod
    def get_installed(cls) -> Enum:
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
    def get(cls) -> Enum:
        """Get the configured memory format.

        This is the memory format currently configured. If None, the installed memory format is returned.

        Returns
        -------
        Enum
            The memory format configured.
        """
        return cls._enum if cls._enum else cls.get_installed()

    @classmethod
    def set(cls, name: str) -> None:
        """Set the memory format."""
        cls._enum = MemoryFormatEnum._member_map_[name.upper()]  # pylint: disable=protected-access,no-member


engine: Engine = Engine()
memory_format: MemoryFormat = MemoryFormat()
