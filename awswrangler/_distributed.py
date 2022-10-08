"""Distributed engine and memory format configuration."""

# pylint: disable=import-outside-toplevel

import importlib.util
from enum import Enum
from typing import Optional


class EngineEnum(Enum):
    """Execution engine enum."""

    RAY = "ray"
    PYTHON = "python"


class MemoryFormatEnum(Enum):
    """Memory format enum."""

    MODIN = "modin"
    PANDAS = "pandas"


class Engine:
    """Execution engine configuration class."""

    _name: Optional[str] = None

    @classmethod
    def get_installed(cls) -> str:
        """Get the installed distribution engine.

        This is the engine that can be imported.

        Returns
        -------
        str
            The distribution engine installed.
        """
        if importlib.util.find_spec("ray"):
            return EngineEnum.RAY.value
        return EngineEnum.PYTHON.value

    @classmethod
    def get(cls) -> str:
        """Get the configured distribution engine.

        This is the engine currently configured. If None, the installed engine is returned.

        Returns
        -------
        str
            The distribution engine configured.
        """
        return cls._name if cls._name else cls.get_installed()

    @classmethod
    def set(cls, name: str) -> None:
        """Set the distribution engine."""
        cls._name = name

    @classmethod
    def register(cls, name: Optional[str] = None) -> None:
        """Register the distribution engine dispatch methods."""
        engine_name = cls.get_installed() if not name else name

        if engine_name == EngineEnum.RAY.value:
            from awswrangler.distributed.ray._register import register_ray

            register_ray()

    @classmethod
    def initialize(cls, name: Optional[str] = None) -> None:
        """Initialize the distribution engine."""
        engine_name = cls.get_installed() if not name else name

        if engine_name == EngineEnum.RAY.value:
            cls._name = EngineEnum.RAY.value

            from awswrangler.distributed.ray import initialize_ray

            initialize_ray()
            cls.register(cls._name)
        else:
            cls._name = EngineEnum.PYTHON.value


class MemoryFormat:
    """Memory format configuration class."""

    _name: Optional[str] = None

    @classmethod
    def get_installed(cls) -> str:
        """Get the installed memory format.

        This is the format that can be imported.

        Returns
        -------
        str
            The memory format installed.
        """
        if importlib.util.find_spec("modin"):
            return MemoryFormatEnum.MODIN.value
        return MemoryFormatEnum.PANDAS.value

    @classmethod
    def get(cls) -> str:
        """Get the configured memory format.

        This is the memory format currently configured. If None, the installed memory format is returned.

        Returns
        -------
        str
            The memory format configured.
        """
        return cls._name if cls._name else cls.get_installed()

    @classmethod
    def set(cls, name: str) -> None:
        """Set the memory format."""
        cls._name = name


engine: Engine = Engine()
memory_format: MemoryFormat = MemoryFormat()
