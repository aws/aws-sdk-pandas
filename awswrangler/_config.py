"""Configuration file for awswrangler."""

import inspect
import logging
import os
from typing import Any, Dict, NamedTuple, Optional, Type

from awswrangler import exceptions

_logger: logging.Logger = logging.getLogger(__name__)


class _ConfigArg(NamedTuple):
    dtype: Type
    nullable: bool = True
    has_default: bool = False
    default: Any = None


_CONFIG_DEFAULTS: Dict[str, _ConfigArg] = {"database": _ConfigArg(dtype=str, nullable=False, has_default=False)}


class _Config:
    """Wrangler's Configuration class."""

    __slots__ = (f"_{attr}" for attr in _CONFIG_DEFAULTS)

    def __init__(self):
        for name, conf in _CONFIG_DEFAULTS.items():
            self._load_config(name=name, conf=conf)

    def _load_config(self, name, conf):
        env_var_name: str = f"WR_{name.upper()}"
        env_var: Optional[str] = os.getenv(env_var_name)
        if env_var is not None:
            value = self._apply_type(
                name=name, value=env_var, dtype=_CONFIG_DEFAULTS[name].dtype, nullable=_CONFIG_DEFAULTS[name].nullable
            )
            self.__setattr__(name, value)
        elif conf.has_default is True:
            self.__setattr__(name, conf.default)

    def __getattr__(self, item: str) -> Any:
        if item in _CONFIG_DEFAULTS:
            return super().__getattribute__(f"_{item}")
        raise AttributeError

    def __getitem__(self, item: str) -> Any:
        return self.__getattr__(item)

    def __setattr__(self, key: str, value: Any) -> Any:
        if key not in _CONFIG_DEFAULTS:
            raise exceptions.InvalidArgumentValue(
                f"{key} is not a valid configuration. " f"Please use: {list(_CONFIG_DEFAULTS.keys())}"
            )
        value = self._apply_type(
            name=key, value=value, dtype=_CONFIG_DEFAULTS[key].dtype, nullable=_CONFIG_DEFAULTS[key].nullable
        )
        super().__setattr__(f"_{key}", value)

    def reset(self, item: Optional[str] = None) -> None:
        """Reset one or all (if None is received) configuration values.

        Parameters
        ----------
        item : str, optional
            Configuration item name.

        Returns
        -------
        None
            None.

        Examples
        --------
        >>> import awswrangler as wr
        >>> wr.config.reset("database")  # Reset one specific configuration
        >>> wr.config.reset()  # Reset all

        """
        if item is None:
            for name, conf in _CONFIG_DEFAULTS.items():
                delattr(self, f"_{name}")
                self._load_config(name=name, conf=conf)
        else:
            delattr(self, f"_{item}")
            self._load_config(name=item, conf=_CONFIG_DEFAULTS[item])

    @staticmethod
    def _apply_type(name: str, value: Any, dtype: Type, nullable: bool) -> Any:
        if _Config._is_null(value=value) is True:
            if nullable is True:
                return None
            exceptions.InvalidArgumentValue(
                f"{name} configuration does not accept null value." f" Please pass {dtype}."
            )
        if isinstance(value, dtype) is False:
            value = dtype(value)
        return value

    @staticmethod
    def _is_null(value: Any) -> bool:
        if value is None:
            return True
        if (isinstance(value, str) is True) and (value.lower() in ("none", "null", "nil")):
            return True
        return False


def apply_configs(function):
    """Decorate some function with configs."""
    signature = inspect.signature(function)
    args_names = list(signature.parameters.keys())
    valid_configs = [x for x in _CONFIG_DEFAULTS if x in args_names]

    def wrapper(*args, **kwargs):
        received_args = signature.bind_partial(*args, **kwargs).arguments
        available_configs = [x for x in valid_configs if (x not in received_args) and (hasattr(config, x) is True)]
        missing_args = {x: config[x] for x in available_configs}
        final_args = {**received_args, **missing_args}
        return function(**final_args)

    wrapper.__doc__ = function.__doc__
    wrapper.__name__ = function.__name__
    wrapper.__signature__ = signature
    return wrapper


config: _Config = _Config()
