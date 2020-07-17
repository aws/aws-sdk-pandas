"""Configuration file for AWS Data Wrangler."""

import inspect
import logging
import os
from typing import Any, Callable, Dict, List, NamedTuple, Optional, Tuple, Type, Union, cast

import pandas as pd  # type: ignore

from awswrangler import _utils, exceptions

_logger: logging.Logger = logging.getLogger(__name__)


_ConfigValueType = Union[str, bool, int, None]


class _ConfigArg(NamedTuple):
    dtype: Type
    nullable: bool


_CONFIG_ARGS: Dict[str, _ConfigArg] = {
    "database": _ConfigArg(dtype=str, nullable=True),
    "ctas_approach": _ConfigArg(dtype=bool, nullable=False),
    "max_cache_seconds": _ConfigArg(dtype=int, nullable=False),
    "max_cache_query_inspections": _ConfigArg(dtype=int, nullable=False),
}


class _Config:
    """Wrangler's Configuration class."""

    __slots__ = tuple(attr for attr in _CONFIG_ARGS) + ("_loaded_values",)

    def __init__(self):
        self._loaded_values: Dict[str, _ConfigValueType] = {}
        name: str
        for name in _CONFIG_ARGS:
            self._load_config(name=name)

    @staticmethod
    def _apply_type(name: str, value: Any, dtype: Type, nullable: bool) -> _ConfigValueType:
        if _Config._is_null(value=value):
            if nullable is True:
                return None
            exceptions.InvalidArgumentValue(f"{name} configuration does not accept a null value. Please pass {dtype}.")
        try:
            return dtype(value) if isinstance(value, dtype) is False else value
        except ValueError:
            raise exceptions.InvalidConfiguration(f"Config {name} must receive a {dtype} value.")

    @staticmethod
    def _is_null(value: _ConfigValueType) -> bool:
        if value is None:
            return True
        if isinstance(value, str) is True:
            value = cast(str, value)
            if value.lower() in ("none", "null", "nil"):
                return True
        return False

    def _load_config(self, name: str) -> bool:
        env_var: Optional[str] = os.getenv(f"WR_{name.upper()}")
        if env_var is not None:
            self.__setattr__(name, env_var)
            return True
        return False

    def __setattr__(self, key: str, value: Any) -> None:
        if key == "_loaded_values":
            super().__setattr__(key, value)
        else:
            if key not in _CONFIG_ARGS:
                raise exceptions.InvalidArgumentValue(
                    f"{key} is not a valid configuration. Please use: {list(_CONFIG_ARGS.keys())}"
                )
            value_casted: _ConfigValueType = self._apply_type(
                name=key, value=value, dtype=_CONFIG_ARGS[key].dtype, nullable=_CONFIG_ARGS[key].nullable
            )
            super().__setattr__(key, value_casted)
            self._loaded_values[key] = value_casted

    def __getitem__(self, item: str) -> _ConfigValueType:
        return getattr(self, item)

    def _reset_item(self, item: str) -> None:
        if hasattr(self, item):
            delattr(self, item)
            del self._loaded_values[item]
        self._load_config(name=item)

    def to_pandas(self) -> pd.DataFrame:
        """Load all configurations on a Pandas DataFrame.

        Returns
        -------
        pd.DataFrame
            Configuration DataFrame.

        Examples
        --------
        >>> import awswrangler as wr
        >>> wr.config.to_pandas()

        """
        args: List[Dict[str, Any]] = []
        for k, v in _CONFIG_ARGS.items():
            arg: Dict[str, Any] = {
                "name": k,
                "Env. Variable": f"WR_{k.upper()}",
                "type": v.dtype,
                "nullable": v.nullable,
            }
            if k in self._loaded_values:
                arg["configured"] = True
                arg["value"] = self._loaded_values[k]
            else:
                arg["configured"] = False
                arg["value"] = None
            args.append(arg)
        return pd.DataFrame(args)

    def _repr_html_(self):
        return self.to_pandas().to_html()

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
            for name in _CONFIG_ARGS:
                self._reset_item(item=name)
        else:
            self._reset_item(item=item)


def _inject_config_doc(doc: str, available_configs: Tuple[str, ...]) -> str:
    header: str = (
        "\n    Note\n    ----"
        "\n    This functions has arguments that can has default values configured globally through "
        "*wr.config* or environment variables:\n\n"
    )
    args: Tuple[str, ...] = tuple(f"    - {x}\n" for x in available_configs)
    args_block: str = "\n".join(args)
    footer: str = (
        "\n    Check out the `Global Configurations Tutorial "
        "<https://github.com/awslabs/aws-data-wrangler/blob/master/tutorials/"
        "021%20-%20Global%20Configurations.ipynb>`_"
        " for details.\n"
    )
    insertion: str = header + args_block + footer + "\n\n"
    return _utils.insert_str(text=doc, token="\n    Parameters", insert=insertion)


def apply_configs(function) -> Callable:
    """Decorate some function with configs."""
    signature = inspect.signature(function)
    args_names: Tuple[str, ...] = tuple(signature.parameters.keys())
    available_configs: Tuple[str, ...] = tuple(x for x in _CONFIG_ARGS if x in args_names)

    def wrapper(*args, **kwargs):
        received_args: Dict[str, Any] = signature.bind_partial(*args, **kwargs).arguments
        activated_configs = [x for x in available_configs if (x not in received_args) and (hasattr(config, x) is True)]
        missing_args: Dict[str, _ConfigValueType] = {x: config[x] for x in activated_configs}
        final_args: Dict[str, Any] = {**received_args, **missing_args}
        return function(**final_args)

    wrapper.__doc__ = _inject_config_doc(doc=function.__doc__, available_configs=available_configs)
    wrapper.__name__ = function.__name__
    wrapper.__setattr__("__signature__", signature)  # pylint: disable=no-member
    return wrapper


config: _Config = _Config()
