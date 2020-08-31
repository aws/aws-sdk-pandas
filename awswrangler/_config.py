"""Configuration file for AWS Data Wrangler."""

import inspect
import logging
import os
from typing import Any, Callable, Dict, List, NamedTuple, Optional, Tuple, Type, Union, cast

import pandas as pd

from awswrangler import _utils, exceptions

_logger: logging.Logger = logging.getLogger(__name__)


_ConfigValueType = Union[str, bool, int, None]


class _ConfigArg(NamedTuple):
    dtype: Type[Union[str, bool, int]]
    nullable: bool
    enforced: bool = False


# Please, also add any new argument as a property in the _Config class
_CONFIG_ARGS: Dict[str, _ConfigArg] = {
    "catalog_id": _ConfigArg(dtype=str, nullable=True),
    "concurrent_partitioning": _ConfigArg(dtype=bool, nullable=False),
    "ctas_approach": _ConfigArg(dtype=bool, nullable=False),
    "database": _ConfigArg(dtype=str, nullable=True),
    "max_cache_query_inspections": _ConfigArg(dtype=int, nullable=False),
    "max_cache_seconds": _ConfigArg(dtype=int, nullable=False),
    "s3_block_size": _ConfigArg(dtype=int, nullable=False, enforced=True),
}


class _Config:
    """Wrangler's Configuration class."""

    def __init__(self) -> None:
        self._loaded_values: Dict[str, _ConfigValueType] = {}
        name: str
        for name in _CONFIG_ARGS:
            self._load_config(name=name)

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
                "enforced": v.enforced,
            }
            if k in self._loaded_values:
                arg["configured"] = True
                arg["value"] = self._loaded_values[k]
            else:
                arg["configured"] = False
                arg["value"] = None
            args.append(arg)
        return pd.DataFrame(args)

    def _load_config(self, name: str) -> bool:
        env_var: Optional[str] = os.getenv(f"WR_{name.upper()}")
        if env_var is not None:
            self._set_config_value(key=name, value=env_var)
            return True
        return False

    def _set_config_value(self, key: str, value: Any) -> None:
        if key not in _CONFIG_ARGS:
            raise exceptions.InvalidArgumentValue(
                f"{key} is not a valid configuration. Please use: {list(_CONFIG_ARGS.keys())}"
            )
        value_casted: _ConfigValueType = self._apply_type(
            name=key, value=value, dtype=_CONFIG_ARGS[key].dtype, nullable=_CONFIG_ARGS[key].nullable
        )
        self._loaded_values[key] = value_casted

    def __getitem__(self, item: str) -> _ConfigValueType:
        if item not in self._loaded_values:
            raise AttributeError(f"{item} not configured yet.")
        return self._loaded_values[item]

    def _reset_item(self, item: str) -> None:
        if item in self._loaded_values:
            del self._loaded_values[item]
        self._load_config(name=item)

    def _repr_html_(self) -> Any:
        return self.to_pandas().to_html()

    @staticmethod
    def _apply_type(name: str, value: Any, dtype: Type[Union[str, bool, int]], nullable: bool) -> _ConfigValueType:
        if _Config._is_null(value=value):
            if nullable is True:
                return None
            exceptions.InvalidArgumentValue(f"{name} configuration does not accept a null value. Please pass {dtype}.")
        try:
            return dtype(value) if isinstance(value, dtype) is False else value
        except ValueError as ex:
            raise exceptions.InvalidConfiguration(f"Config {name} must receive a {dtype} value.") from ex

    @staticmethod
    def _is_null(value: _ConfigValueType) -> bool:
        if value is None:
            return True
        if isinstance(value, str) is True:
            value = cast(str, value)
            if value.lower() in ("none", "null", "nil"):
                return True
        return False

    @property
    def catalog_id(self) -> Optional[str]:
        """Property catalog_id."""
        return cast(Optional[str], self["catalog_id"])

    @catalog_id.setter
    def catalog_id(self, value: Optional[str]) -> None:
        self._set_config_value(key="catalog_id", value=value)

    @property
    def concurrent_partitioning(self) -> bool:
        """Property concurrent_partitioning."""
        return cast(bool, self["concurrent_partitioning"])

    @concurrent_partitioning.setter
    def concurrent_partitioning(self, value: bool) -> None:
        self._set_config_value(key="concurrent_partitioning", value=value)

    @property
    def ctas_approach(self) -> bool:
        """Property ctas_approach."""
        return cast(bool, self["ctas_approach"])

    @ctas_approach.setter
    def ctas_approach(self, value: bool) -> None:
        self._set_config_value(key="ctas_approach", value=value)

    @property
    def database(self) -> Optional[str]:
        """Property database."""
        return cast(Optional[str], self["database"])

    @database.setter
    def database(self, value: Optional[str]) -> None:
        self._set_config_value(key="database", value=value)

    @property
    def max_cache_query_inspections(self) -> int:
        """Property max_cache_query_inspections."""
        return cast(int, self["max_cache_query_inspections"])

    @max_cache_query_inspections.setter
    def max_cache_query_inspections(self, value: int) -> None:
        self._set_config_value(key="max_cache_query_inspections", value=value)

    @property
    def max_cache_seconds(self) -> int:
        """Property max_cache_seconds."""
        return cast(int, self["max_cache_seconds"])

    @max_cache_seconds.setter
    def max_cache_seconds(self, value: int) -> None:
        self._set_config_value(key="max_cache_seconds", value=value)

    @property
    def s3_block_size(self) -> int:
        """Property s3_block_size."""
        return cast(int, self["s3_block_size"])

    @s3_block_size.setter
    def s3_block_size(self, value: int) -> None:
        self._set_config_value(key="s3_block_size", value=value)


def _inject_config_doc(doc: Optional[str], available_configs: Tuple[str, ...]) -> str:
    if doc is None:
        return "Undocumented function."
    if "\n    Parameters" not in doc:
        return doc
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


def apply_configs(function: Callable[..., Any]) -> Callable[..., Any]:
    """Decorate some function with configs."""
    signature = inspect.signature(function)
    args_names: Tuple[str, ...] = tuple(signature.parameters.keys())
    available_configs: Tuple[str, ...] = tuple(x for x in _CONFIG_ARGS if x in args_names)

    def wrapper(*args_raw: Any, **kwargs: Any) -> Any:
        args: Dict[str, Any] = signature.bind_partial(*args_raw, **kwargs).arguments
        for name in available_configs:
            if hasattr(config, name) is True:
                value: _ConfigValueType = config[name]
                if name not in args:
                    _logger.debug("Applying default config argument %s with value %s.", name, value)
                elif _CONFIG_ARGS[name].enforced is True:
                    _logger.debug("Applying ENFORCED config argument %s with value %s.", name, value)
                args[name] = value
        for name, param in signature.parameters.items():
            if param.kind == param.VAR_KEYWORD and name in args:
                if isinstance(args[name], dict) is False:
                    raise RuntimeError(f"Argument {name} ({args[name]}) is a non dictionary keyword argument.")
                keywords: Dict[str, Any] = args[name]
                del args[name]
                args = {**args, **keywords}
        return function(**args)

    wrapper.__doc__ = _inject_config_doc(doc=function.__doc__, available_configs=available_configs)
    wrapper.__name__ = function.__name__
    wrapper.__setattr__("__signature__", signature)  # pylint: disable=no-member
    return wrapper


config: _Config = _Config()
