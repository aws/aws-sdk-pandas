"""Configuration file for AWS SDK for pandas."""

import inspect
import logging
import os
from typing import Any, Callable, Dict, List, NamedTuple, Optional, Tuple, Type, Union, cast

import botocore.config
import pandas as pd

from awswrangler import exceptions

_logger: logging.Logger = logging.getLogger(__name__)


_ConfigValueType = Union[str, bool, int, botocore.config.Config, None]


class _ConfigArg(NamedTuple):
    dtype: Type[Union[str, bool, int, botocore.config.Config]]
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
    "max_remote_cache_entries": _ConfigArg(dtype=int, nullable=False),
    "max_local_cache_entries": _ConfigArg(dtype=int, nullable=False),
    "s3_block_size": _ConfigArg(dtype=int, nullable=False, enforced=True),
    "workgroup": _ConfigArg(dtype=str, nullable=False, enforced=True),
    "chunksize": _ConfigArg(dtype=int, nullable=False, enforced=True),
    # Endpoints URLs
    "s3_endpoint_url": _ConfigArg(dtype=str, nullable=True, enforced=True),
    "athena_endpoint_url": _ConfigArg(dtype=str, nullable=True, enforced=True),
    "sts_endpoint_url": _ConfigArg(dtype=str, nullable=True, enforced=True),
    "glue_endpoint_url": _ConfigArg(dtype=str, nullable=True, enforced=True),
    "redshift_endpoint_url": _ConfigArg(dtype=str, nullable=True, enforced=True),
    "kms_endpoint_url": _ConfigArg(dtype=str, nullable=True, enforced=True),
    "emr_endpoint_url": _ConfigArg(dtype=str, nullable=True, enforced=True),
    "lakeformation_endpoint_url": _ConfigArg(dtype=str, nullable=True, enforced=True),
    "dynamodb_endpoint_url": _ConfigArg(dtype=str, nullable=True, enforced=True),
    "secretsmanager_endpoint_url": _ConfigArg(dtype=str, nullable=True, enforced=True),
    "timestream_query_endpoint_url": _ConfigArg(dtype=str, nullable=True, enforced=True),
    "timestream_write_endpoint_url": _ConfigArg(dtype=str, nullable=True, enforced=True),
    # Botocore config
    "botocore_config": _ConfigArg(dtype=botocore.config.Config, nullable=True),
    "verify": _ConfigArg(dtype=str, nullable=True),
}


class _Config:  # pylint: disable=too-many-instance-attributes,too-many-public-methods
    """AWS Wrangler's Configuration class."""

    def __init__(self) -> None:
        self._loaded_values: Dict[str, _ConfigValueType] = {}
        name: str
        self.s3_endpoint_url = None
        self.athena_endpoint_url = None
        self.sts_endpoint_url = None
        self.glue_endpoint_url = None
        self.redshift_endpoint_url = None
        self.kms_endpoint_url = None
        self.emr_endpoint_url = None
        self.lakeformation_endpoint_url = None
        self.dynamodb_endpoint_url = None
        self.secretsmanager_endpoint_url = None
        self.timestream_write_endpoint_url = None
        self.timestream_query_endpoint_url = None
        self.botocore_config = None
        self.verify = None
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
            if item.endswith("_endpoint_url") or item == "verify":
                self._loaded_values[item] = None
            else:
                del self._loaded_values[item]
        self._load_config(name=item)

    def _repr_html_(self) -> Any:
        return self.to_pandas().to_html()

    @staticmethod
    def _apply_type(name: str, value: Any, dtype: Type[Union[str, bool, int]], nullable: bool) -> _ConfigValueType:
        if _Config._is_null(value=value):
            if nullable is True:
                return None
            raise exceptions.InvalidArgumentValue(
                f"{name} configuration does not accept a null value. Please pass {dtype}."
            )
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
    def max_local_cache_entries(self) -> int:
        """Property max_local_cache_entries."""
        return cast(int, self["max_local_cache_entries"])

    @max_local_cache_entries.setter
    def max_local_cache_entries(self, value: int) -> None:
        try:
            max_remote_cache_entries = cast(int, self["max_remote_cache_entries"])
        except AttributeError:
            max_remote_cache_entries = 50
        if value < max_remote_cache_entries:
            _logger.warning(
                "max_remote_cache_entries shouldn't be greater than max_local_cache_entries. "
                "Therefore max_remote_cache_entries will be set to %s as well.",
                value,
            )
            self._set_config_value(key="max_remote_cache_entries", value=value)
        self._set_config_value(key="max_local_cache_entries", value=value)

    @property
    def max_remote_cache_entries(self) -> int:
        """Property max_remote_cache_entries."""
        return cast(int, self["max_remote_cache_entries"])

    @max_remote_cache_entries.setter
    def max_remote_cache_entries(self, value: int) -> None:
        self._set_config_value(key="max_remote_cache_entries", value=value)

    @property
    def s3_block_size(self) -> int:
        """Property s3_block_size."""
        return cast(int, self["s3_block_size"])

    @s3_block_size.setter
    def s3_block_size(self, value: int) -> None:
        self._set_config_value(key="s3_block_size", value=value)

    @property
    def workgroup(self) -> Optional[str]:
        """Property workgroup."""
        return cast(Optional[str], self["workgroup"])

    @workgroup.setter
    def workgroup(self, value: Optional[str]) -> None:
        self._set_config_value(key="workgroup", value=value)

    @property
    def chunksize(self) -> int:
        """Property chunksize."""
        return cast(int, self["chunksize"])

    @chunksize.setter
    def chunksize(self, value: int) -> None:
        self._set_config_value(key="chunksize", value=value)

    @property
    def s3_endpoint_url(self) -> Optional[str]:
        """Property s3_endpoint_url."""
        return cast(Optional[str], self["s3_endpoint_url"])

    @s3_endpoint_url.setter
    def s3_endpoint_url(self, value: Optional[str]) -> None:
        self._set_config_value(key="s3_endpoint_url", value=value)

    @property
    def athena_endpoint_url(self) -> Optional[str]:
        """Property athena_endpoint_url."""
        return cast(Optional[str], self["athena_endpoint_url"])

    @athena_endpoint_url.setter
    def athena_endpoint_url(self, value: Optional[str]) -> None:
        self._set_config_value(key="athena_endpoint_url", value=value)

    @property
    def sts_endpoint_url(self) -> Optional[str]:
        """Property sts_endpoint_url."""
        return cast(Optional[str], self["sts_endpoint_url"])

    @sts_endpoint_url.setter
    def sts_endpoint_url(self, value: Optional[str]) -> None:
        self._set_config_value(key="sts_endpoint_url", value=value)

    @property
    def glue_endpoint_url(self) -> Optional[str]:
        """Property glue_endpoint_url."""
        return cast(Optional[str], self["glue_endpoint_url"])

    @glue_endpoint_url.setter
    def glue_endpoint_url(self, value: Optional[str]) -> None:
        self._set_config_value(key="glue_endpoint_url", value=value)

    @property
    def redshift_endpoint_url(self) -> Optional[str]:
        """Property redshift_endpoint_url."""
        return cast(Optional[str], self["redshift_endpoint_url"])

    @redshift_endpoint_url.setter
    def redshift_endpoint_url(self, value: Optional[str]) -> None:
        self._set_config_value(key="redshift_endpoint_url", value=value)

    @property
    def kms_endpoint_url(self) -> Optional[str]:
        """Property kms_endpoint_url."""
        return cast(Optional[str], self["kms_endpoint_url"])

    @kms_endpoint_url.setter
    def kms_endpoint_url(self, value: Optional[str]) -> None:
        self._set_config_value(key="kms_endpoint_url", value=value)

    @property
    def emr_endpoint_url(self) -> Optional[str]:
        """Property emr_endpoint_url."""
        return cast(Optional[str], self["emr_endpoint_url"])

    @emr_endpoint_url.setter
    def emr_endpoint_url(self, value: Optional[str]) -> None:
        self._set_config_value(key="emr_endpoint_url", value=value)

    @property
    def lakeformation_endpoint_url(self) -> Optional[str]:
        """Property lakeformation_endpoint_url."""
        return cast(Optional[str], self["lakeformation_endpoint_url"])

    @lakeformation_endpoint_url.setter
    def lakeformation_endpoint_url(self, value: Optional[str]) -> None:
        self._set_config_value(key="lakeformation_endpoint_url", value=value)

    @property
    def dynamodb_endpoint_url(self) -> Optional[str]:
        """Property dynamodb_endpoint_url."""
        return cast(Optional[str], self["dynamodb_endpoint_url"])

    @dynamodb_endpoint_url.setter
    def dynamodb_endpoint_url(self, value: Optional[str]) -> None:
        self._set_config_value(key="dynamodb_endpoint_url", value=value)

    @property
    def secretsmanager_endpoint_url(self) -> Optional[str]:
        """Property secretsmanager_endpoint_url."""
        return cast(Optional[str], self["secretsmanager_endpoint_url"])

    @secretsmanager_endpoint_url.setter
    def secretsmanager_endpoint_url(self, value: Optional[str]) -> None:
        self._set_config_value(key="secretsmanager_endpoint_url", value=value)

    @property
    def timestream_query_endpoint_url(self) -> Optional[str]:
        """
        Property timestream_query_endpoint_url.

        Before setting this endpoint, consult the documentation for DescribeEndpoints.
        https://docs.aws.amazon.com/timestream/latest/developerguide/API_DescribeEndpoints.html
        """
        return cast(Optional[str], self["timestream_query_endpoint_url"])

    @timestream_query_endpoint_url.setter
    def timestream_query_endpoint_url(self, value: Optional[str]) -> None:
        self._set_config_value(key="timestream_query_endpoint_url", value=value)

    @property
    def timestream_write_endpoint_url(self) -> Optional[str]:
        """
        Property timestream_write_endpoint_url.

        Before setting this endpoint, consult the documentation for DescribeEndpoints.
        https://docs.aws.amazon.com/timestream/latest/developerguide/API_DescribeEndpoints.html
        """
        return cast(Optional[str], self["timestream_write_endpoint_url"])

    @timestream_write_endpoint_url.setter
    def timestream_write_endpoint_url(self, value: Optional[str]) -> None:
        self._set_config_value(key="timestream_write_endpoint_url", value=value)

    @property
    def botocore_config(self) -> botocore.config.Config:
        """Property botocore_config."""
        return cast(Optional[botocore.config.Config], self["botocore_config"])

    @botocore_config.setter
    def botocore_config(self, value: Optional[botocore.config.Config]) -> None:
        self._set_config_value(key="botocore_config", value=value)

    @property
    def verify(self) -> Optional[str]:
        """Property verify."""
        return cast(Optional[str], self["verify"])

    @verify.setter
    def verify(self, value: Optional[str]) -> None:
        self._set_config_value(key="verify", value=value)


def _insert_str(text: str, token: str, insert: str) -> str:
    """Insert string into other."""
    index: int = text.find(token)
    return text[:index] + insert + text[index:]


def _inject_config_doc(doc: Optional[str], available_configs: Tuple[str, ...]) -> str:
    if doc is None:
        return "Undocumented function."
    if "\n    Parameters" not in doc:
        return doc
    header: str = (
        "\n\n    Note\n    ----"
        "\n    This function has arguments which can be configured globally through "
        "*wr.config* or environment variables:\n\n"
    )
    args: Tuple[str, ...] = tuple(f"    - {x}\n" for x in available_configs)
    args_block: str = "\n".join(args)
    footer: str = (
        "\n    Check out the `Global Configurations Tutorial "
        "<https://github.com/aws/aws-sdk-pandas/blob/main/tutorials/"
        "021%20-%20Global%20Configurations.ipynb>`_"
        " for details.\n"
    )
    insertion: str = header + args_block + footer + "\n\n"
    return _insert_str(text=doc, token="\n    Parameters", insert=insertion)


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
                    args[name] = value
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
