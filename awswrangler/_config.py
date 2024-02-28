"""Configuration file for AWS SDK for pandas."""

from __future__ import annotations

import inspect
import logging
import os
from functools import wraps
from typing import Any, Callable, Dict, NamedTuple, Optional, TypeVar, Union, cast

import botocore.config
import pandas as pd
from typing_extensions import Literal

from awswrangler import exceptions
from awswrangler.typing import AthenaCacheSettings

_logger: logging.Logger = logging.getLogger(__name__)


_ConfigValueType = Union[str, bool, int, float, botocore.config.Config, Dict[Any, Any]]


class _ConfigArg(NamedTuple):
    dtype: type[_ConfigValueType]
    nullable: bool
    enforced: bool = False
    loaded: bool = False
    default: _ConfigValueType | None = None
    parent_parameter_key: str | None = None
    is_parent: bool = False


# Please, also add any new argument as a property in the _Config class
_CONFIG_ARGS: dict[str, _ConfigArg] = {
    "catalog_id": _ConfigArg(dtype=str, nullable=True),
    "concurrent_partitioning": _ConfigArg(dtype=bool, nullable=False),
    "ctas_approach": _ConfigArg(dtype=bool, nullable=False),
    "database": _ConfigArg(dtype=str, nullable=True),
    "athena_cache_settings": _ConfigArg(dtype=dict, nullable=False, is_parent=True, loaded=True),
    "max_cache_query_inspections": _ConfigArg(dtype=int, nullable=False, parent_parameter_key="athena_cache_settings"),
    "max_cache_seconds": _ConfigArg(dtype=int, nullable=False, parent_parameter_key="athena_cache_settings"),
    "max_remote_cache_entries": _ConfigArg(dtype=int, nullable=False, parent_parameter_key="athena_cache_settings"),
    "max_local_cache_entries": _ConfigArg(dtype=int, nullable=False, parent_parameter_key="athena_cache_settings"),
    "athena_query_wait_polling_delay": _ConfigArg(dtype=float, nullable=False),
    "cloudwatch_query_wait_polling_delay": _ConfigArg(dtype=float, nullable=False),
    "neptune_load_wait_polling_delay": _ConfigArg(dtype=float, nullable=False),
    "timestream_batch_load_wait_polling_delay": _ConfigArg(dtype=float, nullable=False),
    "emr_serverless_job_wait_polling_delay": _ConfigArg(dtype=float, nullable=False),
    "s3_block_size": _ConfigArg(dtype=int, nullable=False, enforced=True),
    "workgroup": _ConfigArg(dtype=str, nullable=False, enforced=True),
    "chunksize": _ConfigArg(dtype=int, nullable=False, enforced=True),
    "suppress_warnings": _ConfigArg(dtype=bool, nullable=False, default=False, loaded=True),
    "dtype_backend": _ConfigArg(dtype=str, nullable=True),
    # Endpoints URLs
    "s3_endpoint_url": _ConfigArg(dtype=str, nullable=True, enforced=True, loaded=True),
    "athena_endpoint_url": _ConfigArg(dtype=str, nullable=True, enforced=True, loaded=True),
    "sts_endpoint_url": _ConfigArg(dtype=str, nullable=True, enforced=True, loaded=True),
    "glue_endpoint_url": _ConfigArg(dtype=str, nullable=True, enforced=True, loaded=True),
    "redshift_endpoint_url": _ConfigArg(dtype=str, nullable=True, enforced=True, loaded=True),
    "kms_endpoint_url": _ConfigArg(dtype=str, nullable=True, enforced=True, loaded=True),
    "emr_endpoint_url": _ConfigArg(dtype=str, nullable=True, enforced=True, loaded=True),
    "dynamodb_endpoint_url": _ConfigArg(dtype=str, nullable=True, enforced=True, loaded=True),
    "secretsmanager_endpoint_url": _ConfigArg(dtype=str, nullable=True, enforced=True, loaded=True),
    "timestream_query_endpoint_url": _ConfigArg(dtype=str, nullable=True, enforced=True, loaded=True),
    "timestream_write_endpoint_url": _ConfigArg(dtype=str, nullable=True, enforced=True, loaded=True),
    # Botocore config
    "botocore_config": _ConfigArg(dtype=botocore.config.Config, nullable=True),
    "verify": _ConfigArg(dtype=str, nullable=True, loaded=True),
    # Distributed
    "address": _ConfigArg(dtype=str, nullable=True),
    "redis_password": _ConfigArg(dtype=str, nullable=True),
    "ignore_reinit_error": _ConfigArg(dtype=bool, nullable=True),
    "include_dashboard": _ConfigArg(dtype=bool, nullable=True),
    "configure_logging": _ConfigArg(dtype=bool, nullable=True),
    "log_to_driver": _ConfigArg(dtype=bool, nullable=True),
    "logging_level": _ConfigArg(dtype=int, nullable=True),
    "object_store_memory": _ConfigArg(dtype=int, nullable=True),
    "cpu_count": _ConfigArg(dtype=int, nullable=True),
    "gpu_count": _ConfigArg(dtype=int, nullable=True),
}


class _Config:
    """AWS Wrangler's Configuration class."""

    def __init__(self) -> None:
        self._loaded_values: dict[str, _ConfigValueType | None] = {}
        self.botocore_config = None

        for name in _CONFIG_ARGS:
            self._load_config(name=name)

    def reset(self, item: str | None = None) -> None:
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
        args: list[dict[str, Any]] = []
        for k, v in _CONFIG_ARGS.items():
            if v.is_parent:
                continue

            arg: dict[str, Any] = {
                "name": k,
                "Env.Variable": f"WR_{k.upper()}",
                "type": v.dtype,
                "nullable": v.nullable,
                "enforced": v.enforced,
                "parent_parameter_name": v.parent_parameter_key,
            }
            if k in self._loaded_values:
                arg["configured"] = True
                arg["value"] = self._loaded_values[k]
            else:
                arg["configured"] = False
                arg["value"] = None
            args.append(arg)
        return pd.DataFrame(args)

    def _load_config(self, name: str) -> None:
        if _CONFIG_ARGS[name].is_parent:
            if self._loaded_values.get(name) is None:
                self._set_config_value(key=name, value={})
            return

        if _CONFIG_ARGS[name].loaded:
            self._set_config_value(key=name, value=_CONFIG_ARGS[name].default)

        env_var: str | None = os.getenv(f"WR_{name.upper()}")
        if env_var is not None:
            self._set_config_value(key=name, value=env_var)

    def _set_config_value(self, key: str, value: Any) -> None:
        if key not in _CONFIG_ARGS:
            raise exceptions.InvalidArgumentValue(
                f"{key} is not a valid configuration. Please use: {list(_CONFIG_ARGS.keys())}"
            )
        value_casted: _ConfigValueType | None = self._apply_type(
            name=key,
            value=value,
            dtype=_CONFIG_ARGS[key].dtype,
            nullable=_CONFIG_ARGS[key].nullable,
        )

        parent_key = _CONFIG_ARGS[key].parent_parameter_key
        if parent_key:
            self._loaded_values[parent_key][key] = value_casted  # type: ignore[index]
        else:
            self._loaded_values[key] = value_casted

    def __getitem__(self, item: str) -> _ConfigValueType | None:
        if issubclass(_CONFIG_ARGS[item].dtype, dict):
            return self._loaded_values[item]

        loaded_values: dict[str, _ConfigValueType | None]
        parent_key = _CONFIG_ARGS[item].parent_parameter_key
        if parent_key:
            loaded_values = self[parent_key]  # type: ignore[assignment]
        else:
            loaded_values = self._loaded_values

        if item not in loaded_values:
            raise AttributeError(f"{item} not configured yet.")

        return loaded_values[item]

    def _reset_item(self, item: str) -> None:
        config_arg = _CONFIG_ARGS[item]
        loaded_values: dict[str, _ConfigValueType | None]

        if config_arg.parent_parameter_key:
            loaded_values = self[config_arg.parent_parameter_key]  # type: ignore[assignment]
        else:
            loaded_values = self._loaded_values

        if item in loaded_values:
            if config_arg.is_parent:
                loaded_values[item] = {}
            elif config_arg.loaded:
                loaded_values[item] = config_arg.default
            else:
                del loaded_values[item]

        self._load_config(name=item)

    def _repr_html_(self) -> Any:
        return self.to_pandas().to_html()

    @staticmethod
    def _apply_type(name: str, value: Any, dtype: type[_ConfigValueType], nullable: bool) -> _ConfigValueType | None:
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
    def _is_null(value: _ConfigValueType | None) -> bool:
        if value is None:
            return True
        if isinstance(value, str) is True:
            value = cast(str, value)
            if value.lower() in ("none", "null", "nil"):
                return True
        return False

    @property
    def catalog_id(self) -> str | None:
        """Property catalog_id."""
        return cast(Optional[str], self["catalog_id"])

    @catalog_id.setter
    def catalog_id(self, value: str | None) -> None:
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
    def database(self) -> str | None:
        """Property database."""
        return cast(Optional[str], self["database"])

    @database.setter
    def database(self, value: str | None) -> None:
        self._set_config_value(key="database", value=value)

    @property
    def athena_cache_settings(self) -> AthenaCacheSettings:
        """Property athena_cache_settings."""
        return cast(AthenaCacheSettings, self["athena_cache_settings"])

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
    def athena_query_wait_polling_delay(self) -> float:
        """Property athena_query_wait_polling_delay."""
        return cast(float, self["athena_query_wait_polling_delay"])

    @athena_query_wait_polling_delay.setter
    def athena_query_wait_polling_delay(self, value: float) -> None:
        self._set_config_value(key="athena_query_wait_polling_delay", value=value)

    @property
    def cloudwatch_query_wait_polling_delay(self) -> float:
        """Property cloudwatch_query_wait_polling_delay."""
        return cast(float, self["cloudwatch_query_wait_polling_delay"])

    @cloudwatch_query_wait_polling_delay.setter
    def cloudwatch_query_wait_polling_delay(self, value: float) -> None:
        self._set_config_value(key="cloudwatch_query_wait_polling_delay", value=value)

    @property
    def neptune_load_wait_polling_delay(self) -> float:
        """Property neptune_load_wait_polling_delay."""
        return cast(float, self["neptune_load_wait_polling_delay"])

    @neptune_load_wait_polling_delay.setter
    def neptune_load_wait_polling_delay(self, value: float) -> None:
        self._set_config_value(key="neptune_load_wait_polling_delay", value=value)

    @property
    def timestream_batch_load_wait_polling_delay(self) -> float:
        """Property timestream_batch_load_wait_polling_delay."""
        return cast(float, self["timestream_batch_load_wait_polling_delay"])

    @timestream_batch_load_wait_polling_delay.setter
    def timestream_batch_load_wait_polling_delay(self, value: float) -> None:
        self._set_config_value(key="timestream_batch_load_wait_polling_delay", value=value)

    @property
    def emr_serverless_job_wait_polling_delay(self) -> float:
        """Property emr_serverless_job_wait_polling_delay."""
        return cast(float, self["emr_serverless_job_wait_polling_delay"])

    @emr_serverless_job_wait_polling_delay.setter
    def emr_serverless_job_wait_polling_delay(self, value: float) -> None:
        self._set_config_value(key="emr_serverless_job_wait_polling_delay", value=value)

    @property
    def s3_block_size(self) -> int:
        """Property s3_block_size."""
        return cast(int, self["s3_block_size"])

    @s3_block_size.setter
    def s3_block_size(self, value: int) -> None:
        self._set_config_value(key="s3_block_size", value=value)

    @property
    def workgroup(self) -> str | None:
        """Property workgroup."""
        return cast(Optional[str], self["workgroup"])

    @workgroup.setter
    def workgroup(self, value: str | None) -> None:
        self._set_config_value(key="workgroup", value=value)

    @property
    def chunksize(self) -> int:
        """Property chunksize."""
        return cast(int, self["chunksize"])

    @chunksize.setter
    def chunksize(self, value: int) -> None:
        self._set_config_value(key="chunksize", value=value)

    @property
    def suppress_warnings(self) -> bool:
        """Property suppress_warnings."""
        return cast(bool, self["suppress_warnings"])

    @suppress_warnings.setter
    def suppress_warnings(self, value: bool) -> None:
        self._set_config_value(key="suppress_warnings", value=value)

    @property
    def dtype_backend(self) -> Literal["numpy_nullable", "pyarrow", None]:
        """Property dtype_backend."""
        return cast(Literal["numpy_nullable", "pyarrow", None], self["dtype_backend"])

    @dtype_backend.setter
    def dtype_backend(self, value: Literal["numpy_nullable", "pyarrow", None]) -> None:
        self._set_config_value(key="dtype_backend", value=value)

    @property
    def s3_endpoint_url(self) -> str | None:
        """Property s3_endpoint_url."""
        return cast(Optional[str], self["s3_endpoint_url"])

    @s3_endpoint_url.setter
    def s3_endpoint_url(self, value: str | None) -> None:
        self._set_config_value(key="s3_endpoint_url", value=value)

    @property
    def athena_endpoint_url(self) -> str | None:
        """Property athena_endpoint_url."""
        return cast(Optional[str], self["athena_endpoint_url"])

    @athena_endpoint_url.setter
    def athena_endpoint_url(self, value: str | None) -> None:
        self._set_config_value(key="athena_endpoint_url", value=value)

    @property
    def sts_endpoint_url(self) -> str | None:
        """Property sts_endpoint_url."""
        return cast(Optional[str], self["sts_endpoint_url"])

    @sts_endpoint_url.setter
    def sts_endpoint_url(self, value: str | None) -> None:
        self._set_config_value(key="sts_endpoint_url", value=value)

    @property
    def glue_endpoint_url(self) -> str | None:
        """Property glue_endpoint_url."""
        return cast(Optional[str], self["glue_endpoint_url"])

    @glue_endpoint_url.setter
    def glue_endpoint_url(self, value: str | None) -> None:
        self._set_config_value(key="glue_endpoint_url", value=value)

    @property
    def redshift_endpoint_url(self) -> str | None:
        """Property redshift_endpoint_url."""
        return cast(Optional[str], self["redshift_endpoint_url"])

    @redshift_endpoint_url.setter
    def redshift_endpoint_url(self, value: str | None) -> None:
        self._set_config_value(key="redshift_endpoint_url", value=value)

    @property
    def kms_endpoint_url(self) -> str | None:
        """Property kms_endpoint_url."""
        return cast(Optional[str], self["kms_endpoint_url"])

    @kms_endpoint_url.setter
    def kms_endpoint_url(self, value: str | None) -> None:
        self._set_config_value(key="kms_endpoint_url", value=value)

    @property
    def emr_endpoint_url(self) -> str | None:
        """Property emr_endpoint_url."""
        return cast(Optional[str], self["emr_endpoint_url"])

    @emr_endpoint_url.setter
    def emr_endpoint_url(self, value: str | None) -> None:
        self._set_config_value(key="emr_endpoint_url", value=value)

    @property
    def dynamodb_endpoint_url(self) -> str | None:
        """Property dynamodb_endpoint_url."""
        return cast(Optional[str], self["dynamodb_endpoint_url"])

    @dynamodb_endpoint_url.setter
    def dynamodb_endpoint_url(self, value: str | None) -> None:
        self._set_config_value(key="dynamodb_endpoint_url", value=value)

    @property
    def secretsmanager_endpoint_url(self) -> str | None:
        """Property secretsmanager_endpoint_url."""
        return cast(Optional[str], self["secretsmanager_endpoint_url"])

    @secretsmanager_endpoint_url.setter
    def secretsmanager_endpoint_url(self, value: str | None) -> None:
        self._set_config_value(key="secretsmanager_endpoint_url", value=value)

    @property
    def timestream_query_endpoint_url(self) -> str | None:
        """
        Property timestream_query_endpoint_url.

        Before setting this endpoint, consult the documentation for DescribeEndpoints.
        https://docs.aws.amazon.com/timestream/latest/developerguide/API_DescribeEndpoints.html
        """
        return cast(Optional[str], self["timestream_query_endpoint_url"])

    @timestream_query_endpoint_url.setter
    def timestream_query_endpoint_url(self, value: str | None) -> None:
        self._set_config_value(key="timestream_query_endpoint_url", value=value)

    @property
    def timestream_write_endpoint_url(self) -> str | None:
        """
        Property timestream_write_endpoint_url.

        Before setting this endpoint, consult the documentation for DescribeEndpoints.
        https://docs.aws.amazon.com/timestream/latest/developerguide/API_DescribeEndpoints.html
        """
        return cast(Optional[str], self["timestream_write_endpoint_url"])

    @timestream_write_endpoint_url.setter
    def timestream_write_endpoint_url(self, value: str | None) -> None:
        self._set_config_value(key="timestream_write_endpoint_url", value=value)

    @property
    def botocore_config(self) -> botocore.config.Config | None:
        """Property botocore_config."""
        return cast(Optional[botocore.config.Config], self["botocore_config"])

    @botocore_config.setter
    def botocore_config(self, value: botocore.config.Config | None) -> None:
        self._set_config_value(key="botocore_config", value=value)

    @property
    def verify(self) -> str | None:
        """Property verify."""
        return cast(Optional[str], self["verify"])

    @verify.setter
    def verify(self, value: str | None) -> None:
        self._set_config_value(key="verify", value=value)

    @property
    def address(self) -> str | None:
        """Property address."""
        return cast(Optional[str], self["address"])

    @address.setter
    def address(self, value: str | None) -> None:
        self._set_config_value(key="address", value=value)

    @property
    def ignore_reinit_error(self) -> bool | None:
        """Property ignore_reinit_error."""
        return cast(Optional[bool], self["ignore_reinit_error"])

    @ignore_reinit_error.setter
    def ignore_reinit_error(self, value: bool | None) -> None:
        self._set_config_value(key="ignore_reinit_error", value=value)

    @property
    def include_dashboard(self) -> bool | None:
        """Property include_dashboard."""
        return cast(Optional[bool], self["include_dashboard"])

    @include_dashboard.setter
    def include_dashboard(self, value: bool | None) -> None:
        self._set_config_value(key="include_dashboard", value=value)

    @property
    def redis_password(self) -> str | None:
        """Property redis_password."""
        return cast(Optional[str], self["redis_password"])

    @redis_password.setter
    def redis_password(self, value: str | None) -> None:
        self._set_config_value(key="redis_password", value=value)

    @property
    def configure_logging(self) -> bool | None:
        """Property configure_logging."""
        return cast(Optional[bool], self["configure_logging"])

    @configure_logging.setter
    def configure_logging(self, value: bool | None) -> None:
        self._set_config_value(key="configure_logging", value=value)

    @property
    def log_to_driver(self) -> bool | None:
        """Property log_to_driver."""
        return cast(Optional[bool], self["log_to_driver"])

    @log_to_driver.setter
    def log_to_driver(self, value: bool | None) -> None:
        self._set_config_value(key="log_to_driver", value=value)

    @property
    def logging_level(self) -> int:
        """Property logging_level."""
        return cast(int, self["logging_level"])

    @logging_level.setter
    def logging_level(self, value: int) -> None:
        self._set_config_value(key="logging_level", value=value)

    @property
    def object_store_memory(self) -> int:
        """Property object_store_memory."""
        return cast(int, self["object_store_memory"])

    @object_store_memory.setter
    def object_store_memory(self, value: int) -> None:
        self._set_config_value(key="object_store_memory", value=value)

    @property
    def cpu_count(self) -> int:
        """Property cpu_count."""
        return cast(int, self["cpu_count"])

    @cpu_count.setter
    def cpu_count(self, value: int) -> None:
        self._set_config_value(key="cpu_count", value=value)

    @property
    def gpu_count(self) -> int:
        """Property gpu_count."""
        return cast(int, self["gpu_count"])

    @gpu_count.setter
    def gpu_count(self, value: int) -> None:
        self._set_config_value(key="gpu_count", value=value)


def _insert_str(text: str, token: str, insert: str) -> str:
    """Insert string into other."""
    index: int = text.find(token)
    return text[:index] + insert + text[index:]


def _inject_config_doc(doc: str | None, available_configs: tuple[str, ...]) -> str:
    if doc is None:
        return "Undocumented function."
    if "\n    Parameters" not in doc:
        return doc
    header: str = (
        "\n\n    Note\n    ----"
        "\n    This function has arguments which can be configured globally through "
        "*wr.config* or environment variables:\n\n"
    )
    args: tuple[str, ...] = tuple(f"    - {x}\n" for x in available_configs)
    args_block: str = "\n".join(args)
    footer: str = (
        "\n    Check out the `Global Configurations Tutorial "
        "<https://github.com/aws/aws-sdk-pandas/blob/main/tutorials/"
        "021%20-%20Global%20Configurations.ipynb>`_"
        " for details.\n"
    )
    insertion: str = header + args_block + footer + "\n\n"
    return _insert_str(text=doc, token="\n    Parameters", insert=insertion)


def _assign_args_value(args: dict[str, Any], name: str, value: Any) -> None:
    if _CONFIG_ARGS[name].is_parent:
        if name not in args:
            args[name] = {}

        nested_args = cast(Dict[str, Any], value)
        for nested_arg_name, nested_arg_value in nested_args.items():
            _assign_args_value(args[name], nested_arg_name, nested_arg_value)
        return

    if name not in args:
        _logger.debug("Applying default config argument %s with value %s.", name, value)
        args[name] = value

    elif _CONFIG_ARGS[name].enforced is True:
        _logger.debug("Applying ENFORCED config argument %s with value %s.", name, value)
        args[name] = value


FunctionType = TypeVar("FunctionType", bound=Callable[..., Any])


def apply_configs(function: FunctionType) -> FunctionType:
    """Decorate some function with configs."""
    signature = inspect.signature(function)
    args_names: tuple[str, ...] = tuple(signature.parameters.keys())
    available_configs: tuple[str, ...] = tuple(x for x in _CONFIG_ARGS if x in args_names)

    @wraps(function)
    def wrapper(*args_raw: Any, **kwargs: Any) -> Any:
        args: dict[str, Any] = signature.bind_partial(*args_raw, **kwargs).arguments
        for name in available_configs:
            if hasattr(config, name) is True:
                value = config[name]
                _assign_args_value(args, name, value)

        for name, param in signature.parameters.items():
            if param.kind == param.VAR_KEYWORD and name in args:
                if isinstance(args[name], dict) is False:
                    raise RuntimeError(f"Argument {name} ({args[name]}) is a non dictionary keyword argument.")
                keywords: dict[str, Any] = args[name]
                del args[name]
                args = {**args, **keywords}
        return function(**args)

    wrapper.__doc__ = _inject_config_doc(doc=function.__doc__, available_configs=available_configs)
    wrapper.__name__ = function.__name__
    wrapper.__setattr__("__signature__", signature)
    return wrapper  # type: ignore[return-value]


config: _Config = _Config()
