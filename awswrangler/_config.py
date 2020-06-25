"""Configuration file for awswrangler."""

import json
import logging
import os
from typing import Any, Dict, NamedTuple, Type


class _Config(NamedTuple):
    env_var: str
    type: Type
    default: Any


CONFIG_FILE: str = ".aws_wrangler.json"
CONFIG_DEFAULTS: Dict[str, _Config] = {
    "cache": _Config(env_var="AWS_WRANGLER_CACHE", type=bool, default=False),
    "max_cache_seconds": _Config(env_var="AWS_WRANGLER_MAX_CACHE_SECONDS", type=int, default=900),
}

# initialize logger
LOG_FORMAT = '{"time_stamp": "%(asctime)s", "log_level": "%(levelname)s", "log_message": %(message)s}'
logging.basicConfig(format=LOG_FORMAT, datefmt="%Y-%m-%d %H:%M:%S")
config_logger = logging.getLogger(__name__)
config_logger.setLevel(getattr(logging, os.getenv("LOG_LEVEL", "INFO")))


class Config:
    """Wrangler's Configuration class."""

    __slots__ = list([f"_{attr}" for attr, _ in CONFIG_DEFAULTS.items()])

    def __init__(self, config_file: str = CONFIG_FILE):
        """Instatiate a Config object.

        Parameters
        ----------
        config_file : str
            Configuration file path.

        Returns
        -------
        Config
            Config object.

        Examples
        --------
        >>> import awswrangler as wr
        >>> config = wr.config.Config()

        """
        if os.path.exists(config_file) is True:
            with open(config_file, "r", encoding="utf-8") as f:
                json_values: Dict[str, Any] = json.load(f)
        else:
            json_values = {}

        # consider only attributes found as keys in CONFIG_DEFAULTS
        for attr, conf in CONFIG_DEFAULTS.items():
            if os.getenv(conf.env_var):
                value: Any = conf.type(os.getenv(conf.env_var))
            elif json_values.get(attr):
                value = json_values.get(attr)
            else:
                value = conf.default
            super(Config, self).__setattr__(f"_{attr}", value)

    def __getattr__(self, item: str) -> Any:
        """Get a configuration item."""
        if item in CONFIG_DEFAULTS.keys():
            return super(Config, self).__getattribute__(f"_{item}")
        raise AttributeError

    def __setattr__(self, key: str, value: Any) -> Any:
        """Set a configuration item."""
        if (key in CONFIG_DEFAULTS.keys()) and (isinstance(value, CONFIG_DEFAULTS[key].type) is True):
            super(Config, self).__setattr__(f"_{key}", value)
        else:
            try:
                super(Config, self).__setattr__(f"_{key}", CONFIG_DEFAULTS[key].type(value))
            except:  # noqa
                raise TypeError


config: Config = Config()
