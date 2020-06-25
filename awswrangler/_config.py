"""
Configuration file for awswrangler

"""
import json
import logging
import os
from collections import namedtuple
from typing import Dict

_Config = namedtuple("_Config", ["env_var", "cast", "default"])  # str  # callable
CONFIG_FILE = ".aws_wrangler.json"
CONFIG_DEFAULTS: Dict[str, _Config] = {
    "cache": _Config(env_var="AWS_WRANGLER_CACHE", cast=bool, default=False),
    "max_cache_seconds": _Config(env_var="AWS_WRANGLER_MAX_CACHE_SECONDS", cast=int, default=900),
}

# initialise logger
LOG_FORMAT = '{"time_stamp": "%(asctime)s", "log_level": "%(levelname)s", "log_message": %(message)s}'
logging.basicConfig(format=LOG_FORMAT, datefmt="%Y-%m-%d %H:%M:%S")
config_logger = logging.getLogger(__name__)
config_logger.setLevel(getattr(logging, os.getenv("LOG_LEVEL", "INFO")))


class Config:
    __slots__ = list([f"_{attr}" for attr, _ in CONFIG_DEFAULTS.items()])

    def __init__(self, config_file: str = CONFIG_FILE):
        if os.path.exists(config_file):
            with open(config_file, "r", encoding="utf-8") as f:
                json_values = json.load(f)
        else:
            json_values = {}

            # consider only attributes found as keys in CONFIG_DEFAULTS
        for attr, conf in CONFIG_DEFAULTS.items():
            if os.getenv(conf.env_var):
                value = conf.cast(os.getenv(conf.env_var))
            elif json_values.get(attr):
                value = json_values.get(attr)
            else:
                value = conf.default
            super(Config, self).__setattr__(f"_{attr}", value)

    def __getattr__(self, item):
        if item in CONFIG_DEFAULTS.keys():
            return super(Config, self).__getattribute__(f"_{item}")
        else:
            raise AttributeError

    def __setattr__(self, key, value):
        if key in CONFIG_DEFAULTS.keys() and isinstance(value, CONFIG_DEFAULTS[key].cast):
            super(Config, self).__setattr__(f"_{key}", value)
        else:
            try:
                super(Config, self).__setattr__(f"_{key}", CONFIG_DEFAULTS[key].cast(value))
            except:
                raise TypeError


config = Config()
