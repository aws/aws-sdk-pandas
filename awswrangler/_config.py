"""
Configuration file for

"""

class Config:
    def __init__(self):
        self._cache = False
        self._max_cache_seconds = 900

    @property
    def cache(self):
        return self._cache

    @cache.setter
    def cache(self, value: bool):
        self._cache = value

    @property
    def max_cache_seconds(self):
        return self._max_cache_seconds

    @max_cache_seconds.setter
    def max_cache_seconds(self, seconds: int):
        self._max_cache_seconds = seconds

config = Config()