"""Amazon S3 Concurrent Write Module (PRIVATE)."""

from __future__ import annotations

import concurrent.futures
import logging
from typing import Any, Callable

import pandas as pd

from awswrangler import _utils

_logger: logging.Logger = logging.getLogger(__name__)


class _WriteProxy:
    def __init__(self, use_threads: bool | int):
        self._exec: concurrent.futures.ThreadPoolExecutor | None
        self._results: list[str] = []
        self._cpus: int = _utils.ensure_cpu_count(use_threads=use_threads)
        if self._cpus > 1:
            self._exec = concurrent.futures.ThreadPoolExecutor(max_workers=self._cpus)
            self._futures: list[Any] = []
        else:
            self._exec = None

    @staticmethod
    def _caller(func: Callable[..., pd.DataFrame], *args: Any, func_kwargs: dict[str, Any]) -> pd.DataFrame:
        _logger.debug("Calling: %s", func)
        return func(*args, **func_kwargs)

    def write(self, func: Callable[..., list[str]], *args: Any, **func_kwargs: Any) -> None:
        """Write File."""
        if self._exec is not None:
            _utils.block_waiting_available_thread(seq=self._futures, max_workers=self._cpus)
            _logger.debug("Submitting: %s", func)
            future = self._exec.submit(
                _WriteProxy._caller,
                func,
                *args,
                func_kwargs=func_kwargs,
            )
            self._futures.append(future)
        else:
            self._results += func(*args, **func_kwargs)

    def close(self) -> list[str]:
        """Close the proxy."""
        if self._exec is not None:
            for future in concurrent.futures.as_completed(self._futures):
                self._results += future.result()
            self._exec.shutdown(wait=True)
        return self._results
