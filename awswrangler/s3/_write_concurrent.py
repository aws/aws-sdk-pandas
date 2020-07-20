"""Amazon S3 CONCURRENT Read Module (PRIVATE)."""

import concurrent.futures
import logging
from contextlib import contextmanager
from typing import Any, Callable, Dict, Iterator, List, Optional

import boto3  # type: ignore
import pandas as pd  # type: ignore

from awswrangler import _utils

_logger: logging.Logger = logging.getLogger(__name__)


def _caller(func: Callable, boto3_primitives: _utils.Boto3PrimitivesType, func_kwargs: Dict[str, Any]) -> pd.DataFrame:
    boto3_session: boto3.Session = _utils.boto3_from_primitives(primitives=boto3_primitives)
    func_kwargs["boto3_session"] = boto3_session
    _logger.debug("Calling: %s", func)
    return func(**func_kwargs)


class _WriteProxy:
    def __init__(self, use_threads: bool):
        self._use_threads: bool = use_threads
        self._exec: Optional[concurrent.futures.ThreadPoolExecutor]
        if self._use_threads is True:
            cpus: Optional[int] = _utils.ensure_cpu_count(use_threads=True)
            self._exec = concurrent.futures.ThreadPoolExecutor(max_workers=cpus)
            self._futures: List[concurrent.futures.Future] = []
        else:
            self._exec = None

    def write(self, func: Callable, boto3_session: boto3.Session, **func_kwargs) -> None:
        """Write File."""
        if self._exec is not None:
            _logger.debug("self._exec: %s", self._exec)
            _logger.debug("Submitting: %s", func)
            future = self._exec.submit(
                fn=_caller,
                func=func,
                boto3_primitives=_utils.boto3_to_primitives(boto3_session=boto3_session),
                func_kwargs=func_kwargs,
            )
            self._futures.append(future)
        else:
            func(boto3_session=boto3_session, **func_kwargs)

    def close(self):
        """Close the proxy."""
        if self._exec is not None:
            for future in concurrent.futures.as_completed(self._futures):
                future.result()
            self._exec.shutdown(wait=True)


@contextmanager
def _write_proxy(use_threads: bool) -> Iterator[_WriteProxy]:
    proxy: _WriteProxy = _WriteProxy(use_threads=use_threads)
    yield proxy
    proxy.close()
