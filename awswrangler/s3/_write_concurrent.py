"""Amazon S3 Concurrent Write Module (PRIVATE)."""

import concurrent.futures
import logging
from typing import Any, Callable, Dict, List, Optional

import boto3  # type: ignore
import pandas as pd  # type: ignore

from awswrangler import _utils

_logger: logging.Logger = logging.getLogger(__name__)


class _WriteProxy:
    def __init__(self, use_threads: bool):
        self._exec: Optional[concurrent.futures.ThreadPoolExecutor]
        self._results: List[Any] = []
        cpus: int = _utils.ensure_cpu_count(use_threads=use_threads)
        if cpus > 1:
            self._exec = concurrent.futures.ThreadPoolExecutor(max_workers=cpus)
            self._futures: List[concurrent.futures.Future] = []
        else:
            self._exec = None

    @staticmethod
    def _caller(
        func: Callable, boto3_primitives: _utils.Boto3PrimitivesType, func_kwargs: Dict[str, Any]
    ) -> pd.DataFrame:
        boto3_session: boto3.Session = _utils.boto3_from_primitives(primitives=boto3_primitives)
        func_kwargs["boto3_session"] = boto3_session
        _logger.debug("Calling: %s", func)
        return func(**func_kwargs)

    def write(self, func: Callable, boto3_session: boto3.Session, **func_kwargs) -> None:
        """Write File."""
        if self._exec is not None:
            _logger.debug("Submitting: %s", func)
            future = self._exec.submit(
                _WriteProxy._caller,
                func=func,
                boto3_primitives=_utils.boto3_to_primitives(boto3_session=boto3_session),
                func_kwargs=func_kwargs,
            )
            self._futures.append(future)
        else:
            self._results.append(func(boto3_session=boto3_session, **func_kwargs))

    def close(self):
        """Close the proxy."""
        if self._exec is not None:
            for future in concurrent.futures.as_completed(self._futures):
                self._results.append(future.result())
            self._exec.shutdown(wait=True)
        return self._results
