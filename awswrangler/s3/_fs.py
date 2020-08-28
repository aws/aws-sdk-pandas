"""Amazon S3 filesystem abstraction layer (PRIVATE)."""

import io
import logging
import socket
from typing import Any, BinaryIO, Dict, List, Optional, Tuple, Union, cast

import boto3
from botocore.exceptions import ClientError

from awswrangler import _utils, exceptions
from awswrangler.s3._describe import size_objects

_logger: logging.Logger = logging.getLogger(__name__)

_S3_RETRYABLE_ERRORS: Tuple[Any, Any] = (socket.timeout, ConnectionError)


class S3Object:  # pylint: disable=too-many-instance-attributes
    """Class to abstract S3 objects as ordinary files."""

    def __init__(
        self,
        path: str,
        block_size: int,
        mode: Optional[str] = "rb",
        newline: Optional[str] = "\n",
        s3_additional_kwargs: Optional[Dict[str, str]] = None,
        boto3_session: Optional[boto3.Session] = None,
        encoding: Optional[str] = "utf-8",
    ) -> None:
        self._bucket, self._key = _utils.parse_path(path=path)
        self._boto3_session: boto3.Session = _utils.ensure_session(session=boto3_session)
        size: Optional[int] = size_objects(path=[path], use_threads=False, boto3_session=self._boto3_session)[path]
        if mode not in {"rb", "wb", "r", "w"}:
            raise NotImplementedError("File mode must be {'rb', 'wb', 'r', 'w'}, not %s" % mode)
        self._mode: str = "rb" if mode is None else mode
        self._block_size: int = block_size
        self._newline: str = "\n" if newline is None else newline
        self._s3_additional_kwargs: Dict[str, str] = {} if s3_additional_kwargs is None else s3_additional_kwargs
        self._client: boto3.client = _utils.client(service_name="s3", session=self._boto3_session)
        self._encoding: str = "utf-8" if encoding is None else encoding
        self._cache: bytes = b""
        self._start: int = 0
        self._end: int = 0
        self._loc: int = 0
        self._text_wrapper: Optional[io.TextIOWrapper] = None
        self._is_context_manager: bool = False
        self.closed: bool = False
        if self.readable() is True:
            if size is None:
                raise exceptions.InvalidArgumentValue(f"S3 object w/o defined size: {path}")
            self._size: int = size
            _logger.debug("self._size: %s", self._size)

    def __enter__(self) -> Union["S3Object", io.TextIOWrapper]:
        """Create the context."""
        self._is_context_manager = True
        if "b" not in self._mode:
            self._text_wrapper = io.TextIOWrapper(cast(BinaryIO, self), encoding=self._encoding, newline=self._newline)
            return self._text_wrapper
        return self

    def __exit__(self, exc_type: Any, exc_value: Any, exc_traceback: Any) -> None:
        """Close the context."""
        _logger.debug("exc_type: %s", exc_type)
        _logger.debug("exc_value: %s", exc_value)
        _logger.debug("exc_traceback: %s", exc_traceback)
        self.close()

    def __del__(self) -> None:
        """Delete object tear down."""
        self.close()

    def __next__(self) -> Union[bytes, str]:
        """Next line."""
        out: Union[bytes, str, None] = self.readline()
        if not out:
            raise StopIteration
        return out

    next = __next__

    def __iter__(self) -> "S3Object":
        """Iterate over lines."""
        return self

    def _fetch_range(self, start: int, end: int) -> bytes:
        _logger.debug("Fetching: s3://%s/%s - Range: %s-%s", self._bucket, self._key, start, end)
        try:
            resp: Dict[str, Any] = _utils.try_it(
                f=self._client.get_object,
                ex=_S3_RETRYABLE_ERRORS,
                base=0.5,
                max_num_tries=6,
                Bucket=self._bucket,
                Key=self._key,
                Range=f"bytes={start}-{end-1}",
            )
        except ClientError as ex:
            if ex.response["Error"].get("Code", "Unknown") in ("416", "InvalidRange"):
                return b""
            raise ex
        return cast(bytes, resp["Body"].read())

    def _fetch(self, start: int, end: int) -> None:
        if (end - start) < self._block_size:
            end = start + self._block_size
        if end > self._size:
            end = self._size

        if start < self._start or end > self._end:
            self._start = start
            self._end = end
            self._cache = self._fetch_range(self._start, self._end)

    def read(self, length: int = -1) -> Union[bytes, str]:
        """Return cached data and fetch on demand chunks."""
        if self._is_context_manager is False:
            raise RuntimeError(
                "Directly usage forbidden. "
                "Please only use S3Object inside the context manager (i.e. WITH statement)."
            )
        _logger.debug("length: %s", length)
        if self.readable() is False:
            raise ValueError("File not in read mode.")
        if length < 0:
            length = self._size - self._loc
        if self.closed is True:
            raise ValueError("I/O operation on closed file.")

        self._fetch(self._loc, self._loc + length)
        out: bytes = self._cache[self._loc - self._start : self._loc - self._start + length]
        self._loc += len(out)

        # Left Trim
        self._cache = self._cache[self._loc - self._start :]
        self._start = self._loc

        return out

    def readline(self, length: int = -1) -> Union[bytes, str]:
        """Read until the next line terminator."""
        self._fetch(self._loc, self._loc + self._block_size)
        while True:
            found: int = self._cache[self._loc - self._start :].find(self._newline.encode(encoding="utf-8"))

            if 0 < length < found:
                return self.read(length + 1)
            if found >= 0:
                return self.read(found + 1)
            if self._end >= self._size:
                return self.read(length)

            self._fetch(self._loc, self._end + self._block_size)

    def readlines(self) -> List[Union[bytes, str]]:
        """Return all lines as list."""
        return list(self)

    def tell(self) -> int:
        """Return the current file location."""
        return self._loc

    def seek(self, loc: int, whence: int = 0) -> int:
        """Set current file location."""
        if self.readable() is False:
            raise ValueError("Seek only available in read mode")
        if whence == 0:
            loc_tmp: int = loc
        elif whence == 1:
            loc_tmp = self._loc + loc
        elif whence == 2:
            loc_tmp = self._size + loc
        else:
            raise ValueError(f"invalid whence ({whence}, should be 0, 1 or 2).")
        if loc_tmp < 0:
            raise ValueError("Seek before start of file")
        self._loc = loc_tmp
        return self._loc

    def flush(self, force: bool = False, retries: int = 10) -> None:
        """Write buffered data to S3."""

    def readable(self) -> bool:
        """Return whether this object is opened for reading."""
        return "r" in self._mode

    def seekable(self) -> bool:
        """Return whether this object is opened for seeking."""
        return self.readable()

    def writable(self) -> bool:
        """Return whether this object is opened for writing."""
        return "w" in self._mode

    def close(self) -> None:
        """Clean up the cache."""
        self._cache = b""
        self.closed = True
