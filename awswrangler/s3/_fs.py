"""Amazon S3 filesystem abstraction layer (PRIVATE)."""

import io
import logging
import socket
from contextlib import contextmanager
from typing import Any, BinaryIO, Dict, Iterator, List, Optional, Set, Tuple, Union, cast

import boto3
from botocore.exceptions import ClientError

from awswrangler import _utils, exceptions
from awswrangler.s3._describe import size_objects

_logger: logging.Logger = logging.getLogger(__name__)

_S3_RETRYABLE_ERRORS: Tuple[Any, Any] = (socket.timeout, ConnectionError)

_MIN_WRITE_BLOCK: int = 5_242_880

_BOTOCORE_ACCEPTED_KWARGS: Dict[str, Set[str]] = {
    "get_object": {"SSECustomerAlgorithm", "SSECustomerKey"},
    "create_multipart_upload": {
        "ACL",
        "Metadata",
        "ServerSideEncryption",
        "StorageClass",
        "SSECustomerAlgorithm",
        "SSECustomerKey",
        "SSEKMSKeyId",
        "SSEKMSEncryptionContext",
        "Tagging",
    },
    "upload_part": {"SSECustomerAlgorithm", "SSECustomerKey"},
    "complete_multipart_upload": set(),
    "put_object": {
        "ACL",
        "Metadata",
        "ServerSideEncryption",
        "StorageClass",
        "SSECustomerAlgorithm",
        "SSECustomerKey",
        "SSEKMSKeyId",
        "SSEKMSEncryptionContext",
        "Tagging",
    },
}


class _S3Object:  # pylint: disable=too-many-instance-attributes
    """Class to abstract S3 objects as ordinary files."""

    def __init__(
        self,
        path: str,
        block_size: int,
        mode: str,
        s3_additional_kwargs: Optional[Dict[str, str]],
        boto3_session: Optional[boto3.Session],
        newline: Optional[str],
        encoding: Optional[str],
    ) -> None:
        self._newline: str = "\n" if newline is None else newline
        self._encoding: str = "utf-8" if encoding is None else encoding
        self._bucket, self._key = _utils.parse_path(path=path)
        self._boto3_session: boto3.Session = _utils.ensure_session(session=boto3_session)
        if mode not in {"rb", "wb", "r", "w"}:
            raise NotImplementedError("File mode must be {'rb', 'wb', 'r', 'w'}, not %s" % mode)
        self._mode: str = "rb" if mode is None else mode
        self._block_size: int = block_size
        self._s3_additional_kwargs: Dict[str, str] = {} if s3_additional_kwargs is None else s3_additional_kwargs
        self._client: boto3.client = _utils.client(service_name="s3", session=self._boto3_session)
        self._loc: int = 0
        self.closed: bool = False

        if self.readable() is True:
            self._cache: bytes = b""
            self._start: int = 0
            self._end: int = 0
            size: Optional[int] = size_objects(path=[path], use_threads=False, boto3_session=self._boto3_session)[path]
            if size is None:
                raise exceptions.InvalidArgumentValue(f"S3 object w/o defined size: {path}")
            self._size: int = size
            _logger.debug("self._size: %s", self._size)
        elif self.writable() is True:
            self._mpu: Dict[str, Any] = {}
            self._buffer: io.BytesIO = io.BytesIO()
            self._parts: List[Dict[str, Any]] = []
            self._size = 0
            if self._block_size < _MIN_WRITE_BLOCK:
                raise ValueError("Block size must be >=5MB for writing.")
        else:
            raise RuntimeError(f"Invalid mode: {self._mode}")

    def __enter__(self) -> Union["_S3Object", io.TextIOWrapper]:
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

    def __iter__(self) -> "_S3Object":
        """Iterate over lines."""
        return self

    def _get_botocore_valid_kwargs(self, function_name: str) -> Dict[str, Any]:
        return {k: v for k, v in self._s3_additional_kwargs.items() if k in _BOTOCORE_ACCEPTED_KWARGS[function_name]}

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
                **self._get_botocore_valid_kwargs(function_name="get_object"),
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
            found: int = self._cache[self._loc - self._start :].find(self._newline.encode(encoding=self._encoding))

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

    def write(self, data: bytes) -> int:
        """Write data to buffer and only upload on close() or if buffer is greater than or equal to block_size."""
        if self.writable() is False:
            raise RuntimeError("File not in write mode.")
        if self.closed:
            raise RuntimeError("I/O operation on closed file.")
        n: int = self._buffer.write(data)
        self._loc += n
        _logger.debug("Writing: %s bytes", n)
        if self._buffer.tell() >= self._block_size:
            self.flush()
        return n

    def flush(self, force: bool = False) -> None:
        """Write buffered data to S3."""
        if self.closed:
            raise RuntimeError("I/O operation on closed file.")
        if self.writable():
            total_size: int = self._buffer.tell()
            if total_size < self._block_size and force is False:
                return None
            if total_size == 0:
                return None
            _logger.debug("Flushing: %s bytes", total_size)
            self._mpu = self._mpu or _utils.try_it(
                f=self._client.create_multipart_upload,
                ex=_S3_RETRYABLE_ERRORS,
                base=0.5,
                max_num_tries=6,
                Bucket=self._bucket,
                Key=self._key,
                **self._get_botocore_valid_kwargs(function_name="create_multipart_upload"),
            )
            self._buffer.seek(0)
            for chunk_size in _utils.get_even_chunks_sizes(
                total_size=total_size, chunk_size=self._block_size, upper_bound=False
            ):
                _logger.debug("chunk_size: %s bytes", chunk_size)
                part: int = len(self._parts) + 1
                resp: Dict[str, Any] = _utils.try_it(
                    f=self._client.upload_part,
                    ex=_S3_RETRYABLE_ERRORS,
                    base=0.5,
                    max_num_tries=6,
                    Bucket=self._bucket,
                    Key=self._key,
                    Body=self._buffer.read(chunk_size),
                    PartNumber=part,
                    UploadId=self._mpu["UploadId"],
                    **self._get_botocore_valid_kwargs(function_name="upload_part"),
                )
                self._parts.append({"PartNumber": part, "ETag": resp["ETag"]})
            self._buffer = io.BytesIO()
        return None

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
        if self.closed:
            return None
        if self.writable():
            _logger.debug("Closing: %s parts", len(self._parts))
            _logger.debug("Buffer tell: %s", self._buffer.tell())
            if self._parts:
                self.flush(force=True)
                part_info: Dict[str, List[Dict[str, Any]]] = {"Parts": self._parts}
                _logger.debug("complete_multipart_upload")
                _utils.try_it(
                    f=self._client.complete_multipart_upload,
                    ex=_S3_RETRYABLE_ERRORS,
                    base=0.5,
                    max_num_tries=6,
                    Bucket=self._bucket,
                    Key=self._key,
                    UploadId=self._mpu["UploadId"],
                    MultipartUpload=part_info,
                    **self._get_botocore_valid_kwargs(function_name="complete_multipart_upload"),
                )
            elif self._buffer.tell() > 0:
                _logger.debug("put_object")
                _utils.try_it(
                    f=self._client.put_object,
                    ex=_S3_RETRYABLE_ERRORS,
                    base=0.5,
                    max_num_tries=6,
                    Bucket=self._bucket,
                    Key=self._key,
                    Body=self._buffer.getvalue(),
                    **self._get_botocore_valid_kwargs(function_name="put_object"),
                )
            self._parts = []
            self._buffer.seek(0)
            self._buffer.truncate(0)
        elif self.readable():
            self._cache = b""
        else:
            raise RuntimeError(f"Invalid mode: {self._mode}")
        self.closed = True
        return None


@contextmanager
def open_s3_object(
    path: str,
    block_size: int,
    mode: str,
    s3_additional_kwargs: Optional[Dict[str, str]] = None,
    boto3_session: Optional[boto3.Session] = None,
    newline: Optional[str] = "\n",
    encoding: Optional[str] = "utf-8",
) -> Iterator[Union[_S3Object, io.TextIOWrapper]]:
    """Return a _S3Object or TextIOWrapper based in the received mode."""
    s3obj: Optional[_S3Object] = None
    text_s3obj: Optional[io.TextIOWrapper] = None
    try:
        s3obj = _S3Object(
            path=path,
            block_size=block_size,
            mode=mode,
            s3_additional_kwargs=s3_additional_kwargs,
            boto3_session=boto3_session,
            encoding=encoding,
            newline=newline,
        )
        if "b" in mode:  # binary
            yield s3obj
        else:  # text
            text_s3obj = io.TextIOWrapper(cast(BinaryIO, s3obj), encoding=encoding, newline=newline)
            yield text_s3obj
    finally:
        if text_s3obj is not None and text_s3obj.closed is False:
            text_s3obj.close()
        if s3obj is not None and s3obj.closed is False:
            s3obj.close()
