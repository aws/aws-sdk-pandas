"""Amazon S3 filesystem abstraction layer (PRIVATE)."""

import concurrent.futures
import io
import itertools
import logging
import math
import socket
from contextlib import contextmanager
from typing import Any, BinaryIO, Dict, Iterator, List, Optional, Set, Tuple, Union, cast

import boto3
from botocore.exceptions import ReadTimeoutError

from awswrangler import _utils, exceptions
from awswrangler._config import apply_configs
from awswrangler.s3._describe import size_objects

_logger: logging.Logger = logging.getLogger(__name__)

_S3_RETRYABLE_ERRORS: Tuple[Any, Any, Any] = (socket.timeout, ConnectionError, ReadTimeoutError)

_MIN_WRITE_BLOCK: int = 5_242_880  # 5 MB (5 * 2**20)
_MIN_PARALLEL_READ_BLOCK: int = 5_242_880  # 5 MB (5 * 2**20)

BOTOCORE_ACCEPTED_KWARGS: Dict[str, Set[str]] = {
    "get_object": {"SSECustomerAlgorithm", "SSECustomerKey"},
    "copy_object": {
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


def get_botocore_valid_kwargs(function_name: str, s3_additional_kwargs: Dict[str, Any]) -> Dict[str, Any]:
    """Filter and keep only the valid botocore key arguments."""
    return {k: v for k, v in s3_additional_kwargs.items() if k in BOTOCORE_ACCEPTED_KWARGS[function_name]}


def _fetch_range(
    range_values: Tuple[int, int],
    bucket: str,
    key: str,
    boto3_primitives: _utils.Boto3PrimitivesType,
    boto3_kwargs: Dict[str, Any],
) -> Tuple[int, bytes]:
    start, end = range_values
    _logger.debug("Fetching: s3://%s/%s - Range: %s-%s", bucket, key, start, end)
    boto3_session: boto3.Session = _utils.boto3_from_primitives(primitives=boto3_primitives)
    client: boto3.client = _utils.client(service_name="s3", session=boto3_session)
    resp: Dict[str, Any] = _utils.try_it(
        f=client.get_object,
        ex=_S3_RETRYABLE_ERRORS,
        base=0.5,
        max_num_tries=6,
        Bucket=bucket,
        Key=key,
        Range=f"bytes={start}-{end - 1}",
        **boto3_kwargs,
    )
    return start, cast(bytes, resp["Body"].read())


class _UploadProxy:
    def __init__(self, use_threads: bool):
        self.closed = False
        self._exec: Optional[concurrent.futures.ThreadPoolExecutor]
        self._results: List[Dict[str, Union[str, int]]] = []
        self._cpus: int = _utils.ensure_cpu_count(use_threads=use_threads)
        if self._cpus > 1:
            self._exec = concurrent.futures.ThreadPoolExecutor(max_workers=self._cpus)
            self._futures: List[Any] = []
        else:
            self._exec = None

    @staticmethod
    def _sort_by_part_number(parts: List[Dict[str, Union[str, int]]]) -> List[Dict[str, Union[str, int]]]:
        return sorted(parts, key=lambda k: k["PartNumber"])

    @staticmethod
    def _caller(
        bucket: str,
        key: str,
        part: int,
        upload_id: str,
        data: bytes,
        boto3_primitives: _utils.Boto3PrimitivesType,
        boto3_kwargs: Dict[str, Any],
    ) -> Dict[str, Union[str, int]]:
        _logger.debug("Upload part %s started.", part)
        boto3_session: boto3.Session = _utils.boto3_from_primitives(primitives=boto3_primitives)
        client: boto3.client = _utils.client(service_name="s3", session=boto3_session)
        resp: Dict[str, Any] = _utils.try_it(
            f=client.upload_part,
            ex=_S3_RETRYABLE_ERRORS,
            base=0.5,
            max_num_tries=6,
            Bucket=bucket,
            Key=key,
            Body=data,
            PartNumber=part,
            UploadId=upload_id,
            **boto3_kwargs,
        )
        _logger.debug("Upload part %s done.", part)
        return {"PartNumber": part, "ETag": resp["ETag"]}

    def upload(
        self,
        bucket: str,
        key: str,
        part: int,
        upload_id: str,
        data: bytes,
        boto3_session: boto3.Session,
        boto3_kwargs: Dict[str, Any],
    ) -> None:
        """Upload Part."""
        if self._exec is not None:
            _utils.block_waiting_available_thread(seq=self._futures, max_workers=self._cpus)
            future = self._exec.submit(
                _UploadProxy._caller,
                bucket=bucket,
                key=key,
                part=part,
                upload_id=upload_id,
                data=data,
                boto3_primitives=_utils.boto3_to_primitives(boto3_session=boto3_session),
                boto3_kwargs=boto3_kwargs,
            )
            self._futures.append(future)
        else:
            self._results.append(
                self._caller(
                    bucket=bucket,
                    key=key,
                    part=part,
                    upload_id=upload_id,
                    data=data,
                    boto3_primitives=_utils.boto3_to_primitives(boto3_session=boto3_session),
                    boto3_kwargs=boto3_kwargs,
                )
            )

    def close(self) -> List[Dict[str, Union[str, int]]]:
        """Close the proxy."""
        if self.closed is True:
            return []
        if self._exec is not None:
            for future in concurrent.futures.as_completed(self._futures):
                self._results.append(future.result())
            self._exec.shutdown(wait=True)
        self.closed = True
        return self._sort_by_part_number(parts=self._results)


class _S3Object:  # pylint: disable=too-many-instance-attributes
    """Class to abstract S3 objects as ordinary files."""

    def __init__(
        self,
        path: str,
        s3_block_size: int,
        mode: str,
        use_threads: bool,
        s3_additional_kwargs: Optional[Dict[str, str]],
        boto3_session: Optional[boto3.Session],
        newline: Optional[str],
        encoding: Optional[str],
    ) -> None:
        self.closed: bool = False
        self._use_threads = use_threads
        self._newline: str = "\n" if newline is None else newline
        self._encoding: str = "utf-8" if encoding is None else encoding
        self._bucket, self._key = _utils.parse_path(path=path)
        self._boto3_session: boto3.Session = _utils.ensure_session(session=boto3_session)
        if mode not in {"rb", "wb", "r", "w"}:
            raise NotImplementedError("File mode must be {'rb', 'wb', 'r', 'w'}, not %s" % mode)
        self._mode: str = "rb" if mode is None else mode
        self._one_shot_download: bool = False
        if 0 < s3_block_size < 3:
            raise exceptions.InvalidArgumentValue(
                "s3_block_size MUST > 2 to define a valid size or "
                "< 1 to avoid blocks and always execute one shot downloads."
            )
        if s3_block_size <= 0:
            _logger.debug("s3_block_size of %d, enabling one_shot_download.", s3_block_size)
            self._one_shot_download = True
        self._s3_block_size: int = s3_block_size
        self._s3_half_block_size: int = s3_block_size // 2
        self._s3_additional_kwargs: Dict[str, str] = {} if s3_additional_kwargs is None else s3_additional_kwargs
        self._client: boto3.client = _utils.client(service_name="s3", session=self._boto3_session)
        self._loc: int = 0

        if self.readable() is True:
            self._cache: bytes = b""
            self._start: int = 0
            self._end: int = 0
            size: Optional[int] = size_objects(path=[path], use_threads=False, boto3_session=self._boto3_session)[path]
            if size is None:
                raise exceptions.InvalidArgumentValue(f"S3 object w/o defined size: {path}")
            self._size: int = size
            _logger.debug("self._size: %s", self._size)
            _logger.debug("self._s3_block_size: %s", self._s3_block_size)
        elif self.writable() is True:
            self._mpu: Dict[str, Any] = {}
            self._buffer: io.BytesIO = io.BytesIO()
            self._parts_count: int = 0
            self._size = 0
            self._upload_proxy: _UploadProxy = _UploadProxy(use_threads=self._use_threads)
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

    @staticmethod
    def _merge_range(ranges: List[Tuple[int, bytes]]) -> bytes:
        return b"".join(data for start, data in sorted(ranges, key=lambda r: r[0]))

    def _fetch_range_proxy(self, start: int, end: int) -> bytes:
        _logger.debug("Fetching: s3://%s/%s - Range: %s-%s", self._bucket, self._key, start, end)
        boto3_primitives: _utils.Boto3PrimitivesType = _utils.boto3_to_primitives(boto3_session=self._boto3_session)
        boto3_kwargs: Dict[str, Any] = get_botocore_valid_kwargs(
            function_name="get_object", s3_additional_kwargs=self._s3_additional_kwargs
        )
        cpus: int = _utils.ensure_cpu_count(use_threads=self._use_threads)
        range_size: int = end - start
        if cpus < 2 or range_size < (2 * _MIN_PARALLEL_READ_BLOCK):
            return _fetch_range(
                range_values=(start, end),
                bucket=self._bucket,
                key=self._key,
                boto3_primitives=boto3_primitives,
                boto3_kwargs=boto3_kwargs,
            )[1]
        sizes: Tuple[int, ...] = _utils.get_even_chunks_sizes(
            total_size=range_size, chunk_size=_MIN_PARALLEL_READ_BLOCK, upper_bound=False
        )
        ranges: List[Tuple[int, int]] = []
        chunk_start: int = start
        for size in sizes:
            ranges.append((chunk_start, chunk_start + size))
            chunk_start += size
        with concurrent.futures.ThreadPoolExecutor(max_workers=cpus) as executor:
            return self._merge_range(
                ranges=list(
                    executor.map(
                        _fetch_range,
                        ranges,
                        itertools.repeat(self._bucket),
                        itertools.repeat(self._key),
                        itertools.repeat(boto3_primitives),
                        itertools.repeat(boto3_kwargs),
                    )
                ),
            )

    def _fetch(self, start: int, end: int) -> None:
        if end > self._size:
            raise ValueError(f"Trying to fetch byte (at position {end - 1}) beyond file size ({self._size})")
        if start < 0:
            raise ValueError(f"Trying to fetch byte (at position {start}) beyond file range ({self._size})")

        if start >= self._start and end <= self._end:
            return None  # Does not require download

        if self._one_shot_download:
            self._start = 0
            self._end = self._size
            self._cache = self._fetch_range_proxy(self._start, self._end)
            return None

        if end - start >= self._s3_block_size:  # Fetching length greater than cache length
            self._cache = self._fetch_range_proxy(start, end)
            self._start = start
            self._end = end
            return None

        # Calculating block START and END positions
        _logger.debug("Downloading: %s (start) / %s (end)", start, end)
        mid: int = int(math.ceil((start + (end - 1)) / 2))
        new_block_start: int = mid - self._s3_half_block_size
        new_block_start = new_block_start + 1 if self._s3_block_size % 2 == 0 else new_block_start
        new_block_end: int = mid + self._s3_half_block_size + 1
        _logger.debug("new_block_start: %s / new_block_end: %s / mid: %s", new_block_start, new_block_end, mid)
        if new_block_start < 0 and new_block_end > self._size:  # both ends overflowing
            new_block_start = 0
            new_block_end = self._size
        elif new_block_end > self._size:  # right overflow
            new_block_start = new_block_start - (new_block_end - self._size)
            new_block_start = 0 if new_block_start < 0 else new_block_start
            new_block_end = self._size
        elif new_block_start < 0:  # left overflow
            new_block_end = new_block_end - new_block_start
            new_block_end = self._size if new_block_end > self._size else new_block_end
            new_block_start = 0
        _logger.debug(
            "new_block_start: %s / new_block_end: %s/ self._start: %s / self._end: %s",
            new_block_start,
            new_block_end,
            self._start,
            self._end,
        )

        # Calculating missing bytes in cache
        if (  # Full block download
            (new_block_start < self._start and new_block_end > self._end)
            or new_block_start > self._end
            or new_block_end < self._start
        ):
            self._cache = self._fetch_range_proxy(new_block_start, new_block_end)
        elif new_block_end > self._end:
            prune_diff: int = new_block_start - self._start
            self._cache = self._cache[prune_diff:] + self._fetch_range_proxy(self._end, new_block_end)
        elif new_block_start < self._start:
            prune_diff = new_block_end - self._end
            self._cache = self._fetch_range_proxy(new_block_start, self._start) + self._cache[:prune_diff]
        else:
            raise RuntimeError("Wrangler's cache calculation error.")
        self._start = new_block_start
        self._end = new_block_end

        return None

    def read(self, length: int = -1) -> Union[bytes, str]:
        """Return cached data and fetch on demand chunks."""
        _logger.debug("Reading: %s bytes at %s", length, self._loc)
        if self.readable() is False:
            raise ValueError("File not in read mode.")
        if self.closed is True:
            raise ValueError("I/O operation on closed file.")
        if length < 0 or self._loc + length > self._size:
            length = self._size - self._loc

        self._fetch(self._loc, self._loc + length)
        out: bytes = self._cache[self._loc - self._start : self._loc - self._start + length]
        self._loc += len(out)
        return out

    def readline(self, length: int = -1) -> Union[bytes, str]:
        """Read until the next line terminator."""
        end: int = self._loc + self._s3_block_size
        end = self._size if end > self._size else end
        self._fetch(self._loc, end)
        while True:
            found: int = self._cache[self._loc - self._start :].find(self._newline.encode(encoding=self._encoding))

            if 0 < length < found:
                return self.read(length + 1)
            if found >= 0:
                return self.read(found + 1)
            if self._end >= self._size:
                return self.read(-1)

            end = self._end + self._s3_half_block_size
            end = self._size if end > self._size else end
            self._fetch(self._loc, end)

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
        """Write data to buffer and only upload on close() or if buffer is greater than or equal to _MIN_WRITE_BLOCK."""
        if self.writable() is False:
            raise RuntimeError("File not in write mode.")
        if self.closed:
            raise RuntimeError("I/O operation on closed file.")
        n: int = self._buffer.write(data)
        self._loc += n
        if self._buffer.tell() >= _MIN_WRITE_BLOCK:
            self.flush()
        return n

    def flush(self, force: bool = False) -> None:
        """Write buffered data to S3."""
        if self.closed:
            raise RuntimeError("I/O operation on closed file.")
        if self.writable():
            total_size: int = self._buffer.tell()
            if total_size < _MIN_WRITE_BLOCK and force is False:
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
                **get_botocore_valid_kwargs(
                    function_name="create_multipart_upload", s3_additional_kwargs=self._s3_additional_kwargs
                ),
            )
            self._buffer.seek(0)
            for chunk_size in _utils.get_even_chunks_sizes(
                total_size=total_size, chunk_size=_MIN_WRITE_BLOCK, upper_bound=False
            ):
                _logger.debug("chunk_size: %s bytes", chunk_size)
                self._parts_count += 1
                self._upload_proxy.upload(
                    bucket=self._bucket,
                    key=self._key,
                    part=self._parts_count,
                    upload_id=self._mpu["UploadId"],
                    data=self._buffer.read(chunk_size),
                    boto3_session=self._boto3_session,
                    boto3_kwargs=get_botocore_valid_kwargs(
                        function_name="upload_part", s3_additional_kwargs=self._s3_additional_kwargs
                    ),
                )
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
            _logger.debug("Closing: %s parts", self._parts_count)
            _logger.debug("Buffer tell: %s", self._buffer.tell())
            if self._parts_count > 0:
                self.flush(force=True)
                pasts: List[Dict[str, Union[str, int]]] = self._upload_proxy.close()
                part_info: Dict[str, List[Dict[str, Any]]] = {"Parts": pasts}
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
                    **get_botocore_valid_kwargs(
                        function_name="complete_multipart_upload", s3_additional_kwargs=self._s3_additional_kwargs
                    ),
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
                    **get_botocore_valid_kwargs(
                        function_name="put_object", s3_additional_kwargs=self._s3_additional_kwargs
                    ),
                )
            self._parts_count = 0
            self._buffer.seek(0)
            self._buffer.truncate(0)
            self._upload_proxy.close()
        elif self.readable():
            self._cache = b""
        else:
            raise RuntimeError(f"Invalid mode: {self._mode}")
        self.closed = True
        return None


@contextmanager
@apply_configs
def open_s3_object(
    path: str,
    mode: str,
    use_threads: bool = False,
    s3_additional_kwargs: Optional[Dict[str, str]] = None,
    s3_block_size: int = -1,  # One shot download
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
            s3_block_size=s3_block_size,
            mode=mode,
            use_threads=use_threads,
            s3_additional_kwargs=s3_additional_kwargs,
            boto3_session=boto3_session,
            encoding=encoding,
            newline=newline,
        )
        if "b" in mode:  # binary
            yield s3obj
        else:  # text
            text_s3obj = io.TextIOWrapper(
                buffer=cast(BinaryIO, s3obj),
                encoding=encoding,
                newline=newline,
                line_buffering=False,
                write_through=False,
            )
            yield text_s3obj
    finally:
        if text_s3obj is not None and text_s3obj.closed is False:
            text_s3obj.close()
        if s3obj is not None and s3obj.closed is False:
            s3obj.close()
