"""Amazon S3 Select Module (PRIVATE)."""

import concurrent.futures
import itertools
import json
import logging
import pprint
from typing import Any, Dict, Iterator, List, Optional, Tuple, Union

import boto3
import pandas as pd

from awswrangler import _utils, exceptions
from awswrangler.s3._describe import size_objects

_logger: logging.Logger = logging.getLogger(__name__)

_RANGE_CHUNK_SIZE: int = int(1024 * 1024)


def _gen_scan_range(obj_size: int) -> Iterator[Tuple[int, int]]:
    for i in range(0, obj_size, _RANGE_CHUNK_SIZE):
        yield (i, i + min(_RANGE_CHUNK_SIZE, obj_size - i))


def _select_object_content(
    args: Dict[str, Any],
    boto3_session: Optional[boto3.Session],
    scan_range: Optional[Tuple[int, int]] = None,
) -> List[Dict[str, Any]]:
    client_s3: boto3.client = _utils.client(service_name="s3", session=boto3_session)

    if scan_range:
        response = client_s3.select_object_content(**args, ScanRange={"Start": scan_range[0], "End": scan_range[1]})
    else:
        response = client_s3.select_object_content(**args)

    payload_records = []
    partial_record: str = ""
    for event in response["Payload"]:
        if "Records" in event:
            records = event["Records"]["Payload"].decode(encoding="utf-8", errors="ignore").split("\n")
            records[0] = partial_record + records[0]
            # Record end can either be a partial record or a return char
            partial_record = records.pop()
            payload_records.extend([json.loads(record) for record in records])
    return payload_records


def _paginate_stream(
    args: Dict[str, Any], path: str, use_threads: Union[bool, int], boto3_session: Optional[boto3.Session]
) -> pd.DataFrame:
    obj_size: int = size_objects(  # type: ignore
        path=[path],
        use_threads=False,
        boto3_session=boto3_session,
    ).get(path)
    if obj_size is None:
        raise exceptions.InvalidArgumentValue(f"S3 object w/o defined size: {path}")
    scan_ranges = _gen_scan_range(obj_size=obj_size)

    if use_threads is False:
        stream_records = list(
            _select_object_content(
                args=args,
                boto3_session=boto3_session,
                scan_range=scan_range,
            )
            for scan_range in scan_ranges
        )
    else:
        cpus: int = _utils.ensure_cpu_count(use_threads=use_threads)
        with concurrent.futures.ThreadPoolExecutor(max_workers=cpus) as executor:
            stream_records = list(
                executor.map(
                    _select_object_content,
                    itertools.repeat(args),
                    itertools.repeat(boto3_session),
                    scan_ranges,
                )
            )
    return pd.DataFrame([item for sublist in stream_records for item in sublist])  # Flatten list of lists


def select_query(
    sql: str,
    path: str,
    input_serialization: str,
    input_serialization_params: Dict[str, Union[bool, str]],
    compression: Optional[str] = None,
    use_threads: Union[bool, int] = False,
    boto3_session: Optional[boto3.Session] = None,
    s3_additional_kwargs: Optional[Dict[str, Any]] = None,
) -> pd.DataFrame:
    r"""Filter contents of an Amazon S3 object based on SQL statement.

    Note: Scan ranges are only supported for uncompressed CSV/JSON, CSV (without quoted delimiters)
    and JSON objects (in LINES mode only). It means scanning cannot be split across threads if the latter
    conditions are not met, leading to lower performance.

    Parameters
    ----------
    sql: str
        SQL statement used to query the object.
    path: str
        S3 path to the object (e.g. s3://bucket/key).
    input_serialization: str,
        Format of the S3 object queried.
        Valid values: "CSV", "JSON", or "Parquet". Case sensitive.
    input_serialization_params: Dict[str, Union[bool, str]]
        Dictionary describing the serialization of the S3 object.
    compression: Optional[str]
        Compression type of the S3 object.
        Valid values: None, "gzip", or "bzip2". gzip and bzip2 are only valid for CSV and JSON objects.
    use_threads : Union[bool, int]
        True to enable concurrent requests, False to disable multiple threads.
        If enabled os.cpu_count() is used as the max number of threads.
        If integer is provided, specified number is used.
    boto3_session : boto3.Session(), optional
        Boto3 Session. The default boto3 session is used if none is provided.
    s3_additional_kwargs : Optional[Dict[str, Any]]
        Forwarded to botocore requests.
        Valid values: "SSECustomerAlgorithm", "SSECustomerKey", "ExpectedBucketOwner".
        e.g. s3_additional_kwargs={'SSECustomerAlgorithm': 'md5'}

    Returns
    -------
    pandas.DataFrame
        Pandas DataFrame with results from query.

    Examples
    --------
    Reading a gzip compressed JSON document

    >>> import awswrangler as wr
    >>> df = wr.s3.select_query(
    ...     sql='SELECT * FROM s3object[*][*]',
    ...     path='s3://bucket/key.json.gzip',
    ...     input_serialization='JSON',
    ...     input_serialization_params={
    ...         'Type': 'Document',
    ...     },
    ...     compression="gzip",
    ... )

    Reading an entire CSV object using threads

    >>> import awswrangler as wr
    >>> df = wr.s3.select_query(
    ...     sql='SELECT * FROM s3object',
    ...     path='s3://bucket/key.csv',
    ...     input_serialization='CSV',
    ...     input_serialization_params={
    ...         'FileHeaderInfo': 'Use',
    ...         'RecordDelimiter': '\r\n'
    ...     },
    ...     use_threads=True,
    ... )

    Reading a single column from Parquet object with pushdown filter

    >>> import awswrangler as wr
    >>> df = wr.s3.select_query(
    ...     sql='SELECT s.\"id\" FROM s3object s where s.\"id\" = 1.0',
    ...     path='s3://bucket/key.snappy.parquet',
    ...     input_serialization='Parquet',
    ... )
    """
    if path.endswith("/"):
        raise exceptions.InvalidArgumentValue("<path> argument should be an S3 key, not a prefix.")
    if input_serialization not in ["CSV", "JSON", "Parquet"]:
        raise exceptions.InvalidArgumentValue("<input_serialization> argument must be 'CSV', 'JSON' or 'Parquet'")
    if compression not in [None, "gzip", "bzip2"]:
        raise exceptions.InvalidCompression(f"Invalid {compression} compression, please use None, 'gzip' or 'bzip2'.")
    if compression and (input_serialization not in ["CSV", "JSON"]):
        raise exceptions.InvalidArgumentCombination(
            "'gzip' or 'bzip2' are only valid for input 'CSV' or 'JSON' objects."
        )
    bucket, key = _utils.parse_path(path)

    args: Dict[str, Any] = {
        "Bucket": bucket,
        "Key": key,
        "Expression": sql,
        "ExpressionType": "SQL",
        "RequestProgress": {"Enabled": False},
        "InputSerialization": {
            input_serialization: input_serialization_params,
            "CompressionType": compression.upper() if compression else "NONE",
        },
        "OutputSerialization": {
            "JSON": {},
        },
    }
    if s3_additional_kwargs:
        args.update(s3_additional_kwargs)
    _logger.debug("args:\n%s", pprint.pformat(args))

    if any(
        [
            compression,
            input_serialization_params.get("AllowQuotedRecordDelimiter"),
            input_serialization_params.get("Type") == "Document",
        ]
    ):  # Scan range is only supported for uncompressed CSV/JSON, CSV (without quoted delimiters)
        # and JSON objects (in LINES mode only)
        _logger.debug("Scan ranges are not supported given provided input.")
        return pd.DataFrame(_select_object_content(args=args, boto3_session=boto3_session))
    return _paginate_stream(args=args, path=path, use_threads=use_threads, boto3_session=boto3_session)
