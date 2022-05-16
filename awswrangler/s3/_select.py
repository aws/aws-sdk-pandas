"""Amazon S3 Select Module (PRIVATE)."""

import concurrent.futures
import datetime
import importlib.util
import itertools
import json
import logging
import pprint
from typing import Any, Dict, Iterator, List, Optional, Tuple, Union

import boto3
from pyarrow import Table, concat_tables

from awswrangler import _utils, exceptions
from awswrangler._distributed import _ray_remote
from awswrangler.s3._describe import size_objects
from awswrangler.s3._list import _path2list
from awswrangler.s3._read import _get_path_ignore_suffix, _read_tables_from_multiple_paths

_ray_found = importlib.util.find_spec("ray")
if _ray_found:
    import ray

_modin_found = importlib.util.find_spec("modin")
if _modin_found:
    import modin.pandas as pd
else:
    import pandas as pd


_logger: logging.Logger = logging.getLogger(__name__)

_RANGE_CHUNK_SIZE: int = int(1024 * 1024)


def _gen_scan_range(obj_size: int, scan_range_chunk_size: Optional[int] = None) -> Iterator[Tuple[int, int]]:
    chunk_size = scan_range_chunk_size or _RANGE_CHUNK_SIZE
    for i in range(0, obj_size, chunk_size):
        yield (i, i + min(chunk_size, obj_size - i))


@_ray_remote
def _select_object_content(
    args: Dict[str, Any],
    boto3_session: Optional[boto3.Session] = None,
    scan_range: Optional[Tuple[int, int]] = None,
) -> Table:
    client_s3: boto3.client = _utils.client(service_name="s3", session=boto3_session)

    if scan_range:
        _logger.debug("scan_range: %s, key: %s", scan_range, args["Key"])
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
    return _utils.pylist_to_arrow(Table, payload_records)


@_ray_remote
def _paginate_stream(
    args: Dict[str, Any],
    path: str,
    scan_range_chunk_size: Optional[int],
    use_threads: Union[bool, int],
    boto3_session: Optional[boto3.Session],
) -> List[Union[Table, "ray.types.ObjectRef[Union[Table, bytes]]"]]:
    obj_size: int = size_objects(  # type: ignore
        path=[path],
        use_threads=False,
        boto3_session=boto3_session,
    ).get(path)
    if obj_size is None:
        raise exceptions.InvalidArgumentValue(f"S3 object w/o defined size: {path}")
    scan_ranges = _gen_scan_range(obj_size=obj_size, scan_range_chunk_size=scan_range_chunk_size)

    if _ray_found:
        return list(
            _select_object_content(
                args=args,
                scan_range=scan_range,
            )
            for scan_range in scan_ranges
        )
    if use_threads is False:
        tables = list(
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
            tables = list(
                executor.map(
                    _select_object_content,
                    itertools.repeat(args),
                    itertools.repeat(boto3_session),
                    scan_ranges,
                )
            )
    return tables


@_ray_remote
def _select_query(
    path: str,
    sql: str,
    input_serialization: str,
    input_serialization_params: Dict[str, Union[bool, str]],
    compression: Optional[str] = None,
    scan_range_chunk_size: Optional[int] = None,
    use_threads: Union[bool, int] = False,
    boto3_session: Optional[boto3.Session] = None,
    s3_additional_kwargs: Optional[Dict[str, Any]] = None,
) -> List[Union[Table, "ray.types.ObjectRef[Union[Table, bytes]]"]]:
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
        return [
            _select_object_content(args=args, boto3_session=boto3_session)
            if _ray_found
            else _select_object_content(args=args, boto3_session=boto3_session)
        ]
    return (  # type: ignore
        ray.get(
            _paginate_stream(
                args=args,
                path=path,
                scan_range_chunk_size=scan_range_chunk_size,
                use_threads=use_threads,
                boto3_session=boto3_session,
            )
        )
        if _ray_found
        else _paginate_stream(
            args=args,
            path=path,
            scan_range_chunk_size=scan_range_chunk_size,
            use_threads=use_threads,
            boto3_session=boto3_session,
        )
    )


def select_query(
    sql: str,
    path: Union[str, List[str]],
    input_serialization: str,
    input_serialization_params: Dict[str, Union[bool, str]],
    compression: Optional[str] = None,
    scan_range_chunk_size: Optional[int] = None,
    path_suffix: Union[str, List[str], None] = None,
    path_ignore_suffix: Union[str, List[str], None] = None,
    ignore_empty: bool = True,
    use_threads: Union[bool, int] = True,
    last_modified_begin: Optional[datetime.datetime] = None,
    last_modified_end: Optional[datetime.datetime] = None,
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
    path : Union[str, List[str]]
        S3 prefix (accepts Unix shell-style wildcards)
        (e.g. s3://bucket/prefix) or list of S3 objects paths (e.g. ``[s3://bucket/key0, s3://bucket/key1]``).
    input_serialization: str,
        Format of the S3 object queried.
        Valid values: "CSV", "JSON", or "Parquet". Case sensitive.
    input_serialization_params: Dict[str, Union[bool, str]]
        Dictionary describing the serialization of the S3 object.
    compression: Optional[str]
        Compression type of the S3 object.
        Valid values: None, "gzip", or "bzip2". gzip and bzip2 are only valid for CSV and JSON objects.
    scan_range_chunk_size: int, optional
        Chunk size used to split the S3 object into scan ranges. 1,048,576 by default.
    path_suffix: Union[str, List[str], None]
        Suffix or List of suffixes to be read (e.g. [".csv"]).
        If None, read all files. (default)
    path_ignore_suffix: Union[str, List[str], None]
        Suffix or List of suffixes for S3 keys to be ignored. (e.g. ["_SUCCESS"]).
        If None, read all files. (default)
    ignore_empty: bool
        Ignore files with 0 bytes.
    use_threads : Union[bool, int]
        True (default) to enable concurrent requests, False to disable multiple threads.
        If enabled os.cpu_count() is used as the max number of threads.
        If integer is provided, specified number is used.
    last_modified_begin: datetime, optional
        Filter S3 objects by Last modified date.
        Filter is only applied after listing all objects.
    last_modified_end: datetime, optional
        Filter S3 objects by Last modified date.
        Filter is only applied after listing all objects.
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

    Reading an entire CSV object

    >>> import awswrangler as wr
    >>> df = wr.s3.select_query(
    ...     sql='SELECT * FROM s3object',
    ...     path='s3://bucket/key.csv',
    ...     input_serialization='CSV',
    ...     input_serialization_params={
    ...         'FileHeaderInfo': 'Use',
    ...         'RecordDelimiter': '\r\n'
    ...     },
    ... )

    Reading a single column from Parquet object with pushdown filter

    >>> import awswrangler as wr
    >>> df = wr.s3.select_query(
    ...     sql='SELECT s.\"id\" FROM s3object s where s.\"id\" = 1.0',
    ...     path='s3://bucket/key.snappy.parquet',
    ...     input_serialization='Parquet',
    ... )
    """
    if input_serialization not in ["CSV", "JSON", "Parquet"]:
        raise exceptions.InvalidArgumentValue("<input_serialization> argument must be 'CSV', 'JSON' or 'Parquet'")
    if compression not in [None, "gzip", "bzip2"]:
        raise exceptions.InvalidCompression(f"Invalid {compression} compression, please use None, 'gzip' or 'bzip2'.")
    if compression and (input_serialization not in ["CSV", "JSON"]):
        raise exceptions.InvalidArgumentCombination(
            "'gzip' or 'bzip2' are only valid for input 'CSV' or 'JSON' objects."
        )

    paths: List[str] = _path2list(
        path=path,
        boto3_session=boto3_session,
        suffix=path_suffix,
        ignore_suffix=_get_path_ignore_suffix(path_ignore_suffix=path_ignore_suffix),
        ignore_empty=ignore_empty,
        last_modified_begin=last_modified_begin,
        last_modified_end=last_modified_end,
        s3_additional_kwargs=s3_additional_kwargs,
    )
    if len(paths) < 1:
        raise exceptions.NoFilesFound(f"No files Found on: {path}.")
    _logger.debug("paths:\n%s", paths)

    args: Dict[str, Any] = {
        "sql": sql,
        "input_serialization": input_serialization,
        "input_serialization_params": input_serialization_params,
        "compression": compression,
        "scan_range_chunk_size": scan_range_chunk_size,
        "use_threads": use_threads,
        "boto3_session": boto3_session,
        "s3_additional_kwargs": s3_additional_kwargs,
    }
    _logger.debug("args:\n%s", pprint.pformat(args))

    if _ray_found:
        ds = ray.data.from_arrow_refs(
            _utils.flatten_list(*ray.get([_select_query(path=path, **args) for path in paths]))
        )
        if _modin_found:
            return ds.to_modin()
        return ds
    return concat_tables(
        _utils.flatten_list(
            *_read_tables_from_multiple_paths(
                read_func=_select_query,
                paths=paths,
                use_threads=use_threads,
                kwargs=args,
            )
        ),
    ).to_pandas(use_threads=use_threads, split_blocks=True, self_destruct=True, ignore_metadata=True)
