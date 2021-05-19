"""Amazon S3 Select Module (PRIVATE)."""

import concurrent.futures
import itertools
from io import StringIO
import logging
import pprint
from typing import Any, Dict, Iterator, List, Optional, Tuple, Union

import boto3
import pandas as pd

from awswrangler import _utils, exceptions
from awswrangler.s3._describe import size_objects

_logger: logging.Logger = logging.getLogger(__name__)

_RANGE_CHUNK_SIZE: int = 5_242_880  # 5 MB (5 * 2**20)


def _select_object_content(
    args: Dict[str, Any], scan_range: Optional[Tuple[int, int]], client_s3: Optional[boto3.Session]
) -> pd.DataFrame:
    if scan_range:
        args.update({"ScanRange": {"Start": scan_range[0], "End": scan_range[1]}})
    response = client_s3.select_object_content(**args)
    l: pd.DataFrame = []
    print(type(response["Payload"]))
    for event in response["Payload"]:
        print(type(event))
        if "Records" in event:
            l.append(pd.read_csv(StringIO(event["Records"]["Payload"].decode("utf-8"))))
    return pd.concat(l)

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

    scan_ranges: List[Tuple[int, int]] = []
    for i in range(0, obj_size, _RANGE_CHUNK_SIZE):
        scan_ranges.append((i, i + min(_RANGE_CHUNK_SIZE, obj_size - i)))

    dfs_iterator: List[pd.Dataframe] = []
    client_s3: boto3.client = _utils.client(service_name="s3", session=boto3_session)
    if use_threads is False:
        dfs_iterator = list(
            _select_object_content(
                args=args,
                scan_range=scan_range,
                client_s3=client_s3,
            )
            for scan_range in scan_ranges
        )
    else:
        cpus: int = _utils.ensure_cpu_count(use_threads=use_threads)
        with concurrent.futures.ThreadPoolExecutor(max_workers=cpus) as executor:
            dfs_iterator = list(
                executor.map(
                    _select_object_content,
                    itertools.repeat(args),
                    scan_ranges,
                    itertools.repeat(client_s3),
                )
            )
    return pd.concat([df for df in dfs_iterator])


# TODO: clarify when to use @config (e.g. read_parquet vs read_parquet_table)
def select_query(  # Read sql query or S3 select? Here or in a separate file?
    sql: str,
    path: str,
    input_serialization: str,
    output_serialization: str,
    input_serialization_params: Dict[str, Union[bool, str]] = {},
    output_serialization_params: Dict[str, str] = {},
    compression: Optional[str] = None,
    use_threads: Union[bool, int] = False,
    boto3_session: Optional[boto3.Session] = None,
    params: Optional[Dict[str, Any]] = None,
    s3_additional_kwargs: Optional[Dict[str, Any]] = None,
) -> Union[pd.DataFrame, Iterator[pd.DataFrame]]:

    if path.endswith("/"):
        raise exceptions.InvalidArgumentValue("<path> argument should be an S3 key, not a prefix.")
    if input_serialization not in ["CSV", "JSON", "Parquet"]:
        raise exceptions.InvalidArgumentValue("<input_serialization> argument must be 'CSV', 'JSON' or 'Parquet'")
    if compression not in [None, "gzip", "bzip2"]:
        raise exceptions.InvalidCompression(f"Invalid {compression} compression, please use None, 'gzip' or 'bzip2'.")
    else:
        if compression and (input_serialization not in ["CSV", "JSON"]):
            raise exceptions.InvalidArgumentCombination(
                "'gzip' or 'bzip2' are only valid for input 'CSV' or 'JSON' objects."
            )
    if output_serialization not in [None, "CSV", "JSON"]:
        raise exceptions.InvalidArgumentValue("<output_serialization> argument must be None, 'csv' or 'json'")
    if params is None:
        params = {}
    for key, value in params.items():
        sql = sql.replace(f":{key};", str(value))
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
            output_serialization: output_serialization_params,
        },
    }
    if s3_additional_kwargs:
        args.update(s3_additional_kwargs)
    _logger.debug("args:\n%s", pprint.pformat(args))

    return _paginate_stream(args=args, path=path, use_threads=use_threads, boto3_session=boto3_session)
