"""Amazon S3 Read Core Module (PRIVATE)."""
import logging
from typing import TYPE_CHECKING, Any, Callable, Dict, Iterator, List, Optional, Tuple, Union

import botocore.exceptions
import pandas as pd
import pandas.io.parsers
from pandas.io.common import infer_compression

from awswrangler import exceptions
from awswrangler.s3._fs import open_s3_object
from awswrangler.s3._read import _apply_partitions

if TYPE_CHECKING:
    from mypy_boto3_s3 import S3Client

_logger: logging.Logger = logging.getLogger(__name__)


def _get_read_details(path: str, pandas_kwargs: Dict[str, Any]) -> Tuple[str, Optional[str], Optional[str]]:
    if pandas_kwargs.get("compression", "infer") == "infer":
        pandas_kwargs["compression"] = infer_compression(path, compression="infer")
    mode: str = (
        "r" if pandas_kwargs.get("compression") is None and pandas_kwargs.get("encoding_errors") != "ignore" else "rb"
    )
    encoding: Optional[str] = pandas_kwargs.get("encoding", "utf-8")
    newline: Optional[str] = pandas_kwargs.get("lineterminator", None)
    return mode, encoding, newline


def _read_text_chunked(
    path: str,
    chunksize: int,
    parser_func: Callable[..., pd.DataFrame],
    path_root: Optional[str],
    s3_client: Optional["S3Client"],
    pandas_kwargs: Dict[str, Any],
    s3_additional_kwargs: Optional[Dict[str, str]],
    dataset: bool,
    use_threads: Union[bool, int],
    version_id: Optional[str] = None,
) -> Iterator[pd.DataFrame]:
    mode, encoding, newline = _get_read_details(path=path, pandas_kwargs=pandas_kwargs)
    with open_s3_object(
        path=path,
        version_id=version_id,
        mode=mode,
        s3_block_size=10 * 1024 * 1024,  # 10 MB (10 * 2**20)
        encoding=encoding,
        use_threads=use_threads,
        s3_client=s3_client,
        s3_additional_kwargs=s3_additional_kwargs,
        newline=newline,
    ) as f:
        reader: pandas.io.parsers.TextFileReader = parser_func(f, chunksize=chunksize, **pandas_kwargs)
        for df in reader:
            yield _apply_partitions(df=df, dataset=dataset, path=path, path_root=path_root)


def _read_text_files_chunked(
    paths: List[str],
    chunksize: int,
    parser_func: Callable[..., pd.DataFrame],
    path_root: Optional[str],
    s3_client: "S3Client",
    pandas_kwargs: Dict[str, Any],
    s3_additional_kwargs: Optional[Dict[str, str]],
    dataset: bool,
    use_threads: Union[bool, int],
    version_ids: Optional[Dict[str, str]],
) -> Iterator[pd.DataFrame]:
    for path in paths:
        _logger.debug("path: %s", path)

        yield from _read_text_chunked(
            path=path,
            chunksize=chunksize,
            parser_func=parser_func,
            path_root=path_root,
            s3_client=s3_client,
            pandas_kwargs=pandas_kwargs,
            s3_additional_kwargs=s3_additional_kwargs,
            dataset=dataset,
            use_threads=use_threads,
            version_id=version_ids.get(path) if version_ids else None,
        )


def _read_text_file(
    s3_client: Optional["S3Client"],
    path: str,
    version_id: Optional[str],
    parser_func: Callable[..., pd.DataFrame],
    path_root: Optional[str],
    pandas_kwargs: Dict[str, Any],
    s3_additional_kwargs: Optional[Dict[str, str]],
    dataset: bool,
) -> pd.DataFrame:
    mode, encoding, newline = _get_read_details(path=path, pandas_kwargs=pandas_kwargs)
    try:
        with open_s3_object(
            path=path,
            version_id=version_id,
            mode=mode,
            use_threads=False,
            s3_block_size=-1,  # One shot download
            encoding=encoding,
            s3_client=s3_client,
            s3_additional_kwargs=s3_additional_kwargs,
            newline=newline,
        ) as f:
            df: pd.DataFrame = parser_func(f, **pandas_kwargs)
    except botocore.exceptions.ClientError as e:
        if e.response["Error"]["Code"] == "404":
            raise exceptions.NoFilesFound(f"No files Found on: {path}.")
        raise e
    return _apply_partitions(df=df, dataset=dataset, path=path, path_root=path_root)
