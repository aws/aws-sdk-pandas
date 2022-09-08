from typing import Any, Callable, Dict, Optional, Tuple

from ray.data.datasource.file_based_datasource import (
    FileBasedDatasource,
)

import boto3
import botocore
import pandas as pd
from pandas.io.common import infer_compression
import pyarrow
from awswrangler import _utils, exceptions
from awswrangler.s3._fs import open_s3_object
from awswrangler.s3._read import _apply_partitions
from awswrangler.s3._read_text_core import _read_text_file


def _get_read_details(path: str, pandas_kwargs: Dict[str, Any]) -> Tuple[str, Optional[str], Optional[str]]:
    if pandas_kwargs.get("compression", "infer") == "infer":
        pandas_kwargs["compression"] = infer_compression(path, compression="infer")
    mode: str = "r" if pandas_kwargs.get("compression") is None else "rb"
    encoding: Optional[str] = pandas_kwargs.get("encoding", "utf-8")
    newline: Optional[str] = pandas_kwargs.get("lineterminator", None)
    return mode, encoding, newline


class PandasDatasource(FileBasedDatasource):
    def __init__(self, read_text_func: Callable[..., pd.DataFrame]) -> None:
        super().__init__()
        self.read_text_func = read_text_func

    def _read_file(
        self,
        f: pyarrow.NativeFile,
        path: str,
        path_root: str,
        dataset: bool,
        version_id: Optional[str],
        boto3_session: Optional[boto3.Session],
        s3_additional_kwargs: Optional[Dict[str, str]],
        pandas_kwargs: Dict[str, Any],
    ) -> pd.DataFrame:
        return _read_text_file(
            path=f"s3://{path}",
            parser_func=self.read_text_func,
            path_root=path_root,
            dataset=dataset,
            version_id=version_id,
            boto3_session=boto3_session,
            s3_additional_kwargs=s3_additional_kwargs,
            pandas_kwargs=pandas_kwargs,
        )
