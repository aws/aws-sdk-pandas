"""Distributed PandasTextDatasource Module."""
from typing import Any, Callable, Dict, Optional

import boto3
import pandas as pd
import pyarrow
from ray.data.datasource.file_based_datasource import FileBasedDatasource

from awswrangler.s3._read_text_core import _read_text_file


class PandasTextDatasource(FileBasedDatasource):
    """Pandas text datasource, for reading and writing text files using Pandas."""
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
