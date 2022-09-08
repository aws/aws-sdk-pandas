"""Distributed PandasTextDatasource Module."""
from typing import Any, Callable, Dict, Iterator, Optional

import pandas as pd
import pyarrow
from ray.data.datasource.file_based_datasource import FileBasedDatasource

from awswrangler import _utils
from awswrangler.s3._read_text_core import _read_text_chunked, _read_text_file


class PandasTextDatasource(FileBasedDatasource):
    """Pandas text datasource, for reading and writing text files using Pandas."""
    def __init__(
        self,
        read_text_func: Callable[..., pd.DataFrame],
        max_rows_per_block: Optional[int] = None,
    ) -> None:
        super().__init__()
        self.read_text_func = read_text_func
        self.max_rows_per_block = max_rows_per_block

    def _read_stream(
        self,
        f: pyarrow.NativeFile,
        path: str,
        path_root: str,
        dataset: bool,
        version_id: Optional[str],
        boto3_session: Optional[_utils.Boto3PrimitivesType],
        s3_additional_kwargs: Optional[Dict[str, str]],
        pandas_kwargs: Dict[str, Any],
    ) -> Iterator[pd.DataFrame]:
        if self.max_rows_per_block is None:
            yield self._read_file(
                f,
                path=path,
                path_root=path_root,
                dataset=dataset,
                version_id=version_id,
                boto3_session=boto3_session,
                s3_additional_kwargs=s3_additional_kwargs,
                pandas_kwargs=pandas_kwargs,
            )
        else:
            yield from _read_text_chunked(
                path=f"s3://{path}",
                chunksize=self.max_rows_per_block,
                parser_func=self.read_text_func,
                path_root=path_root,
                dataset=dataset,
                boto3_session=boto3_session,
                pandas_kwargs=pandas_kwargs,
                s3_additional_kwargs=s3_additional_kwargs,
                use_threads=False,
                version_id=version_id,
            )

    def _read_file(
        self,
        f: pyarrow.NativeFile,
        path: str,
        path_root: str,
        dataset: bool,
        version_id: Optional[str],
        boto3_session: Optional[_utils.Boto3PrimitivesType],
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
