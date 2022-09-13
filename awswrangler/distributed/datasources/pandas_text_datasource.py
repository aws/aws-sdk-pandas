"""Distributed PandasTextDatasource Module."""
from typing import Any, Callable, Dict, Iterator, Optional

import pandas as pd
import pyarrow
from ray.data.datasource.file_based_datasource import FileBasedDatasource

from awswrangler import _utils
from awswrangler.s3._read_text_core import _read_text_chunked, _read_text_file


# The number of rows to read per batch. This is sized to generate 10MiB batches
# for rows about 1KiB in size.
READER_ROW_BATCH_SIZE = 10_0000


class PandasTextDatasource(FileBasedDatasource):
    """Pandas text datasource, for reading and writing text files using Pandas."""
    def __init__(
        self,
        read_text_func: Callable[..., pd.DataFrame],
    ) -> None:
        super().__init__()
        self.read_text_func = read_text_func

    def _read_stream(
        self,
        f: pyarrow.NativeFile,
        path: str,
        path_root: str,
        dataset: bool,
        version_id_dict: Dict[str, Optional[str]],
        boto3_session: Optional[_utils.Boto3PrimitivesType],
        s3_additional_kwargs: Optional[Dict[str, str]],
        pandas_kwargs: Dict[str, Any],
    ) -> Iterator[pd.DataFrame]:
        s3_path = f"s3://{path}"
        yield from _read_text_chunked(
            path=s3_path,
            chunksize=READER_ROW_BATCH_SIZE,
            parser_func=self.read_text_func,
            path_root=path_root,
            dataset=dataset,
            boto3_session=boto3_session,
            pandas_kwargs=pandas_kwargs,
            s3_additional_kwargs=s3_additional_kwargs,
            use_threads=False,
            version_id=version_id_dict.get(s3_path),
        )


class PandasJSONDatasource(PandasTextDatasource):
    """Pandas JSON datasource, for reading and writing JSON files using Pandas."""
    def __init__(self) -> None:
        super().__init__(pd.read_json)

    def _read_stream(
        self,
        f: pyarrow.NativeFile,
        path: str,
        path_root: str,
        dataset: bool,
        version_id_dict: Dict[str, Optional[str]],
        boto3_session: Optional[_utils.Boto3PrimitivesType],
        s3_additional_kwargs: Optional[Dict[str, str]],
        pandas_kwargs: Dict[str, Any],
    ) -> Iterator[pd.DataFrame]:
        pandas_lines = pandas_kwargs.get("lines", False)
        if pandas_lines:
            yield from super()._read_stream(
                f,
                path,
                path_root,
                dataset,
                version_id_dict,
                boto3_session,
                s3_additional_kwargs,
                pandas_kwargs,
            )
        else:
            s3_path = f"s3://{path}"
            yield _read_text_file(
                path=s3_path,
                parser_func=pd.read_json,
                path_root=path_root,
                dataset=dataset,
                boto3_session=boto3_session,
                pandas_kwargs=pandas_kwargs,
                s3_additional_kwargs=s3_additional_kwargs,
                version_id=version_id_dict.get(s3_path),
            )
