"""Distributed PandasTextDatasource Module."""
from typing import Any, Callable, Dict, Iterator, Optional

import pandas as pd
import pyarrow
from ray.data._internal.pandas_block import PandasBlockAccessor
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
        file_format: str,
        read_text_func: Callable[..., pd.DataFrame],
    ) -> None:
        super().__init__()
        self.file_format = file_format
        self.read_text_func = read_text_func

    def _file_format(self) -> str:
        return self.file_format

    def _read_stream(  # type: ignore  # pylint: disable=arguments-differ
        self,
        f: pyarrow.NativeFile,
        path: str,
        path_root: str,
        dataset: bool,
        version_id_dict: Dict[str, Optional[str]],
        boto3_session: Optional[_utils.Boto3PrimitivesType],
        s3_additional_kwargs: Optional[Dict[str, str]],
        pandas_kwargs: Dict[str, Any],
        **reader_args: Any,
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

    def _read_file(self, f: pyarrow.NativeFile, path: str, **reader_args: Any) -> pd.DataFrame:
        raise NotImplementedError()

    def _write_block(  # type: ignore  # pylint: disable=signature-differs
        self,
        f: pyarrow.NativeFile,
        block: PandasBlockAccessor,
        writer_args_fn: Callable[[], Dict[str, Any]],
        **writer_args: Any,
    ) -> None:
        raise NotImplementedError()


class PandasCSVDataSource(PandasTextDatasource):  # pylint: disable=abstract-method
    """Pandas CSV datasource, for reading and writing CSV files using Pandas."""

    def __init__(self) -> None:
        super().__init__("csv", pd.read_csv)


class PandasFWFDataSource(PandasTextDatasource):  # pylint: disable=abstract-method
    """Pandas FWF datasource, for reading and writing FWF files using Pandas."""

    def __init__(self) -> None:
        super().__init__("fwf", pd.read_fwf)


class PandasJSONDatasource(PandasTextDatasource):  # pylint: disable=abstract-method
    """Pandas JSON datasource, for reading and writing JSON files using Pandas."""

    def __init__(self) -> None:
        super().__init__("json", pd.read_json)

    def _read_stream(  # type: ignore
        self,
        f: pyarrow.NativeFile,
        path: str,
        path_root: str,
        dataset: bool,
        version_id_dict: Dict[str, Optional[str]],
        boto3_session: Optional[_utils.Boto3PrimitivesType],
        s3_additional_kwargs: Optional[Dict[str, str]],
        pandas_kwargs: Dict[str, Any],
        **reader_args: Any,
    ) -> Iterator[pd.DataFrame]:  # type: ignore
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
