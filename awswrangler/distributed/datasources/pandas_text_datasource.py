"""Distributed PandasTextDatasource Module."""
import logging
from typing import Any, Callable, Dict, Iterator, List, Optional

import pandas as pd
import pyarrow
from ray.data.datasource.file_based_datasource import FileBasedDatasource
from ray.data.impl.pandas_block import PandasBlockAccessor
from ray.types import ObjectRef

from awswrangler import _utils
from awswrangler.s3._fs import open_s3_object
from awswrangler.s3._read_text_core import _read_text_chunked, _read_text_file

_logger: logging.Logger = logging.getLogger(__name__)

# The number of rows to read per batch. This is sized to generate 10MiB batches
# for rows about 1KiB in size.
READER_ROW_BATCH_SIZE = 10_0000


class PandasTextDatasource(FileBasedDatasource):
    """Pandas text datasource, for reading and writing text files using Pandas."""

    def __init__(
        self,
        file_format: str,
        read_text_func: Callable[..., pd.DataFrame],
        write_text_func: Optional[Callable[..., None]],
    ) -> None:
        super().__init__()

        self.file_format = file_format
        self.read_text_func = read_text_func
        self.write_text_func = write_text_func

        self._write_paths: List[str] = []

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

    def _write_block(  # type: ignore  # pylint: disable=arguments-differ
        self,
        f: pyarrow.NativeFile,
        block: PandasBlockAccessor,
        writer_args_fn: Callable[[], Dict[str, Any]],
        file_path: str,
        boto3_session: Optional[_utils.Boto3PrimitivesType],
        s3_additional_kwargs: Optional[Dict[str, str]],
        mode: str,
        encoding: str,
        newline: str,
        pandas_kwargs: Dict[str, Any],
        **writer_args: Any,
    ) -> None:
        if not self.write_text_func:
            raise RuntimeError(f"Write function not support for {self.file_format}")

        frame = block.to_pandas()

        with open_s3_object(
            path=file_path,
            mode=mode,
            use_threads=False,
            s3_additional_kwargs=s3_additional_kwargs,
            boto3_session=boto3_session,
            encoding=encoding,
            newline=newline,
        ) as file:
            _logger.debug("pandas_kwargs: %s", pandas_kwargs)
            self.write_text_func(frame, file, **pandas_kwargs)

    def on_write_complete(self, write_results: List[Any], **_: Any) -> None:
        """Execute callback on write complete."""
        _logger.debug("Write complete %s.", write_results)

        # Collect and return all write task paths
        self._write_paths.extend(write_results)

    def on_write_failed(self, write_results: List[ObjectRef[Any]], error: Exception, **_: Any) -> None:
        """Execute callback on write failed."""
        _logger.debug("Write failed %s.", write_results)
        raise error

    def get_write_paths(self) -> List[str]:
        """Return S3 paths of where the results have been written."""
        return self._write_paths


class PandasCSVDataSource(PandasTextDatasource):  # pylint: disable=abstract-method
    """Pandas CSV datasource, for reading and writing CSV files using Pandas."""

    def __init__(self) -> None:
        super().__init__("csv", pd.read_csv, pd.DataFrame.to_csv)


class PandasFWFDataSource(PandasTextDatasource):  # pylint: disable=abstract-method
    """Pandas FWF datasource, for reading and writing FWF files using Pandas."""

    def __init__(self) -> None:
        super().__init__("fwf", pd.read_fwf, None)


class PandasJSONDatasource(PandasTextDatasource):  # pylint: disable=abstract-method
    """Pandas JSON datasource, for reading and writing JSON files using Pandas."""

    def __init__(self) -> None:
        super().__init__("json", pd.read_json, pd.DataFrame.to_json)

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
