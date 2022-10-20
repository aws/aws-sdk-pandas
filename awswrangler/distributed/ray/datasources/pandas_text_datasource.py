"""Ray PandasTextDatasource Module."""
import io
import logging
from typing import Any, Callable, Dict, Iterator, List, Optional

import pandas as pd
import pyarrow
from ray.data._internal.pandas_block import PandasBlockAccessor

from awswrangler._arrow import _add_table_partitions
from awswrangler.distributed.ray.datasources.pandas_file_based_datasource import PandasFileBasedDatasource
from awswrangler.s3._read_text_core import _read_text_chunked, _read_text_file

_logger: logging.Logger = logging.getLogger(__name__)

# The number of rows to read per batch. This is sized to generate 10MiB batches
# for rows about 1KiB in size.
READER_ROW_BATCH_SIZE = 10_0000


class PandasTextDatasource(PandasFileBasedDatasource):  # pylint: disable=abstract-method
    """Pandas text datasource, for reading and writing text files using Pandas."""

    def __init__(
        self,
        read_text_func: Callable[..., pd.DataFrame],
        write_text_func: Optional[Callable[..., None]],
    ) -> None:
        super().__init__()

        self.read_text_func = read_text_func
        self.write_text_func = write_text_func

        self._write_paths: List[str] = []

    def _read_stream(  # type: ignore  # pylint: disable=arguments-differ
        self,
        f: pyarrow.NativeFile,  # Refactor reader to use wr.open_s3_object
        path: str,
        path_root: str,
        dataset: bool,
        version_ids: Dict[str, Optional[str]],
        s3_additional_kwargs: Optional[Dict[str, str]],
        pandas_kwargs: Optional[Dict[str, Any]],
        **reader_args: Any,
    ) -> Iterator[pd.DataFrame]:
        read_text_func = self.read_text_func

        if not s3_additional_kwargs:
            s3_additional_kwargs = {}

        if not pandas_kwargs:
            pandas_kwargs = {}

        s3_path = f"s3://{path}"
        yield from _read_text_chunked(
            path=s3_path,
            chunksize=READER_ROW_BATCH_SIZE,
            parser_func=read_text_func,
            path_root=path_root,
            dataset=dataset,
            boto3_session=None,
            pandas_kwargs=pandas_kwargs,
            s3_additional_kwargs=s3_additional_kwargs,
            use_threads=False,
            version_id=version_ids.get(s3_path),
        )

    def _read_file(self, f: pyarrow.NativeFile, path: str, **reader_args: Any) -> pd.DataFrame:
        raise NotImplementedError()

    def _write_block(  # type: ignore  # pylint: disable=arguments-differ, arguments-renamed
        self,
        f: io.TextIOWrapper,
        block: PandasBlockAccessor,
        pandas_kwargs: Optional[Dict[str, Any]],
        **writer_args: Any,
    ) -> None:
        write_text_func = self.write_text_func

        if not pandas_kwargs:
            pandas_kwargs = {}

        write_text_func(block.to_pandas(), f, **pandas_kwargs)  # type: ignore


class PandasCSVDataSource(PandasTextDatasource):  # pylint: disable=abstract-method
    """Pandas CSV datasource, for reading and writing CSV files using Pandas."""

    _FILE_EXTENSION = "csv"

    def __init__(self) -> None:
        super().__init__(pd.read_csv, pd.DataFrame.to_csv)


class PandasFWFDataSource(PandasTextDatasource):  # pylint: disable=abstract-method
    """Pandas FWF datasource, for reading and writing FWF files using Pandas."""

    _FILE_EXTENSION = "fwf"

    def __init__(self) -> None:
        super().__init__(pd.read_fwf, None)


class PandasJSONDatasource(PandasTextDatasource):  # pylint: disable=abstract-method
    """Pandas JSON datasource, for reading and writing JSON files using Pandas."""

    _FILE_EXTENSION = "json"

    def __init__(self) -> None:
        super().__init__(pd.read_json, pd.DataFrame.to_json)

    def _read_stream(  # type: ignore
        self,
        f: pyarrow.NativeFile,
        path: str,
        path_root: str,
        dataset: bool,
        version_ids: Dict[str, Optional[str]],
        s3_additional_kwargs: Optional[Dict[str, str]],
        pandas_kwargs: Dict[str, Any],
        **reader_args: Any,
    ) -> Iterator[pd.DataFrame]:  # type: ignore
        read_text_func = self.read_text_func

        pandas_lines = pandas_kwargs.get("lines", False)
        if pandas_lines:
            yield from super()._read_stream(
                f,
                path,
                path_root,
                dataset,
                version_ids,
                s3_additional_kwargs,
                pandas_kwargs,
            )
        else:
            s3_path = f"s3://{path}"
            yield _read_text_file(
                path=s3_path,
                parser_func=read_text_func,
                path_root=path_root,
                dataset=dataset,
                boto3_session=None,
                pandas_kwargs=pandas_kwargs,
                s3_additional_kwargs=s3_additional_kwargs,
                version_id=version_ids.get(s3_path),
            )
