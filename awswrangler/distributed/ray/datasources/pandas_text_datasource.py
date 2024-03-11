"""Ray PandasTextDatasource Module."""

from __future__ import annotations

import logging
from typing import Any, Callable, Iterator

import pandas as pd
import pyarrow
from ray.data.datasource.file_based_datasource import FileBasedDatasource

from awswrangler import exceptions
from awswrangler.s3._read_text_core import _read_text_chunked, _read_text_file

_logger: logging.Logger = logging.getLogger(__name__)

# The number of rows to read per batch. This is sized to generate 10MiB batches
# for rows about 1KiB in size.
READER_ROW_BATCH_SIZE = 10_0000


class PandasTextDatasource(FileBasedDatasource):
    """Pandas text datasource, for reading text files using Pandas."""

    def __init__(
        self,
        paths: str | list[str],
        dataset: bool,
        path_root: str,
        read_text_func: Callable[..., pd.DataFrame],
        version_ids: dict[str, str] | None = None,
        s3_additional_kwargs: dict[str, str] | None = None,
        pandas_kwargs: dict[str, Any] | None = None,
        **file_based_datasource_kwargs: Any,
    ) -> None:
        super().__init__(paths, **file_based_datasource_kwargs)

        self.dataset = dataset
        self.path_root = path_root
        self.read_text_func = read_text_func

        self.version_ids = version_ids or {}
        self.s3_additional_kwargs = s3_additional_kwargs or {}
        self.pandas_kwargs = pandas_kwargs or {}

    def _read_stream(self, f: pyarrow.NativeFile, path: str) -> Iterator[pd.DataFrame]:
        read_text_func = self.read_text_func

        s3_path = f"s3://{path}"
        yield from _read_text_chunked(
            path=s3_path,
            chunksize=READER_ROW_BATCH_SIZE,
            parser_func=read_text_func,
            path_root=self.path_root,
            dataset=self.dataset,
            s3_client=None,
            pandas_kwargs=self.pandas_kwargs,
            s3_additional_kwargs=self.s3_additional_kwargs,
            use_threads=False,
            version_id=self.version_ids.get(s3_path) if self.version_ids else None,
        )


class PandasCSVDataSource(PandasTextDatasource):
    """Pandas CSV datasource, for reading CSV files using Pandas."""

    _FILE_EXTENSIONS = ["csv"]

    def __init__(
        self,
        paths: str | list[str],
        dataset: bool,
        path_root: str,
        version_ids: dict[str, str] | None = None,
        s3_additional_kwargs: dict[str, str] | None = None,
        pandas_kwargs: dict[str, Any] | None = None,
        **file_based_datasource_kwargs: Any,
    ) -> None:
        super().__init__(
            paths,
            dataset,
            path_root,
            pd.read_csv,
            version_ids=version_ids,
            s3_additional_kwargs=s3_additional_kwargs,
            pandas_kwargs=pandas_kwargs,
            **file_based_datasource_kwargs,
        )

    def _read_stream(self, f: pyarrow.NativeFile, path: str) -> Iterator[pd.DataFrame]:
        pandas_header_arg = self.pandas_kwargs.get("header", "infer")
        pandas_names_arg = self.pandas_kwargs.get("names", None)

        if pandas_header_arg is None and not pandas_names_arg:
            raise exceptions.InvalidArgumentCombination(
                "Distributed read_csv cannot read CSV files without header, or a `names` parameter."
            )

        yield from super()._read_stream(f, path)


class PandasFWFDataSource(PandasTextDatasource):
    """Pandas FWF datasource, for reading FWF files using Pandas."""

    _FILE_EXTENSIONS = ["fwf"]

    def __init__(
        self,
        paths: str | list[str],
        dataset: bool,
        path_root: str,
        version_ids: dict[str, str] | None = None,
        s3_additional_kwargs: dict[str, str] | None = None,
        pandas_kwargs: dict[str, Any] | None = None,
        **file_based_datasource_kwargs: Any,
    ) -> None:
        super().__init__(
            paths,
            dataset,
            path_root,
            pd.read_fwf,
            version_ids=version_ids,
            s3_additional_kwargs=s3_additional_kwargs,
            pandas_kwargs=pandas_kwargs,
            **file_based_datasource_kwargs,
        )


class PandasJSONDatasource(PandasTextDatasource):
    """Pandas JSON datasource, for reading JSON files using Pandas."""

    _FILE_EXTENSIONS = ["json"]

    def __init__(
        self,
        paths: str | list[str],
        dataset: bool,
        path_root: str,
        version_ids: dict[str, str] | None = None,
        s3_additional_kwargs: dict[str, str] | None = None,
        pandas_kwargs: dict[str, Any] | None = None,
        **file_based_datasource_kwargs: Any,
    ) -> None:
        super().__init__(
            paths,
            dataset,
            path_root,
            pd.read_json,
            version_ids=version_ids,
            s3_additional_kwargs=s3_additional_kwargs,
            pandas_kwargs=pandas_kwargs,
            **file_based_datasource_kwargs,
        )

    def _read_stream(self, f: pyarrow.NativeFile, path: str) -> Iterator[pd.DataFrame]:
        read_text_func = self.read_text_func

        pandas_lines = self.pandas_kwargs.get("lines", False)
        if pandas_lines:
            yield from super()._read_stream(f, path)
        else:
            s3_path = f"s3://{path}"
            yield _read_text_file(
                path=s3_path,
                parser_func=read_text_func,
                path_root=self.path_root,
                dataset=self.dataset,
                s3_client=None,
                pandas_kwargs=self.pandas_kwargs,
                s3_additional_kwargs=self.s3_additional_kwargs,
                version_id=self.version_ids.get(s3_path) if self.version_ids else None,
            )
