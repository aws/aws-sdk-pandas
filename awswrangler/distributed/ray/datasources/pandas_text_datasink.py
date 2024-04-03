"""Ray PandasTextDatasink Module."""

from __future__ import annotations

import io
import logging
from typing import Any, Callable

import pandas as pd
from ray.data.block import BlockAccessor
from ray.data.datasource.filename_provider import FilenameProvider

from awswrangler.distributed.ray.datasources.file_datasink import _BlockFileDatasink

_logger: logging.Logger = logging.getLogger(__name__)


class _PandasTextDatasink(_BlockFileDatasink):
    """A datasink that writes text files using Pandas IO."""

    def __init__(
        self,
        path: str,
        file_format: str,
        write_text_func: Callable[..., None] | None,
        *,
        filename_provider: FilenameProvider | None = None,
        dataset_uuid: str | None = None,
        open_s3_object_args: dict[str, Any] | None = None,
        pandas_kwargs: dict[str, Any] | None = None,
        **write_args: Any,
    ):
        super().__init__(
            path,
            file_format=file_format,
            filename_provider=filename_provider,
            dataset_uuid=dataset_uuid,
            open_s3_object_args=open_s3_object_args,
            pandas_kwargs=pandas_kwargs,
            **write_args,
        )

        self.write_text_func = write_text_func

    def write_block(self, file: io.TextIOWrapper, block: BlockAccessor) -> None:
        """
        Write a block of data to a file.

        Parameters
        ----------
        block : BlockAccessor
        file : pa.NativeFile
        """
        write_text_func = self.write_text_func

        write_text_func(block.to_pandas(), file, **self.pandas_kwargs)  # type: ignore[misc]


class PandasCSVDatasink(_PandasTextDatasink):
    """A datasink that writes CSV files using Pandas IO."""

    def __init__(
        self,
        path: str,
        *,
        filename_provider: FilenameProvider | None = None,
        dataset_uuid: str | None = None,
        open_s3_object_args: dict[str, Any] | None = None,
        pandas_kwargs: dict[str, Any] | None = None,
        **write_args: Any,
    ):
        super().__init__(
            path,
            "csv",
            pd.DataFrame.to_csv,
            filename_provider=filename_provider,
            dataset_uuid=dataset_uuid,
            open_s3_object_args=open_s3_object_args,
            pandas_kwargs=pandas_kwargs,
            **write_args,
        )


class PandasJSONDatasink(_PandasTextDatasink):
    """A datasink that writes CSV files using Pandas IO."""

    def __init__(
        self,
        path: str,
        *,
        filename_provider: FilenameProvider | None = None,
        dataset_uuid: str | None = None,
        open_s3_object_args: dict[str, Any] | None = None,
        pandas_kwargs: dict[str, Any] | None = None,
        **write_args: Any,
    ):
        super().__init__(
            path,
            "json",
            pd.DataFrame.to_json,
            filename_provider=filename_provider,
            dataset_uuid=dataset_uuid,
            open_s3_object_args=open_s3_object_args,
            pandas_kwargs=pandas_kwargs,
            **write_args,
        )
