"""Ray PandasTextDatasink Module."""

import io
import logging
from typing import Any, Callable, Dict, Optional

import pandas as pd
from ray.data.block import BlockAccessor
from ray.data.datasource.block_path_provider import BlockWritePathProvider

from awswrangler.distributed.ray.datasources.file_datasink import _BlockFileDatasink

_logger: logging.Logger = logging.getLogger(__name__)


class _PandasTextDatasink(_BlockFileDatasink):
    """A datasink that writes text files using Pandas IO."""

    def __init__(
        self,
        path: str,
        file_format: str,
        write_text_func: Optional[Callable[..., None]],
        *,
        block_path_provider: Optional[BlockWritePathProvider] = None,
        dataset_uuid: Optional[str] = None,
        open_s3_object_args: Optional[Dict[str, Any]] = None,
        pandas_kwargs: Optional[Dict[str, Any]] = None,
        **write_args: Any,
    ):
        super().__init__(
            path,
            file_format=file_format,
            block_path_provider=block_path_provider,
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
        block_path_provider: Optional[BlockWritePathProvider] = None,
        dataset_uuid: Optional[str] = None,
        open_s3_object_args: Optional[Dict[str, Any]] = None,
        pandas_kwargs: Optional[Dict[str, Any]] = None,
        **write_args: Any,
    ):
        super().__init__(
            path,
            "csv",
            pd.DataFrame.to_csv,
            block_path_provider=block_path_provider,
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
        block_path_provider: Optional[BlockWritePathProvider] = None,
        dataset_uuid: Optional[str] = None,
        open_s3_object_args: Optional[Dict[str, Any]] = None,
        pandas_kwargs: Optional[Dict[str, Any]] = None,
        **write_args: Any,
    ):
        super().__init__(
            path,
            "json",
            pd.DataFrame.to_json,
            block_path_provider=block_path_provider,
            dataset_uuid=dataset_uuid,
            open_s3_object_args=open_s3_object_args,
            pandas_kwargs=pandas_kwargs,
            **write_args,
        )
