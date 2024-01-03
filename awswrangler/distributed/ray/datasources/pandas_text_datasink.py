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
        s3_additional_kwargs: Optional[Dict[str, str]] = None,
        pandas_kwargs: Optional[Dict[str, Any]] = None,
        **write_args: Any,
    ):
        super().__init__(
            path,
            file_format=file_format,
            block_path_provider=block_path_provider,
            dataset_uuid=dataset_uuid,
            s3_additional_kwargs=s3_additional_kwargs,
            pandas_kwargs=pandas_kwargs,
            **write_args,
        )

        self.write_text_func = write_text_func

    def write_block_to_file(self, file: io.TextIOWrapper, block: BlockAccessor):
        """
        Write a block of data to a file.

        Parameters
        ----------
        block : BlockAccessor
        file : pa.NativeFile
        """
        write_text_func = self.write_text_func

        write_text_func(block.to_pandas(), file, **self.pandas_args)  # type: ignore[misc]


class PandasCSVDatasink(_PandasTextDatasink):  # pylint: disable=abstract-method
    """A datasink that writes CSV files using Pandas IO."""

    def __init__(
        self,
        path: str,
        *,
        block_path_provider: Optional[BlockWritePathProvider] = None,
        dataset_uuid: Optional[str] = None,
        s3_additional_kwargs: Optional[Dict[str, str]] = None,
        pandas_kwargs: Optional[Dict[str, Any]] = None,
        **write_args: Any,
    ):
        super().__init__(
            path,
            "csv",
            pd.DataFrame.to_csv,
            block_path_provider=block_path_provider,
            dataset_uuid=dataset_uuid,
            s3_additional_kwargs=s3_additional_kwargs,
            pandas_kwargs=pandas_kwargs,
            **write_args,
        )


class PandasJSONDatasink(_PandasTextDatasink):  # pylint: disable=abstract-method
    """A datasink that writes CSV files using Pandas IO."""

    def __init__(
        self,
        path: str,
        *,
        block_path_provider: Optional[BlockWritePathProvider] = None,
        dataset_uuid: Optional[str] = None,
        s3_additional_kwargs: Optional[Dict[str, str]] = None,
        pandas_kwargs: Optional[Dict[str, Any]] = None,
        **write_args: Any,
    ):
        super().__init__(
            path,
            "json",
            pd.DataFrame.to_json,
            block_path_provider=block_path_provider,
            dataset_uuid=dataset_uuid,
            s3_additional_kwargs=s3_additional_kwargs,
            pandas_kwargs=pandas_kwargs,
            **write_args,
        )
