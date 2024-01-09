"""Ray ArrowParquetDatasink Module."""
from __future__ import annotations

import logging
from typing import Any

import pyarrow as pa
from ray.data.block import BlockAccessor
from ray.data.datasource.block_path_provider import BlockWritePathProvider

from awswrangler._arrow import _df_to_table
from awswrangler.distributed.ray.datasources.file_datasink import _BlockFileDatasink
from awswrangler.s3._write import _COMPRESSION_2_EXT

_logger: logging.Logger = logging.getLogger(__name__)


class ArrowParquetDatasink(_BlockFileDatasink):
    """A datasink that writes Parquet files."""

    def __init__(
        self,
        path: str,
        *,
        block_path_provider: BlockWritePathProvider | None = None,
        dataset_uuid: str | None = None,
        open_s3_object_args: dict[str, Any] | None = None,
        pandas_kwargs: dict[str, Any] | None = None,
        schema: pa.Schema | None = None,
        index: bool = False,
        dtype: dict[str, str] | None = None,
        pyarrow_additional_kwargs: dict[str, Any] | None = None,
        **write_args: Any,
    ):
        super().__init__(
            path,
            file_format="parquet",
            block_path_provider=block_path_provider,
            dataset_uuid=dataset_uuid,
            open_s3_object_args=open_s3_object_args,
            pandas_kwargs=pandas_kwargs,
            **write_args,
        )

        self.pyarrow_additional_kwargs = pyarrow_additional_kwargs or {}
        self.schema = schema
        self.index = index
        self.dtype = dtype

    def write_block(self, file: pa.NativeFile, block: BlockAccessor) -> None:
        """
        Write a block of data to a file.

        Parameters
        ----------
        file : pa.NativeFile
        block : BlockAccessor
        """
        pa.parquet.write_table(
            _df_to_table(block.to_pandas(), schema=self.schema, index=self.index, dtype=self.dtype),
            file,
            **self.pyarrow_additional_kwargs,
        )

    def _get_file_suffix(self, file_format: str, compression: str | None) -> str:
        if compression is not None:
            return f"{_COMPRESSION_2_EXT.get(compression)[1:]}.{file_format}"  # type: ignore[index]
        return file_format
