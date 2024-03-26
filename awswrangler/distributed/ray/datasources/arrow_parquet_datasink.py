"""Ray ArrowParquetDatasink Module."""

from __future__ import annotations

import logging
from typing import Any

import pyarrow as pa
from ray.data.block import BlockAccessor
from ray.data.datasource.filename_provider import FilenameProvider

from awswrangler._arrow import _df_to_table
from awswrangler.distributed.ray.datasources.file_datasink import _BlockFileDatasink
from awswrangler.distributed.ray.datasources.filename_provider import _DefaultFilenameProvider
from awswrangler.s3._write import _COMPRESSION_2_EXT

_logger: logging.Logger = logging.getLogger(__name__)


class _ParquetFilenameProvider(_DefaultFilenameProvider):
    """Parquet filename provider where compression comes before file format."""

    def _generate_filename(self, file_id: str) -> str:
        filename = ""
        if self._dataset_uuid is not None:
            filename += f"{self._dataset_uuid}_"
        filename += f"{file_id}"
        if self._bucket_id is not None:
            filename += f"_bucket-{self._bucket_id:05d}"
        filename += f"{_COMPRESSION_2_EXT.get(self._compression)}.{self._file_format}"
        return filename


class ArrowParquetDatasink(_BlockFileDatasink):
    """A datasink that writes Parquet files."""

    def __init__(
        self,
        path: str,
        *,
        filename_provider: FilenameProvider | None = None,
        dataset_uuid: str | None = None,
        open_s3_object_args: dict[str, Any] | None = None,
        pandas_kwargs: dict[str, Any] | None = None,
        schema: pa.Schema | None = None,
        index: bool = False,
        dtype: dict[str, str] | None = None,
        pyarrow_additional_kwargs: dict[str, Any] | None = None,
        compression: str | None = None,
        **write_args: Any,
    ):
        file_format = "parquet"
        write_args = write_args or {}

        if filename_provider is None:
            bucket_id = write_args.get("bucket_id", None)

            filename_provider = _ParquetFilenameProvider(
                dataset_uuid=dataset_uuid,
                file_format=file_format,
                compression=compression,
                bucket_id=bucket_id,
            )

        super().__init__(
            path,
            file_format=file_format,
            filename_provider=filename_provider,
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
