"""Ray ArrowParquetDatasink Module."""

import logging
from typing import Any, Dict, Optional

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
        block_path_provider: Optional[BlockWritePathProvider] = None,
        dataset_uuid: Optional[str] = None,
        s3_additional_kwargs: Optional[Dict[str, str]] = None,
        pandas_kwargs: Optional[Dict[str, Any]] = None,
        schema: Optional[pa.Schema] = None,
        pyarrow_additional_kwargs: Optional[Dict[str, Any]] = None,
        **write_args: Any,
    ):
        super().__init__(
            path,
            file_format="parquet",
            block_path_provider=block_path_provider,
            dataset_uuid=dataset_uuid,
            s3_additional_kwargs=s3_additional_kwargs,
            pandas_kwargs=pandas_kwargs,
            **write_args,
        )

        self.pyarrow_additional_kwargs = pyarrow_additional_kwargs or {}
        self.schema = schema

    def write_block(self, file: pa.NativeFile, block: BlockAccessor) -> None:
        """
        Write a block of data to a file.

        Parameters
        ----------
        file : pa.NativeFile
        block : BlockAccessor
        """
        write_args = self.write_args

        dtype: Optional[Dict[str, str]] = write_args.pop("dtype", None)
        index: bool = write_args.pop("index", False)

        pa.parquet.write_table(
            _df_to_table(block.to_pandas(), schema=self.schema, index=index, dtype=dtype),
            file,
            **self.pyarrow_additional_kwargs,
        )

    def _get_file_suffix(self, file_format: str, compression: Optional[str]) -> str:
        if compression is not None:
            return f"{_COMPRESSION_2_EXT.get(compression)[1:]}.{file_format}"  # type: ignore[index]
        return file_format
