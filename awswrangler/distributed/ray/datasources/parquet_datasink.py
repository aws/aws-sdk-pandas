"""Ray ParquetDatasink Module."""

import logging
from typing import Any, Dict, Optional

import pyarrow as pa
from ray.data.block import BlockAccessor
from ray.data.datasource.block_path_provider import BlockWritePathProvider

from awswrangler._arrow import _df_to_table
from awswrangler.distributed.ray.datasources.file_datasink import _BlockFileDatasink

_logger: logging.Logger = logging.getLogger(__name__)


class ParquetDatasink(_BlockFileDatasink):
    """A datasink that writes Parquet files."""

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
            file_format="parquet",
            block_path_provider=block_path_provider,
            dataset_uuid=dataset_uuid,
            s3_additional_kwargs=s3_additional_kwargs,
            pandas_kwargs=pandas_kwargs,
            **write_args,
        )

    def write_block(self, file: pa.NativeFile, block: BlockAccessor) -> None:
        """
        Write a block of data to a file.

        Parameters
        ----------
        block : BlockAccessor
        file : pa.NativeFile
        """
        write_args = self.write_args

        schema: pa.Schema = write_args.pop("schema", None)
        dtype: Optional[Dict[str, str]] = write_args.pop("dtype", None)
        index: bool = write_args.pop("index", False)
        pyarrow_additional_kwargs = write_args.pop("pyarrow_additional_kwargs", {})

        pa.parquet.write_table(
            _df_to_table(block.to_pandas(), schema=schema, index=index, dtype=dtype),
            file,
            **pyarrow_additional_kwargs,
        )
