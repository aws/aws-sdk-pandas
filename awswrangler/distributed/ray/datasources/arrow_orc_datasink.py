"""Ray PandasTextDatasink Module."""

import io
import logging
from typing import Any, Dict, Optional

import pyarrow as pa
from ray.data.block import BlockAccessor
from ray.data.datasource.block_path_provider import BlockWritePathProvider

from awswrangler._arrow import _df_to_table
from awswrangler.distributed.ray.datasources.file_datasink import _BlockFileDatasink

_logger: logging.Logger = logging.getLogger(__name__)


class ArrowORCDatasink(_BlockFileDatasink):
    """A datasink that writes CSV files using Arrow."""

    def __init__(
        self,
        path: str,
        *,
        block_path_provider: Optional[BlockWritePathProvider] = None,
        dataset_uuid: Optional[str] = None,
        open_s3_object_args: Optional[Dict[str, Any]] = None,
        pandas_kwargs: Optional[Dict[str, Any]] = None,
        schema: Optional[pa.Schema] = None,
        index: bool = False,
        dtype: Optional[Dict[str, str]] = None,
        pyarrow_additional_kwargs: Optional[Dict[str, Any]] = None,
        **write_args: Any,
    ):
        super().__init__(
            path,
            file_format="orc",
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

    def write_block(self, file: io.TextIOWrapper, block: BlockAccessor) -> None:
        """
        Write a block of data to a file.

        Parameters
        ----------
        file : io.TextIOWrapper
        block : BlockAccessor
        """
        from pyarrow import orc

        compression: str = self.write_args.get("compression", None) or "UNCOMPRESSED"

        orc.write_table(
            _df_to_table(block.to_pandas(), schema=self.schema, index=self.index, dtype=self.dtype),
            file,
            compression=compression,
            **self.pyarrow_additional_kwargs,
        )
