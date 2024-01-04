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
        s3_additional_kwargs: Optional[Dict[str, str]] = None,
        pandas_kwargs: Optional[Dict[str, Any]] = None,
        **write_args: Any,
    ):
        super().__init__(
            path,
            file_format="orc",
            block_path_provider=block_path_provider,
            dataset_uuid=dataset_uuid,
            s3_additional_kwargs=s3_additional_kwargs,
            pandas_kwargs=pandas_kwargs,
            **write_args,
        )

    def write_block(self, file: io.TextIOWrapper, block: BlockAccessor):
        """
        Write a block of data to a file.

        Parameters
        ----------
        file : io.TextIOWrapper
        block : BlockAccessor
        """
        from pyarrow import orc

        write_args = self.write_args

        schema: Optional[pa.schema] = write_args.get("schema", None)
        dtype: Optional[Dict[str, str]] = write_args.get("dtype", None)
        index: bool = write_args.get("index", False)
        compression: str = write_args.get("compression", None) or "UNCOMPRESSED"
        pyarrow_additional_kwargs: Dict[str, Any] = write_args.get("pyarrow_additional_kwargs", {})

        orc.write_table(
            _df_to_table(block.to_pandas(), schema=schema, index=index, dtype=dtype),
            file,
            compression=compression,
            **pyarrow_additional_kwargs,
        )
