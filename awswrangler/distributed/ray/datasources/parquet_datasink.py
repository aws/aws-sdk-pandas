"""Ray ParquetDatasink Module."""

import logging
from typing import Any, Callable, Dict, List, Optional

import pyarrow as pa
from ray.data.block import BlockAccessor
from ray.data.datasource.block_path_provider import BlockWritePathProvider
from ray.data.datasource.file_based_datasource import _resolve_kwargs
from ray.data.datasource.file_datasink import BlockBasedFileDatasink
from ray.data.datasource.filename_provider import FilenameProvider

from awswrangler._arrow import _df_to_table

_logger: logging.Logger = logging.getLogger(__name__)


class ParquetDatasink(BlockBasedFileDatasink):
    """A datasink that writes Parquet files."""

    def __init__(
        self,
        path: str,
        *,
        arrow_parquet_args_fn: Callable[[], Dict[str, Any]] = lambda: {},
        arrow_parquet_args: Optional[Dict[str, Any]] = None,
        filesystem: Optional[pa.fs.FileSystem] = None,
        try_create_dir: bool = True,
        open_stream_args: Optional[Dict[str, Any]] = None,
        filename_provider: Optional[FilenameProvider] = None,
        block_path_provider: Optional[BlockWritePathProvider] = None,
        dataset_uuid: Optional[str] = None,
    ):
        if arrow_parquet_args is None:
            arrow_parquet_args = {}

        self.arrow_parquet_args_fn = arrow_parquet_args_fn
        self.arrow_parquet_args = arrow_parquet_args

        self._write_paths: List[str] = []

        super().__init__(
            path,
            filesystem=filesystem,
            try_create_dir=try_create_dir,
            open_stream_args=open_stream_args,
            filename_provider=filename_provider,
            block_path_provider=block_path_provider,
            dataset_uuid=dataset_uuid,
            file_format="parquet",
        )

    def write_block_to_file(self, block: BlockAccessor, file: pa.NativeFile) -> None:
        """
        Write a block of data to a file.

        Parameters
        ----------
        block : BlockAccessor
        file : pa.NativeFile
        """
        writer_args = _resolve_kwargs(self.arrow_parquet_args_fn, **self.arrow_parquet_args)

        schema: pa.Schema = writer_args.pop("schema", None)
        dtype: Optional[Dict[str, str]] = writer_args.pop("dtype", None)
        index: bool = writer_args.pop("index", False)

        pa.parquet.write_table(
            _df_to_table(block.to_pandas(), schema=schema, index=index, dtype=dtype),
            file,
            **writer_args,
        )

    # Note: this callback function is called once by the main thread after
    # [all write tasks complete](https://github.com/ray-project/ray/blob/ray-2.3.0/python/ray/data/dataset.py#L2716)
    # and is meant to be used for singular actions like
    # [committing a transaction](https://docs.ray.io/en/latest/data/api/doc/ray.data.Datasource.html).
    # As deceptive as it may look, there is no race condition here.
    def on_write_complete(self, write_results: List[Any], **_: Any) -> None:
        """Execute callback after all write tasks complete."""
        _logger.debug("Write complete %s.", write_results)

        # Collect and return all write task paths
        self._write_paths.extend(write_results)

    def get_write_paths(self) -> List[str]:
        """Return S3 paths of where the results have been written."""
        return self._write_paths
