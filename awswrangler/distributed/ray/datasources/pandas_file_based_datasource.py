"""Ray PandasFileBasedDatasource Module."""
from dataclasses import dataclass
import logging
from typing import Any, Callable, Dict, Iterable, List, Optional

import pandas as pd
import pyarrow
from progressbar import ProgressBar
from ray.data._internal.delegating_block_builder import DelegatingBlockBuilder
from ray.data.block import Block, BlockAccessor, BlockMetadata
from ray.data.datasource.datasource import WriteResult
from ray.data.datasource.file_based_datasource import (
    BlockWritePathProvider,
    DefaultBlockWritePathProvider,
    FileBasedDatasource,
)
from ray.types import ObjectRef

from awswrangler.distributed.ray import ray_remote
from awswrangler.s3._fs import open_s3_object
from awswrangler.s3._write import _COMPRESSION_2_EXT

_logger: logging.Logger = logging.getLogger(__name__)


class UserProvidedKeyBlockWritePathProvider(BlockWritePathProvider):
    """Block write path provider.

    Used when writing single-block datasets into a user-provided S3 key.
    """

    def _get_write_path_for_block(  # type: ignore[override]
        self,
        base_path: str,
        *,
        filesystem: Optional["pyarrow.fs.FileSystem"] = None,
        dataset_uuid: Optional[str] = None,
        block: Optional[Block] = None,
        block_index: Optional[int] = None,
        file_format: Optional[str] = None,
    ) -> str:
        return base_path
    

@dataclass
class TaskContext:
    """Describes the information of a task running block transform."""

    # The index of task. Each task has a unique task index within the same
    # operator.
    task_idx: int

    # The dictionary of sub progress bar to update. The key is name of sub progress
    # bar. Note this is only used on driver side.
    sub_progress_bar_dict: Optional[Dict[str, ProgressBar]] = None


class PandasFileBasedDatasource(FileBasedDatasource):  # pylint: disable=abstract-method
    """Pandas file based datasource, for reading and writing Pandas blocks."""

    _FILE_EXTENSION: str

    def __init__(self) -> None:
        super().__init__()

        self._write_paths: List[str] = []

    def _read_file(self, f: pyarrow.NativeFile, path: str, **reader_args: Any) -> pd.DataFrame:
        raise NotImplementedError()
    
    def write(
        self,
        blocks: Iterable[Block],
        ctx: TaskContext,
        path: str,
        dataset_uuid: str,
        filesystem: Optional[pyarrow.fs.FileSystem] = None,
        try_create_dir: bool = True,
        open_stream_args: Optional[Dict[str, Any]] = None,
        block_path_provider: BlockWritePathProvider = DefaultBlockWritePathProvider(),
        write_args_fn: Callable[[], Dict[str, Any]] = lambda: {},
        _block_udf: Optional[Callable[[pd.DataFrame], pd.DataFrame]] = None,
        ray_remote_args: Optional[Dict[str, Any]] = None,
        s3_additional_kwargs: Optional[Dict[str, str]] = None,
        pandas_kwargs: Optional[Dict[str, Any]] = None,
        compression: Optional[str] = None,
        mode: str = "wb",
        **write_args: Any,
    ) -> WriteResult:
        """Write blocks for a file-based datasource."""
        _write_block_to_file = self._write_block

        if ray_remote_args is None:
            ray_remote_args = {}

        if pandas_kwargs is None:
            pandas_kwargs = {}

        if not compression:
            compression = pandas_kwargs.get("compression")

        def write_block(write_path: str, block: pd.DataFrame) -> str:
            if _block_udf is not None:
                block = _block_udf(block)

            with open_s3_object(
                path=write_path,
                mode=mode,
                use_threads=False,
                s3_additional_kwargs=s3_additional_kwargs,
                encoding=write_args.get("encoding"),
                newline=write_args.get("newline"),
            ) as f:
                _write_block_to_file(
                    f,
                    BlockAccessor.for_block(block),
                    pandas_kwargs=pandas_kwargs,
                    compression=compression,
                    **write_args,
                )
                return write_path
            
        file_suffix = self._get_file_suffix(self._FILE_EXTENSION, compression)

        builder = DelegatingBlockBuilder()
        for block in blocks:
            builder.add_block(block)
        block = builder.build()

        write_path = block_path_provider(
            path,
            filesystem=filesystem,
            dataset_uuid=dataset_uuid,
            block=block,
            block_index=ctx.task_idx,
            file_format=file_suffix,
        )

        return write_block(write_path, block)

    def _get_file_suffix(self, file_format: str, compression: Optional[str]) -> str:
        return f"{file_format}{_COMPRESSION_2_EXT.get(compression)}"

    def _write_block(
        self,
        f: "pyarrow.NativeFile",
        block: BlockAccessor[Any],
        writer_args_fn: Callable[[], Dict[str, Any]] = lambda: {},
        **writer_args: Any,
    ) -> None:
        raise NotImplementedError("Subclasses of PandasFileBasedDatasource must implement _write_block().")

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

    def on_write_failed(self, write_results: List[ObjectRef[Any]], error: Exception, **_: Any) -> None:
        """Execute callback after write tasks fail."""
        _logger.debug("Write failed %s.", write_results)
        raise error

    def get_write_paths(self) -> List[str]:
        """Return S3 paths of where the results have been written."""
        return self._write_paths
