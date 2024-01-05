"""Ray FileDatasink Module."""

import logging
from typing import Any, Dict, Iterable, List, Optional, Union

import pandas as pd
from ray.data._internal.delegating_block_builder import DelegatingBlockBuilder
from ray.data._internal.execution.interfaces import TaskContext
from ray.data.block import Block, BlockAccessor
from ray.data.datasource.block_path_provider import BlockWritePathProvider, DefaultBlockWritePathProvider
from ray.data.datasource.datasink import Datasink
from ray.types import ObjectRef

from awswrangler.distributed.ray import ray_get
from awswrangler.s3._fs import open_s3_object
from awswrangler.s3._write import _COMPRESSION_2_EXT

_logger: logging.Logger = logging.getLogger(__name__)


class _BlockFileDatasink(Datasink):
    def __init__(
        self,
        path: str,
        file_format: str,
        *,
        block_path_provider: Optional[BlockWritePathProvider] = DefaultBlockWritePathProvider(),
        dataset_uuid: Optional[str] = None,
        s3_additional_kwargs: Optional[Dict[str, str]] = None,
        pandas_kwargs: Optional[Dict[str, Any]] = None,
        **write_args: Any,
    ):
        self.path = path
        self.file_format = file_format
        self.block_path_provider = block_path_provider
        self.dataset_uuid = dataset_uuid
        self.s3_additional_kwargs = s3_additional_kwargs
        self.pandas_kwargs = pandas_kwargs or {}
        self.write_args = write_args or {}

        self._write_paths: List[str] = []

    def write(
        self,
        blocks: Iterable[Union[Block, ObjectRef[pd.DataFrame]]],
        ctx: TaskContext,
    ) -> Any:
        _write_block_to_file = self.write_block
        write_args = self.write_args

        mode = write_args.get("mode", "wb")
        compression = write_args.get("compression")
        encoding = write_args.get("encoding")
        newline = write_args.get("newline")

        def _write_block(write_path: str, block: pd.DataFrame) -> str:
            with open_s3_object(
                path=write_path,
                mode=mode,
                s3_additional_kwargs=self.s3_additional_kwargs,
                encoding=encoding,
                newline=newline,
            ) as f:
                _write_block_to_file(f, BlockAccessor.for_block(block))
                return write_path

        file_suffix = self._get_file_suffix(self.file_format, compression)

        builder = DelegatingBlockBuilder()  # type: ignore[no-untyped-call]
        for block in blocks:
            # Dereference the block if ObjectRef is passed
            builder.add_block(ray_get(block) if isinstance(block, ObjectRef) else block)  # type: ignore[arg-type]
        block = builder.build()

        write_path = self.block_path_provider(  # type: ignore[misc]
            self.path,
            dataset_uuid=self.dataset_uuid,
            block_index=ctx.task_idx,
            task_index=ctx.task_idx,
            file_format=file_suffix,
        )

        return _write_block(write_path, block)

    def write_block(self, file: Any, block: BlockAccessor) -> None:
        raise NotImplementedError

    def _get_file_suffix(self, file_format: str, compression: Optional[str]) -> str:
        return f"{file_format}{_COMPRESSION_2_EXT.get(compression)}"

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
