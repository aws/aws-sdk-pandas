"""Ray FileDatasink Module."""

from __future__ import annotations

import logging
import posixpath
from typing import Any, Iterable

import pandas as pd
from ray.data._internal.delegating_block_builder import DelegatingBlockBuilder
from ray.data._internal.execution.interfaces import TaskContext
from ray.data.block import Block, BlockAccessor
from ray.data.datasource.datasink import Datasink
from ray.data.datasource.filename_provider import FilenameProvider
from ray.types import ObjectRef

from awswrangler.distributed.ray import ray_get
from awswrangler.distributed.ray.datasources.filename_provider import _DefaultFilenameProvider
from awswrangler.s3._fs import open_s3_object

_logger: logging.Logger = logging.getLogger(__name__)


class _BlockFileDatasink(Datasink):
    def __init__(
        self,
        path: str,
        file_format: str,
        *,
        filename_provider: FilenameProvider | None = None,
        dataset_uuid: str | None = None,
        open_s3_object_args: dict[str, Any] | None = None,
        pandas_kwargs: dict[str, Any] | None = None,
        **write_args: Any,
    ):
        self.path = path
        self.file_format = file_format
        self.dataset_uuid = dataset_uuid
        self.open_s3_object_args = open_s3_object_args or {}
        self.pandas_kwargs = pandas_kwargs or {}
        self.write_args = write_args or {}

        if filename_provider is None:
            compression = self.pandas_kwargs.get("compression", None)
            bucket_id = self.write_args.get("bucket_id", None)

            filename_provider = _DefaultFilenameProvider(
                dataset_uuid=dataset_uuid,
                file_format=file_format,
                compression=compression,
                bucket_id=bucket_id,
            )

        self.filename_provider = filename_provider
        self._write_paths: list[str] = []

    def write(
        self,
        blocks: Iterable[Block | ObjectRef[pd.DataFrame]],
        ctx: TaskContext,
    ) -> Any:
        _write_block_to_file = self.write_block

        def _write_block(write_path: str, block: pd.DataFrame) -> str:
            with open_s3_object(
                path=write_path,
                **self.open_s3_object_args,
            ) as f:
                _write_block_to_file(f, BlockAccessor.for_block(block))
                return write_path

        builder = DelegatingBlockBuilder()  # type: ignore[no-untyped-call]
        for block in blocks:
            # Dereference the block if ObjectRef is passed
            builder.add_block(ray_get(block) if isinstance(block, ObjectRef) else block)  # type: ignore[arg-type]
        block = builder.build()

        write_path = self.path

        if write_path.endswith("/"):
            filename = self.filename_provider.get_filename_for_block(block, ctx.task_idx, 0)
            write_path = posixpath.join(self.path, filename)

        return _write_block(write_path, block)

    def write_block(self, file: Any, block: BlockAccessor) -> None:
        raise NotImplementedError

    # Note: this callback function is called once by the main thread after
    # [all write tasks complete](https://github.com/ray-project/ray/blob/ray-2.3.0/python/ray/data/dataset.py#L2716)
    # and is meant to be used for singular actions like
    # [committing a transaction](https://docs.ray.io/en/latest/data/api/doc/ray.data.Datasource.html).
    # As deceptive as it may look, there is no race condition here.
    def on_write_complete(self, write_results: list[Any], **_: Any) -> None:
        """Execute callback after all write tasks complete."""
        _logger.debug("Write complete %s.", write_results)

        # Collect and return all write task paths
        self._write_paths.extend(write_results)

    def get_write_paths(self) -> list[str]:
        """Return S3 paths of where the results have been written."""
        return self._write_paths
