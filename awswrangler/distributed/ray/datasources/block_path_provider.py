"""Ray BlockPathProvider Module."""
from __future__ import annotations

import pyarrow
from ray.data.block import Block
from ray.data.datasource.block_path_provider import BlockWritePathProvider


class UserProvidedKeyBlockWritePathProvider(BlockWritePathProvider):
    """Block write path provider.

    Used when writing single-block datasets into a user-provided S3 key.
    """

    def _get_write_path_for_block(
        self,
        base_path: str,
        *,
        filesystem: "pyarrow.fs.FileSystem" | None = None,
        dataset_uuid: str | None = None,
        block: Block | None = None,
        block_index: int | None = None,
        task_index: int | None = None,
        file_format: str | None = None,
    ) -> str:
        return base_path
