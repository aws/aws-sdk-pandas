"""Ray BlockPathProvider Module."""
from typing import Optional

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
        filesystem: Optional["pyarrow.fs.FileSystem"] = None,
        dataset_uuid: Optional[str] = None,
        block: Optional[Block] = None,
        block_index: Optional[int] = None,
        task_index: Optional[int] = None,
        file_format: Optional[str] = None,
    ) -> str:
        return base_path
