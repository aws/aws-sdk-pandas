"""Ray DefaultFilenameProvider Module."""

from __future__ import annotations

from typing import Any

from ray.data.block import Block
from ray.data.datasource.filename_provider import FilenameProvider

from awswrangler.s3._write import _COMPRESSION_2_EXT


class _DefaultFilenameProvider(FilenameProvider):
    def __init__(
        self,
        file_format: str,
        dataset_uuid: str | None = None,
        compression: str | None = None,
        bucket_id: int | None = None,
    ):
        self._dataset_uuid = dataset_uuid
        self._file_format = file_format
        self._compression = compression
        self._bucket_id = bucket_id

    def get_filename_for_block(
        self,
        block: Block,
        task_index: int,
        block_index: int,
    ) -> str:
        file_id = f"{task_index:06}_{block_index:06}"
        return self._generate_filename(file_id)

    def get_filename_for_row(self, row: dict[str, Any], task_index: int, block_index: int, row_index: int) -> str:
        file_id = f"{task_index:06}_{block_index:06}_{row_index:06}"
        return self._generate_filename(file_id)

    def _generate_filename(self, file_id: str) -> str:
        filename = ""
        if self._dataset_uuid is not None:
            filename += f"{self._dataset_uuid}_"
        filename += f"{file_id}"
        if self._bucket_id is not None:
            filename += f"_bucket-{self._bucket_id:05d}"
        filename += f".{self._file_format}{_COMPRESSION_2_EXT.get(self._compression)}"
        return filename
