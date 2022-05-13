"""Ray Utils (PRIVATE)."""

import importlib.util
import posixpath

from awswrangler.s3._write import _get_file_path

_ray_found = importlib.util.find_spec("ray")
if _ray_found:
    from ray.data.datasource.file_based_datasource import BlockWritePathProvider


class CustomBlockWritePathProvider(BlockWritePathProvider):
    def __init__(self, file_name: str, num_files: int) -> None:
        self._file_name = file_name
        self._num_files = num_files

    def get_file_path(self, file_path: str, num_files: int, index: int) -> str:
        return _get_file_path(file_counter=index, file_path=file_path) if num_files > 1 else file_path

    def _get_write_path_for_block(  # type: ignore
        self,
        base_path: str,
        block_index: int,
        **args,
    ) -> str:
        file_path = posixpath.join(base_path, self._file_name)
        return self.get_file_path(file_path=file_path, num_files=self._num_files, index=block_index)
