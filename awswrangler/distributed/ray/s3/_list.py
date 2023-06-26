"""Ray S3 List module (PRIVATE)."""
import datetime
import fnmatch
import logging
from typing import TYPE_CHECKING, Any, Dict, Iterator, List, Optional, Union

from pyarrow.fs import FileSelector, FileType, _resolve_filesystem_and_path

if TYPE_CHECKING:
    from mypy_boto3_s3 import S3Client

_logger: logging.Logger = logging.getLogger(__name__)


def _list_objects_s3fs(  # pylint: disable=unused-argument
    bucket: str,
    pattern: str,
    prefix: str,
    s3_client: "S3Client",
    delimiter: Optional[str],
    s3_additional_kwargs: Optional[Dict[str, Any]],
    suffix: Union[List[str], None],
    ignore_suffix: Union[List[str], None],
    last_modified_begin: Optional[datetime.datetime],
    last_modified_end: Optional[datetime.datetime],
    ignore_empty: bool,
) -> Iterator[List[str]]:
    """Expand the provided S3 directory path to a list of object paths."""
    resolved_filesystem, resolved_path = _resolve_filesystem_and_path(f"s3://{bucket}/{prefix}", None)
    paths: List[str] = []

    path_info = resolved_filesystem.get_file_info(resolved_path)

    if path_info.type in (FileType.File, FileType.Directory):
        if path_info.type == FileType.File:
            files = [path_info]
            base_path = resolved_path
        else:
            selector = FileSelector(resolved_path, recursive=True)
            files = resolved_filesystem.get_file_info(selector)
            base_path = selector.base_dir

        for file_ in files:
            if not file_.is_file:
                continue
            if ignore_empty and file_.size == 0:
                continue
            file_path = file_.path
            if not file_path.startswith(base_path):
                continue
            if (ignore_suffix is not None) and file_path.endswith(tuple(ignore_suffix)):
                continue
            if (suffix is None) or file_path.endswith(tuple(suffix)):
                if last_modified_begin is not None:
                    if file_.mtime < last_modified_begin:
                        continue
                if last_modified_end is not None:
                    if file_.mtime > last_modified_end:
                        continue
                paths.append(f"s3://{file_path}")

            if prefix != pattern:
                paths = fnmatch.filter(paths, f"s3://{bucket}/{pattern}")

            if paths:
                yield paths
            paths = []
