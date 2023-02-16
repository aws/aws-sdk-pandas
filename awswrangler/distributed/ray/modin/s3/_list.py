import datetime
from typing import TYPE_CHECKING, Any, Dict, Iterator, List, Optional, Union

from pyarrow.fs import FileSelector, _resolve_filesystem_and_path

from awswrangler import exceptions
from awswrangler.s3._list import _validate_datetimes

if TYPE_CHECKING:
    from mypy_boto3_s3 import S3Client


def _list_objects_filesystem(  # pylint: disable=unused-argument
    path: str,
    s3_client: Optional["S3Client"],
    s3_additional_kwargs: Optional[Dict[str, Any]],
    delimiter: Optional[str] = None,
    suffix: Union[str, List[str], None] = None,
    ignore_suffix: Union[str, List[str], None] = None,
    last_modified_begin: Optional[datetime.datetime] = None,
    last_modified_end: Optional[datetime.datetime] = None,
    ignore_empty: bool = False,
) -> Iterator[List[str]]:
    """Expand the provided directory path to a list of file paths."""
    if s3_additional_kwargs:
        raise exceptions.InvalidArgument(f"Additional S3 args specified: {s3_additional_kwargs}")

    filesystem, path = _resolve_filesystem_and_path(path, None)

    selector = FileSelector(path, recursive=True)
    files = filesystem.get_file_info(selector)
    base_path = selector.base_dir
    filtered_paths = []

    _suffix: Union[List[str], None] = [suffix] if isinstance(suffix, str) else suffix
    _ignore_suffix: Union[List[str], None] = [ignore_suffix] if isinstance(ignore_suffix, str) else ignore_suffix
    _validate_datetimes(last_modified_begin=last_modified_begin, last_modified_end=last_modified_end)

    for file_ in files:
        if not file_.is_file:
            continue
        if ignore_empty and file_.size == 0:
            continue
        file_path = file_.path
        if not file_path.startswith(base_path):
            continue
        if (_ignore_suffix is not None) and file_path.endswith(tuple(_ignore_suffix)):
            continue
        if (_suffix is None) or file_path.endswith(tuple(_suffix)):
            if last_modified_begin is not None:
                if file_.mtime < last_modified_begin:
                    continue
            if last_modified_end is not None:
                if file_.mtime > last_modified_end:
                    continue
            filtered_paths.append(f"s3://{file_path}")

        if filtered_paths:
            yield filtered_paths
        filtered_paths = []
