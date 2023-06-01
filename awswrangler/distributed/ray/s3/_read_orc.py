from typing import TYPE_CHECKING, Dict, Optional, Union

import pyarrow as pa
from pyarrow.fs import _resolve_filesystem_and_path

from awswrangler import _utils
from awswrangler.s3._read_orc import _pyarrow_orc_file_wrapper

if TYPE_CHECKING:
    from mypy_boto3_s3 import S3Client


@_utils.retry(ex=OSError)
def _read_orc_metadata_file_distributed(
    s3_client: Optional["S3Client"],
    path: str,
    s3_additional_kwargs: Optional[Dict[str, str]],
    use_threads: Union[bool, int],
    version_id: Optional[str] = None,
) -> Optional[pa.schema]:
    resolved_filesystem, resolved_path = _resolve_filesystem_and_path(path)

    with resolved_filesystem.open_input_file(resolved_path) as f:
        orc_file = _pyarrow_orc_file_wrapper(
            source=f,
        )

        if orc_file:
            return orc_file.schema

    return None
