from __future__ import annotations

from typing import TYPE_CHECKING

import pyarrow as pa
from pyarrow.fs import _resolve_filesystem_and_path

from awswrangler import _utils
from awswrangler.s3._read_orc import _pyarrow_orc_file_wrapper

if TYPE_CHECKING:
    from mypy_boto3_s3 import S3Client


@_utils.retry(ex=OSError)
def _read_orc_metadata_file_distributed(
    s3_client: "S3Client" | None,
    path: str,
    s3_additional_kwargs: dict[str, str] | None,
    use_threads: bool | int,
    version_id: str | None = None,
) -> pa.schema | None:
    resolved_filesystem, resolved_path = _resolve_filesystem_and_path(path)

    with resolved_filesystem.open_input_file(resolved_path) as f:
        orc_file = _pyarrow_orc_file_wrapper(
            source=f,
        )

        if orc_file:
            return orc_file.schema

    return None
