from __future__ import annotations

from typing import TYPE_CHECKING

import pyarrow as pa
from pyarrow.fs import _resolve_filesystem_and_path

from awswrangler import _utils
from awswrangler.s3._read_parquet import _pyarrow_parquet_file_wrapper

if TYPE_CHECKING:
    from mypy_boto3_s3 import S3Client


@_utils.retry(ex=OSError)
def _read_parquet_metadata_file_distributed(
    s3_client: "S3Client" | None,
    path: str,
    s3_additional_kwargs: dict[str, str] | None,
    use_threads: bool | int,
    version_id: str | None = None,
    coerce_int96_timestamp_unit: str | None = None,
) -> pa.schema | None:
    resolved_filesystem, resolved_path = _resolve_filesystem_and_path(path)

    with resolved_filesystem.open_input_file(resolved_path) as f:
        pq_file = _pyarrow_parquet_file_wrapper(
            source=f,
            coerce_int96_timestamp_unit=coerce_int96_timestamp_unit,
        )

        if pq_file:
            return pq_file.schema.to_arrow_schema()

    return None
