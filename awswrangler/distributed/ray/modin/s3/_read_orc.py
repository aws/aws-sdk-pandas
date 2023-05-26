"""Modin on Ray S3 read ORC module (PRIVATE)."""
from typing import TYPE_CHECKING, Any, Dict, Iterator, List, Optional, Union

import modin.pandas as pd
import pyarrow as pa

if TYPE_CHECKING:
    from mypy_boto3_s3 import S3Client


def _read_orc_distributed(  # pylint: disable=unused-argument
    paths: List[str],
    path_root: Optional[str],
    schema: Optional[pa.schema],
    columns: Optional[List[str]],
    use_threads: Union[bool, int],
    parallelism: int,
    version_ids: Optional[Dict[str, str]],
    s3_client: Optional["S3Client"],
    s3_additional_kwargs: Optional[Dict[str, Any]],
    arrow_kwargs: Dict[str, Any],
    bulk_read: bool,
) -> Union[pd.DataFrame, Iterator[pd.DataFrame]]:
    raise NotImplementedError()
