"""Modin on Ray S3 write ORC module (PRIVATE)."""
import logging
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Union

import modin.pandas as pd
import pyarrow as pa

if TYPE_CHECKING:
    from mypy_boto3_s3 import S3Client

_logger: logging.Logger = logging.getLogger(__name__)


def _to_orc_distributed(  # pylint: disable=unused-argument
    df: pd.DataFrame,
    schema: "pa.Schema",
    index: bool,
    compression: Optional[str],
    compression_ext: str,
    pyarrow_additional_kwargs: Optional[Dict[str, Any]],
    cpus: int,
    dtype: Dict[str, str],
    s3_client: Optional["S3Client"],
    s3_additional_kwargs: Optional[Dict[str, str]],
    use_threads: Union[bool, int],
    path: Optional[str] = None,
    path_root: Optional[str] = None,
    filename_prefix: Optional[str] = "",
    max_rows_by_file: Optional[int] = 0,
    bucketing: bool = False,
) -> List[str]:
    raise NotImplementedError()
