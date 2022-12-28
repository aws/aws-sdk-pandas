"""Amazon Redshift Module."""
# pylint: disable=too-many-lines

import logging
from typing import Any, Dict, Iterator, List, Optional, Tuple, Union

import pandas as pd
import pyarrow as pa
import redshift_connector

from awswrangler import _utils, exceptions, s3
from awswrangler.redshift import _get_paths_from_manifest, unload_to_files
from awswrangler.s3 import read_parquet

_logger: logging.Logger = logging.getLogger(__name__)


def _read_sql_query_distributed(  # pylint: disable=unused-argument
    sql: str,
    con: redshift_connector.Connection,
    index_col: Optional[Union[str, List[str]]] = None,
    params: Optional[Union[List[Any], Tuple[Any, ...], Dict[Any, Any]]] = None,
    chunksize: Optional[int] = None,
    dtype: Optional[Dict[str, pa.DataType]] = None,
    safe: bool = True,
    timestamp_as_object: bool = False,
    parallelism: int = -1,
    unload_path: Optional[str] = None,
    unload_manifest: bool = False,
    unload_max_file_size: Optional[float] = None,
    unload_iam_role: Optional[str] = None,
) -> Union[pd.DataFrame, Iterator[pd.DataFrame]]:
    if not unload_path:
        raise exceptions.InvalidArgumentValue("<unload_path> argument must be provided when using Ray")
    if not unload_max_file_size:
        unload_max_file_size = 512.0
        _logger.warning(
            "Unload `MAXFILESIZE` is not specified. "
            "Defaulting to `512.0 MB` corresponding to the recommended Ray target block size."
        )
    unload_to_files(
        sql=sql,
        path=unload_path,
        con=con,
        unload_format="PARQUET",
        manifest=unload_manifest,
        max_file_size=unload_max_file_size,
        iam_role=unload_iam_role,
    )
    return read_parquet(
        path=_get_paths_from_manifest(f"{unload_path}manifest") if unload_manifest else unload_path,
        parallelism=parallelism,
    )
