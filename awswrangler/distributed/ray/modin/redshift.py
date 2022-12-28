"""Amazon Redshift Module."""
# pylint: disable=too-many-lines

import logging
from typing import Any, Dict, Iterator, List, Optional, Tuple, Union

import pandas as pd
import pyarrow as pa
import redshift_connector

from awswrangler import exceptions
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
    unload_params: Optional[Dict[str, Any]] = None,
) -> Union[pd.DataFrame, Iterator[pd.DataFrame]]:
    if not unload_params:
        unload_params = {}
    path: Optional[str] = unload_params.get("path")
    if not path:
        raise exceptions.InvalidArgumentValue("<unload_params.path> argument must be provided when using Ray")
    max_file_size: Optional[float] = unload_params.get("max_file_size")
    if not max_file_size:
        max_file_size = 512.0
        _logger.warning(
            "Unload `MAXFILESIZE` is not specified. "
            "Defaulting to `512.0 MB` corresponding to the recommended Ray target block size."
        )
    manifest: bool = unload_params.get("manifest", False)
    unload_to_files(
        sql=sql,
        path=path,
        con=con,
        unload_format="PARQUET",
        manifest=manifest,
        max_file_size=max_file_size,
        iam_role=unload_params.get("iam_role"),
    )
    return read_parquet(
        path=_get_paths_from_manifest(f"{path}manifest") if manifest else path,
        parallelism=parallelism,
    )
