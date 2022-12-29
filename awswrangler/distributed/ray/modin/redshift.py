"""Amazon Redshift Module."""
# pylint: disable=too-many-lines

import logging
from typing import Any, Dict, Iterator, List, Optional, Tuple, Union

import pandas as pd
import pyarrow as pa
import redshift_connector

from awswrangler import exceptions
from awswrangler.redshift import _write_manifest, copy_from_files, unload
from awswrangler.s3 import to_parquet

_logger: logging.Logger = logging.getLogger(__name__)


def _read_sql_query_distributed(  # pylint: disable=unused-argument
    sql: str,
    con: redshift_connector.Connection,
    index_col: Optional[Union[str, List[str]]],
    params: Optional[Union[List[Any], Tuple[Any, ...], Dict[Any, Any]]],
    chunksize: Optional[int],
    dtype: Optional[Dict[str, pa.DataType]],
    safe: bool,
    timestamp_as_object: bool,
    parallelism: int,
    unload_params: Optional[Dict[str, Any]],
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
    return unload(
        sql=sql,
        path=path,
        con=con,
        manifest=manifest,
        max_file_size=max_file_size,
        iam_role=unload_params.get("iam_role"),
    )


def _insert_distributed(  # pylint: disable=unused-argument
    df: pd.DataFrame,
    cursor: redshift_connector.Cursor,
    table: str,
    schema: str,
    mode: str,
    column_names: List[str],
    use_column_names: bool,
    chunksize: int,
    copy_params: Optional[Dict[str, Any]],
) -> None:
    if not copy_params:
        copy_params = {}
    path: Optional[str] = copy_params.get("path")
    if not path:
        raise exceptions.InvalidArgumentValue("<copy_params.path> argument must be provided when using Ray")
    to_parquet(df=df, path=path, dataset=True)
    manifest: bool = copy_params.get("manifest", False)
    if manifest:
        path = _write_manifest(path_root=path)
    copy_from_files(
        path=path,
        con=cursor.connection,
        table=table,
        schema=schema,
        mode=mode,
        commit_transaction=False,
        manifest=manifest,
        iam_role=copy_params.get("iam_role"),
    )
