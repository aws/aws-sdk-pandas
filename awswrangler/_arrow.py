"""Arrow Utilities Module (PRIVATE)."""

import datetime
import json
import logging
from typing import Any, Dict, Optional, Tuple, cast

import pyarrow as pa
import pytz

import awswrangler.pandas as pd
from awswrangler._data_types import athena2pyarrow

_logger: logging.Logger = logging.getLogger(__name__)


def _extract_partitions_from_path(path_root: str, path: str) -> Dict[str, str]:
    path_root = path_root if path_root.endswith("/") else f"{path_root}/"
    if path_root not in path:
        raise Exception(f"Object {path} is not under the root path ({path_root}).")
    path_wo_filename: str = path.rpartition("/")[0] + "/"
    path_wo_prefix: str = path_wo_filename.replace(f"{path_root}/", "")
    dirs: Tuple[str, ...] = tuple(x for x in path_wo_prefix.split("/") if x and (x.count("=") > 0))
    if not dirs:
        return {}
    values_tups = cast(Tuple[Tuple[str, str]], tuple(tuple(x.split("=", maxsplit=1)[:2]) for x in dirs))
    values_dics: Dict[str, str] = dict(values_tups)
    return values_dics


def _add_table_partitions(
    table: pa.Table,
    path: str,
    path_root: Optional[str],
) -> pa.Table:
    part = _extract_partitions_from_path(path_root, path) if path_root else None
    if part:
        for col, value in part.items():
            part_value = pa.array([value] * len(table)).dictionary_encode()
            if col not in table.schema.names:
                table = table.append_column(col, part_value)
            else:
                table = table.set_column(
                    table.schema.get_field_index(col),
                    col,
                    part_value,
                )
    return table


def ensure_df_is_mutable(df: pd.DataFrame) -> pd.DataFrame:
    """Ensure that all columns has the writeable flag True."""
    for column in df.columns.to_list():
        if hasattr(df[column].values, "flags") is True:
            if df[column].values.flags.writeable is False:
                s: pd.Series = df[column]
                df[column] = None
                df[column] = s
    return df


def _apply_timezone(df: pd.DataFrame, metadata: Dict[str, Any]) -> pd.DataFrame:
    for c in metadata["columns"]:
        if "field_name" in c and c["field_name"] is not None:
            col_name = str(c["field_name"])
        elif "name" in c and c["name"] is not None:
            col_name = str(c["name"])
        else:
            continue
        if col_name in df.columns and c["pandas_type"] == "datetimetz":
            timezone: datetime.tzinfo = pa.lib.string_to_tzinfo(c["metadata"]["timezone"])
            _logger.debug("applying timezone (%s) on column %s", timezone, col_name)
            if hasattr(df[col_name].dtype, "tz") is False:
                df[col_name] = df[col_name].dt.tz_localize(tz="UTC")
            if timezone is not None and timezone != pytz.UTC and hasattr(df[col_name].dt, "tz_convert"):
                df[col_name] = df[col_name].dt.tz_convert(tz=timezone)
    return df


def _table_to_df(
    table: pa.Table,
    kwargs: Dict[str, Any],
) -> pd.DataFrame:
    """Convert a PyArrow table to a Pandas DataFrame and apply metadata.

    This method should be used across to codebase to ensure this conversion is consistent.
    """
    metadata: Dict[str, Any] = {}
    if table.schema.metadata is not None and b"pandas" in table.schema.metadata:
        metadata = json.loads(table.schema.metadata[b"pandas"])

    df = table.to_pandas(**kwargs)

    df = ensure_df_is_mutable(df=df)
    if metadata:
        _logger.debug("metadata: %s", metadata)
        df = _apply_timezone(df=df, metadata=metadata)
    return df


def _df_to_table(
    df: pd.DataFrame,
    schema: Optional[pa.Schema] = None,
    index: Optional[bool] = None,
    dtype: Optional[Dict[str, str]] = None,
    cpus: Optional[int] = None,
) -> pa.Table:
    table: pa.Table = pa.Table.from_pandas(df=df, schema=schema, nthreads=cpus, preserve_index=index, safe=True)
    if dtype:
        for col_name, col_type in dtype.items():
            if col_name in table.column_names:
                col_index = table.column_names.index(col_name)
                pyarrow_dtype = athena2pyarrow(col_type)
                field = pa.field(name=col_name, type=pyarrow_dtype)
                table = table.set_column(col_index, field, table.column(col_name).cast(pyarrow_dtype))
                _logger.debug("Casting column %s (%s) to %s (%s)", col_name, col_index, col_type, pyarrow_dtype)
    return table
