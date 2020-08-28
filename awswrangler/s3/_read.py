"""Amazon S3 Read Module (PRIVATE)."""

import logging
from typing import Any, Callable, Dict, List, Optional, Set, Tuple, Union, cast

import numpy as np
import pandas as pd
from pandas.api.types import union_categoricals

from awswrangler import exceptions
from awswrangler.s3._list import _prefix_cleanup

_logger: logging.Logger = logging.getLogger(__name__)


def _get_path_root(path: Union[str, List[str]], dataset: bool) -> Optional[str]:
    if (dataset is True) and (not isinstance(path, str)):
        raise exceptions.InvalidArgument("The path argument must be a string if dataset=True (Amazon S3 prefix).")
    return _prefix_cleanup(str(path)) if dataset is True else None


def _get_path_ignore_suffix(path_ignore_suffix: Union[str, List[str], None]) -> Union[List[str], None]:
    if isinstance(path_ignore_suffix, str):
        path_ignore_suffix = [path_ignore_suffix, "/_SUCCESS"]
    elif path_ignore_suffix is None:
        path_ignore_suffix = ["/_SUCCESS"]
    else:
        path_ignore_suffix = path_ignore_suffix + ["/_SUCCESS"]
    return path_ignore_suffix


def _extract_partitions_metadata_from_paths(
    path: str, paths: List[str]
) -> Tuple[Optional[Dict[str, str]], Optional[Dict[str, List[str]]]]:
    """Extract partitions metadata from Amazon S3 paths."""
    path = path if path.endswith("/") else f"{path}/"
    partitions_types: Dict[str, str] = {}
    partitions_values: Dict[str, List[str]] = {}
    for p in paths:
        if path not in p:
            raise exceptions.InvalidArgumentValue(f"Object {p} is not under the root path ({path}).")
        path_wo_filename: str = p.rpartition("/")[0] + "/"
        if path_wo_filename not in partitions_values:
            path_wo_prefix: str = path_wo_filename.replace(f"{path}/", "")
            dirs: Tuple[str, ...] = tuple(x for x in path_wo_prefix.split("/") if (x != "") and (x.count("=") == 1))
            if dirs:
                values_tups = cast(Tuple[Tuple[str, str]], tuple(tuple(x.split("=")[:2]) for x in dirs))
                values_dics: Dict[str, str] = dict(values_tups)
                p_values: List[str] = list(values_dics.values())
                p_types: Dict[str, str] = {x: "string" for x in values_dics.keys()}
                if not partitions_types:
                    partitions_types = p_types
                if p_values:
                    partitions_types = p_types
                    partitions_values[path_wo_filename] = p_values
                elif p_types != partitions_types:
                    raise exceptions.InvalidSchemaConvergence(
                        f"At least two different partitions schema detected: {partitions_types} and {p_types}"
                    )
    if not partitions_types:
        return None, None
    return partitions_types, partitions_values


def _extract_partitions_from_path(path_root: str, path: str) -> Dict[str, str]:
    """Extract partitions values and names from Amazon S3 path."""
    path_root = path_root if path_root.endswith("/") else f"{path_root}/"
    if path_root not in path:
        raise exceptions.InvalidArgumentValue(f"Object {path} is not under the root path ({path_root}).")
    path_wo_filename: str = path.rpartition("/")[0] + "/"
    path_wo_prefix: str = path_wo_filename.replace(f"{path_root}/", "")
    dirs: Tuple[str, ...] = tuple(x for x in path_wo_prefix.split("/") if (x != "") and (x.count("=") == 1))
    if not dirs:
        return {}
    values_tups = cast(Tuple[Tuple[str, str]], tuple(tuple(x.split("=")[:2]) for x in dirs))
    values_dics: Dict[str, str] = dict(values_tups)
    return values_dics


def _apply_partition_filter(
    path_root: str, paths: List[str], filter_func: Optional[Callable[[Dict[str, str]], bool]]
) -> List[str]:
    if filter_func is None:
        return paths
    return [p for p in paths if filter_func(_extract_partitions_from_path(path_root=path_root, path=p)) is True]


def _apply_partitions(df: pd.DataFrame, dataset: bool, path: str, path_root: Optional[str]) -> pd.DataFrame:
    if dataset is False:
        return df
    if dataset is True and path_root is None:
        raise exceptions.InvalidArgument("A path_root is required when dataset=True.")
    path_root = cast(str, path_root)
    partitions: Dict[str, str] = _extract_partitions_from_path(path_root=path_root, path=path)
    _logger.debug("partitions: %s", partitions)
    count: int = len(df.index)
    _logger.debug("count: %s", count)
    for name, value in partitions.items():
        df[name] = pd.Categorical.from_codes(np.repeat([0], count), categories=[value])
    return df


def _extract_partitions_dtypes_from_table_details(response: Dict[str, Any]) -> Dict[str, str]:
    dtypes: Dict[str, str] = {}
    if "PartitionKeys" in response["Table"]:
        for par in response["Table"]["PartitionKeys"]:
            dtypes[par["Name"]] = par["Type"]
    return dtypes


def _union(dfs: List[pd.DataFrame], ignore_index: Optional[bool]) -> pd.DataFrame:
    if ignore_index is None:
        ignore_index = False
        for df in dfs:
            if hasattr(df, "_awswrangler_ignore_index"):
                if df._awswrangler_ignore_index is True:  # pylint: disable=protected-access
                    ignore_index = True
                    break
    cats: Tuple[Set[str], ...] = tuple(set(df.select_dtypes(include="category").columns) for df in dfs)
    for col in set.intersection(*cats):
        cat = union_categoricals([df[col] for df in dfs])
        for df in dfs:
            df[col] = pd.Categorical(df[col].values, categories=cat.categories)
    return pd.concat(objs=dfs, sort=False, copy=False, ignore_index=ignore_index)
