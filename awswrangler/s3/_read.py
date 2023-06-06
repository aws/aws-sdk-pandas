"""Amazon S3 Read Module (PRIVATE)."""

import itertools
import logging
from abc import ABC, abstractmethod
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    Iterable,
    Iterator,
    List,
    NamedTuple,
    Optional,
    Set,
    Tuple,
    Union,
    cast,
)

import boto3
import numpy as np
import pandas as pd
import pyarrow as pa
from pandas.api.types import union_categoricals

from awswrangler import _data_types, _utils, exceptions
from awswrangler._arrow import _extract_partitions_from_path
from awswrangler._executor import _BaseExecutor, _get_executor
from awswrangler.catalog._get import _get_partitions
from awswrangler.catalog._utils import _catalog_id
from awswrangler.distributed.ray import ray_get
from awswrangler.s3._list import _path2list, _prefix_cleanup

if TYPE_CHECKING:
    from mypy_boto3_glue.type_defs import GetTableResponseTypeDef
    from mypy_boto3_s3 import S3Client

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
            path_wo_prefix: str = path_wo_filename.replace(f"{path}", "")
            dirs: Tuple[str, ...] = tuple(x for x in path_wo_prefix.split("/") if x and (x.count("=") > 0))
            if dirs:
                values_tups = cast(Tuple[Tuple[str, str]], tuple(tuple(x.split("=", maxsplit=1)[:2]) for x in dirs))
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
    partitions: Dict[str, str] = _extract_partitions_from_path(path_root=path_root, path=path)
    _logger.debug("partitions: %s", partitions)
    count: int = len(df.index)
    _logger.debug("count: %s", count)
    for name, value in partitions.items():
        df[name] = pd.Categorical.from_codes(np.repeat([0], count), categories=[value])
    return df


def _extract_partitions_dtypes_from_table_details(response: "GetTableResponseTypeDef") -> Dict[str, str]:
    dtypes: Dict[str, str] = {}
    for par in response["Table"].get("PartitionKeys", []):
        dtypes[par["Name"]] = par["Type"]
    return dtypes


def _union(dfs: List[pd.DataFrame], ignore_index: bool) -> pd.DataFrame:
    cats: Tuple[Set[str], ...] = tuple(set(df.select_dtypes(include="category").columns) for df in dfs)
    for col in set.intersection(*cats):
        cat = union_categoricals([df[col] for df in dfs])
        for df in dfs:
            df[col] = pd.Categorical(df[col].values, categories=cat.categories)
    return pd.concat(objs=dfs, sort=False, copy=False, ignore_index=ignore_index)


def _check_version_id(
    paths: List[str], version_id: Optional[Union[str, Dict[str, str]]] = None
) -> Optional[Dict[str, str]]:
    if len(paths) > 1 and version_id is not None and not isinstance(version_id, dict):
        raise exceptions.InvalidArgumentCombination(
            "If multiple paths are provided along with a file version ID, the version ID parameter must be a dict."
        )
    if isinstance(version_id, dict) and not all(version_id.values()):
        raise exceptions.InvalidArgumentValue("Values in version ID dict cannot be None.")
    return (
        version_id if isinstance(version_id, dict) else {paths[0]: version_id} if isinstance(version_id, str) else None
    )


class _InternalReadTableMetadataReturnValue(NamedTuple):
    columns_types: Dict[str, str]
    partitions_types: Optional[Dict[str, str]]
    partitions_values: Optional[Dict[str, List[str]]]


class _TableMetadataReader(ABC):
    @abstractmethod
    def _read_metadata_file(
        self,
        s3_client: Optional["S3Client"],
        path: str,
        s3_additional_kwargs: Optional[Dict[str, str]],
        use_threads: Union[bool, int],
        version_id: Optional[str] = None,
        coerce_int96_timestamp_unit: Optional[str] = None,
    ) -> pa.schema:
        pass

    def _read_schemas_from_files(
        self,
        paths: List[str],
        sampling: float,
        use_threads: Union[bool, int],
        s3_client: "S3Client",
        s3_additional_kwargs: Optional[Dict[str, str]],
        version_ids: Optional[Dict[str, str]],
        coerce_int96_timestamp_unit: Optional[str] = None,
    ) -> List[pa.schema]:
        paths = _utils.list_sampling(lst=paths, sampling=sampling)

        executor: _BaseExecutor = _get_executor(use_threads=use_threads)
        schemas = ray_get(
            executor.map(
                self._read_metadata_file,
                s3_client,
                paths,
                itertools.repeat(s3_additional_kwargs),
                itertools.repeat(use_threads),
                [version_ids.get(p) if isinstance(version_ids, dict) else None for p in paths],
                itertools.repeat(coerce_int96_timestamp_unit),
            )
        )
        return [schema for schema in schemas if schema is not None]

    def _validate_schemas_from_files(
        self,
        validate_schema: bool,
        paths: List[str],
        sampling: float,
        use_threads: Union[bool, int],
        s3_client: "S3Client",
        s3_additional_kwargs: Optional[Dict[str, str]],
        version_ids: Optional[Dict[str, str]],
        coerce_int96_timestamp_unit: Optional[str] = None,
    ) -> pa.schema:
        schemas: List[pa.schema] = self._read_schemas_from_files(
            paths=paths,
            sampling=sampling,
            use_threads=use_threads,
            s3_client=s3_client,
            s3_additional_kwargs=s3_additional_kwargs,
            version_ids=version_ids,
            coerce_int96_timestamp_unit=coerce_int96_timestamp_unit,
        )
        return _validate_schemas(schemas, validate_schema)

    def validate_schemas(
        self,
        paths: List[str],
        path_root: Optional[str],
        columns: Optional[List[str]],
        validate_schema: bool,
        s3_client: "S3Client",
        version_ids: Optional[Dict[str, str]] = None,
        use_threads: Union[bool, int] = True,
        coerce_int96_timestamp_unit: Optional[str] = None,
        s3_additional_kwargs: Optional[Dict[str, Any]] = None,
    ) -> pa.schema:
        schema = self._validate_schemas_from_files(
            validate_schema=validate_schema,
            paths=paths,
            sampling=1.0,
            use_threads=use_threads,
            s3_client=s3_client,
            s3_additional_kwargs=s3_additional_kwargs,
            coerce_int96_timestamp_unit=coerce_int96_timestamp_unit,
            version_ids=version_ids,
        )
        if path_root:
            partition_types, _ = _extract_partitions_metadata_from_paths(path=path_root, paths=paths)
            if partition_types:
                partition_schema = pa.schema(
                    fields={k: _data_types.athena2pyarrow(dtype=v) for k, v in partition_types.items()}
                )
                schema = pa.unify_schemas([schema, partition_schema])
        if columns:
            schema = pa.schema([schema.field(column) for column in columns], schema.metadata)
        _logger.debug("Resolved pyarrow schema:\n%s", schema)

        return schema

    def read_table_metadata(
        self,
        path: Union[str, List[str]],
        path_suffix: Optional[str],
        path_ignore_suffix: Union[str, List[str], None],
        ignore_empty: bool,
        ignore_null: bool,
        dtype: Optional[Dict[str, str]],
        sampling: float,
        dataset: bool,
        use_threads: Union[bool, int],
        boto3_session: Optional[boto3.Session],
        s3_additional_kwargs: Optional[Dict[str, str]],
        version_id: Optional[Union[str, Dict[str, str]]] = None,
        coerce_int96_timestamp_unit: Optional[str] = None,
    ) -> _InternalReadTableMetadataReturnValue:
        """Handle table metadata internally."""
        s3_client = _utils.client(service_name="s3", session=boto3_session)
        path_root: Optional[str] = _get_path_root(path=path, dataset=dataset)
        paths: List[str] = _path2list(
            path=path,
            s3_client=s3_client,
            suffix=path_suffix,
            ignore_suffix=_get_path_ignore_suffix(path_ignore_suffix=path_ignore_suffix),
            ignore_empty=ignore_empty,
            s3_additional_kwargs=s3_additional_kwargs,
        )
        version_ids = _check_version_id(paths=paths, version_id=version_id)

        # Files
        schemas: List[pa.schema] = self._read_schemas_from_files(
            paths=paths,
            sampling=sampling,
            use_threads=use_threads,
            s3_client=s3_client,
            s3_additional_kwargs=s3_additional_kwargs,
            version_ids=version_ids,
            coerce_int96_timestamp_unit=coerce_int96_timestamp_unit,
        )
        merged_schemas = _validate_schemas(schemas=schemas, validate_schema=False)

        columns_types: Dict[str, str] = _data_types.athena_types_from_pyarrow_schema(
            schema=merged_schemas, partitions=None, ignore_null=ignore_null
        )[0]

        # Partitions
        partitions_types: Optional[Dict[str, str]] = None
        partitions_values: Optional[Dict[str, List[str]]] = None
        if (dataset is True) and (path_root is not None):
            partitions_types, partitions_values = _extract_partitions_metadata_from_paths(path=path_root, paths=paths)

        # Casting
        if dtype:
            for k, v in dtype.items():
                if columns_types and k in columns_types:
                    columns_types[k] = v
                if partitions_types and k in partitions_types:
                    partitions_types[k] = v

        return _InternalReadTableMetadataReturnValue(columns_types, partitions_types, partitions_values)


def _validate_schemas(schemas: List[pa.schema], validate_schema: bool) -> pa.schema:
    first: pa.schema = schemas[0]
    if len(schemas) == 1:
        return first
    first_dict = {s.name: s.type for s in first}
    if validate_schema:
        for schema in schemas[1:]:
            if first_dict != {s.name: s.type for s in schema}:
                raise exceptions.InvalidSchemaConvergence(
                    f"At least 2 different schemas were detected:\n    1 - {first}\n    2 - {schema}."
                )
    return pa.unify_schemas(schemas)


def _ensure_locations_are_valid(paths: Iterable[str]) -> Iterator[str]:
    for path in paths:
        suffix: str = path.rpartition("/")[2]
        # If the suffix looks like a partition,
        if suffix and (suffix.count("=") == 1):
            # the path should end in a '/' character.
            path = f"{path}/"  # ruff: noqa: PLW2901
        yield path


def _get_paths_for_glue_table(
    table: str,
    database: str,
    filename_suffix: Union[str, List[str], None] = None,
    filename_ignore_suffix: Union[str, List[str], None] = None,
    catalog_id: Optional[str] = None,
    partition_filter: Optional[Callable[[Dict[str, str]], bool]] = None,
    boto3_session: Optional[boto3.Session] = None,
    s3_additional_kwargs: Optional[Dict[str, Any]] = None,
) -> Tuple[Union[str, List[str]], Optional[str], "GetTableResponseTypeDef"]:
    client_glue = _utils.client(service_name="glue", session=boto3_session)
    s3_client = _utils.client(service_name="s3", session=boto3_session)

    res = client_glue.get_table(**_catalog_id(catalog_id=catalog_id, DatabaseName=database, Name=table))
    try:
        location: str = res["Table"]["StorageDescriptor"]["Location"]
        path: str = location if location.endswith("/") else f"{location}/"
    except KeyError as ex:
        raise exceptions.InvalidTable(f"Missing s3 location for {database}.{table}.") from ex

    path_root: Optional[str] = None
    paths: Union[str, List[str]] = path

    # If filter is available, fetch & filter out partitions
    # Then list objects & process individual object keys under path_root
    if partition_filter:
        available_partitions_dict = _get_partitions(
            database=database,
            table=table,
            catalog_id=catalog_id,
            boto3_session=boto3_session,
        )
        available_partitions = list(_ensure_locations_are_valid(available_partitions_dict.keys()))
        if available_partitions:
            paths = []
            path_root = path
            partitions: Union[str, List[str]] = _apply_partition_filter(
                path_root=path_root, paths=available_partitions, filter_func=partition_filter
            )
            for partition in partitions:
                paths += _path2list(
                    path=partition,
                    s3_client=s3_client,
                    suffix=filename_suffix,
                    ignore_suffix=_get_path_ignore_suffix(path_ignore_suffix=filename_ignore_suffix),
                    s3_additional_kwargs=s3_additional_kwargs,
                )

    return paths, path_root, res
