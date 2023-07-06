"""Amazon CSV S3 Write Module (PRIVATE)."""

import logging
import uuid
from abc import ABC, abstractmethod
from enum import Enum
from typing import TYPE_CHECKING, Any, Callable, Dict, List, NamedTuple, Optional, Union

import boto3
import pandas as pd
import pyarrow as pa

from awswrangler import _data_types, _utils, catalog, exceptions, lakeformation, typing
from awswrangler._distributed import EngineEnum
from awswrangler._utils import copy_df_shallow
from awswrangler.s3._delete import delete_objects
from awswrangler.s3._write_dataset import _to_dataset

if TYPE_CHECKING:
    from mypy_boto3_s3 import S3Client

_logger: logging.Logger = logging.getLogger(__name__)


_COMPRESSION_2_EXT: Dict[Optional[str], str] = {
    None: "",
    "gzip": ".gz",
    "snappy": ".snappy",
    "bz2": ".bz2",
    "xz": ".xz",
    "zip": ".zip",
    "zstd": ".zstd",
}


def _extract_dtypes_from_table_input(table_input: Dict[str, Any]) -> Dict[str, str]:
    dtypes: Dict[str, str] = {}
    for col in table_input["StorageDescriptor"]["Columns"]:
        dtypes[col["Name"]] = col["Type"]
    if "PartitionKeys" in table_input:
        for par in table_input["PartitionKeys"]:
            dtypes[par["Name"]] = par["Type"]
    return dtypes


def _apply_dtype(
    df: pd.DataFrame, dtype: Dict[str, str], catalog_table_input: Optional[Dict[str, Any]], mode: str
) -> pd.DataFrame:
    if mode in ("append", "overwrite_partitions"):
        if catalog_table_input is not None:
            catalog_types: Optional[Dict[str, str]] = _extract_dtypes_from_table_input(table_input=catalog_table_input)
            if catalog_types is not None:
                for k, v in catalog_types.items():
                    dtype[k] = v
    df = _data_types.cast_pandas_with_athena_types(df=df, dtype=dtype)
    return df


def _validate_args(
    df: pd.DataFrame,
    table: Optional[str],
    database: Optional[str],
    dataset: bool,
    path: Optional[str],
    partition_cols: Optional[List[str]],
    bucketing_info: Optional[typing.BucketingInfoTuple],
    mode: Optional[str],
    description: Optional[str],
    parameters: Optional[Dict[str, str]],
    columns_comments: Optional[Dict[str, str]],
    execution_engine: Enum,
) -> None:
    if df.empty is True:
        _logger.warning("Empty DataFrame will be written.")
    if dataset is False:
        if path is None:
            raise exceptions.InvalidArgumentValue("If dataset is False, the `path` argument must be passed.")
        if execution_engine == EngineEnum.PYTHON and path.endswith("/"):
            raise exceptions.InvalidArgumentValue(
                "If <dataset=False>, the argument <path> should be a key, not a prefix."
            )
        if partition_cols:
            raise exceptions.InvalidArgumentCombination("Please, pass dataset=True to be able to use partition_cols.")
        if bucketing_info:
            raise exceptions.InvalidArgumentCombination("Please, pass dataset=True to be able to use bucketing_info.")
        if mode is not None:
            raise exceptions.InvalidArgumentCombination("Please pass dataset=True to be able to use mode.")
        if any(arg is not None for arg in (table, description, parameters, columns_comments)):
            raise exceptions.InvalidArgumentCombination(
                "Please pass dataset=True to be able to use any one of these "
                "arguments: database, table, description, parameters, "
                "columns_comments."
            )
    elif (database is None) != (table is None):
        raise exceptions.InvalidArgumentCombination(
            "Arguments database and table must be passed together. If you want to store your dataset metadata in "
            "the Glue Catalog, please ensure you are passing both."
        )
    elif all(x is None for x in [path, database, table]):
        raise exceptions.InvalidArgumentCombination(
            "You must specify a `path` if dataset is True and database/table are not enabled."
        )
    elif bucketing_info and bucketing_info[1] <= 0:
        raise exceptions.InvalidArgumentValue(
            "Please pass a value greater than 1 for the number of buckets for bucketing."
        )


class _SanitizeResult(NamedTuple):
    frame: pd.DataFrame
    dtype: Dict[str, str]
    partition_cols: List[str]
    bucketing_info: Optional[typing.BucketingInfoTuple]


def _sanitize(
    df: pd.DataFrame,
    dtype: Dict[str, str],
    partition_cols: List[str],
    bucketing_info: Optional[typing.BucketingInfoTuple] = None,
) -> _SanitizeResult:
    df = catalog.sanitize_dataframe_columns_names(df=df)
    partition_cols = [catalog.sanitize_column_name(p) for p in partition_cols]
    if bucketing_info:
        bucketing_info = [
            catalog.sanitize_column_name(bucketing_col) for bucketing_col in bucketing_info[0]
        ], bucketing_info[1]
    dtype = {catalog.sanitize_column_name(k): v for k, v in dtype.items()}
    _utils.check_duplicated_columns(df=df)
    return _SanitizeResult(df, dtype, partition_cols, bucketing_info)


def _get_chunk_file_path(file_counter: int, file_path: str) -> str:
    slash_index: int = file_path.rfind("/")
    dot_index: int = file_path.find(".", slash_index)
    file_index: str = "_" + str(file_counter)
    if dot_index == -1:
        file_path = file_path + file_index
    else:
        file_path = file_path[:dot_index] + file_index + file_path[dot_index:]
    return file_path


def _get_write_table_args(pyarrow_additional_kwargs: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    write_table_args: Dict[str, Any] = {}
    if pyarrow_additional_kwargs and "write_table_args" in pyarrow_additional_kwargs:
        write_table_args = pyarrow_additional_kwargs.pop("write_table_args")
    return write_table_args


def _get_file_path(
    path_root: Optional[str] = None,
    path: Optional[str] = None,
    filename_prefix: Optional[str] = None,
    compression_ext: str = "",
    bucket_id: Optional[int] = None,
    extension: str = ".parquet",
) -> str:
    if bucket_id is not None:
        filename_prefix = f"{filename_prefix}_bucket-{bucket_id:05d}"
    if path is None and path_root is not None:
        file_path: str = f"{path_root}{filename_prefix}{compression_ext}{extension}"
    elif path is not None and path_root is None:
        file_path = path
    else:
        raise RuntimeError("path and path_root received at the same time.")
    return file_path


class _S3WriteStrategy(ABC):
    @property
    @abstractmethod
    def _write_to_s3_func(self) -> Callable[..., List[str]]:
        pass

    @abstractmethod
    def _write_to_s3(
        self,
        df: pd.DataFrame,
        schema: pa.Schema,
        index: bool,
        compression: Optional[str],
        compression_ext: str,
        pyarrow_additional_kwargs: Dict[str, Any],
        cpus: int,
        dtype: Dict[str, str],
        s3_client: Optional["S3Client"],
        s3_additional_kwargs: Optional[Dict[str, str]],
        use_threads: Union[bool, int],
        path: Optional[str] = None,
        path_root: Optional[str] = None,
        filename_prefix: Optional[str] = None,
        max_rows_by_file: Optional[int] = 0,
        bucketing: bool = False,
    ) -> List[str]:
        pass

    @abstractmethod
    def _create_glue_table(
        self,
        database: str,
        table: str,
        path: str,
        columns_types: Dict[str, str],
        table_type: Optional[str],
        partitions_types: Optional[Dict[str, str]],
        bucketing_info: Optional[typing.BucketingInfoTuple],
        catalog_id: Optional[str],
        compression: Optional[str],
        description: Optional[str],
        parameters: Optional[Dict[str, str]],
        columns_comments: Optional[Dict[str, str]],
        mode: str,
        catalog_versioning: bool,
        transaction_id: Optional[str],
        athena_partition_projection_settings: Optional[typing.AthenaPartitionProjectionSettings],
        boto3_session: Optional[boto3.Session],
        catalog_table_input: Optional[Dict[str, Any]],
    ) -> None:
        pass

    @abstractmethod
    def _add_glue_partitions(
        self,
        database: str,
        table: str,
        partitions_values: Dict[str, List[str]],
        bucketing_info: Optional[typing.BucketingInfoTuple] = None,
        catalog_id: Optional[str] = None,
        compression: Optional[str] = None,
        boto3_session: Optional[boto3.Session] = None,
        columns_types: Optional[Dict[str, str]] = None,
        partitions_parameters: Optional[Dict[str, str]] = None,
    ) -> None:
        pass

    def write(  # pylint: disable=too-many-arguments,too-many-locals,too-many-branches,too-many-statements
        self,
        df: pd.DataFrame,
        path: Optional[str],
        index: bool,
        compression: Optional[str],
        pyarrow_additional_kwargs: Dict[str, Any],
        max_rows_by_file: Optional[int],
        use_threads: Union[bool, int],
        boto3_session: Optional[boto3.Session],
        s3_additional_kwargs: Optional[Dict[str, Any]],
        sanitize_columns: bool,
        dataset: bool,
        filename_prefix: Optional[str],
        partition_cols: Optional[List[str]],
        bucketing_info: Optional[typing.BucketingInfoTuple],
        concurrent_partitioning: bool,
        mode: Optional[str],
        catalog_versioning: bool,
        schema_evolution: bool,
        database: Optional[str],
        table: Optional[str],
        description: Optional[str],
        parameters: Optional[Dict[str, str]],
        columns_comments: Optional[Dict[str, str]],
        transaction_id: Optional[str],
        regular_partitions: bool,
        table_type: Optional[str],
        dtype: Optional[Dict[str, str]],
        athena_partition_projection_settings: Optional[typing.AthenaPartitionProjectionSettings],
        catalog_id: Optional[str],
        compression_ext: str,
    ) -> typing._S3WriteDataReturnValue:
        # Initializing defaults
        partition_cols = partition_cols if partition_cols else []
        dtype = dtype if dtype else {}
        partitions_values: Dict[str, List[str]] = {}
        mode = "append" if mode is None else mode
        commit_trans: bool = False
        if transaction_id:
            table_type = "GOVERNED"

        filename_prefix = filename_prefix + uuid.uuid4().hex if filename_prefix else uuid.uuid4().hex
        cpus: int = _utils.ensure_cpu_count(use_threads=use_threads)
        s3_client = _utils.client(service_name="s3", session=boto3_session)

        # Sanitize table to respect Athena's standards
        if (sanitize_columns is True) or (database is not None and table is not None):
            df, dtype, partition_cols, bucketing_info = _sanitize(
                df=copy_df_shallow(df),
                dtype=dtype,
                partition_cols=partition_cols,
                bucketing_info=bucketing_info,
            )

        # Evaluating dtype
        catalog_table_input: Optional[Dict[str, Any]] = None
        if database is not None and table is not None:
            catalog_table_input = catalog._get_table_input(  # pylint: disable=protected-access
                database=database,
                table=table,
                boto3_session=boto3_session,
                transaction_id=transaction_id,
                catalog_id=catalog_id,
            )
            catalog_path: Optional[str] = None
            if catalog_table_input:
                table_type = catalog_table_input["TableType"]
                catalog_path = catalog_table_input["StorageDescriptor"]["Location"]
            if path is None:
                if catalog_path:
                    path = catalog_path
                else:
                    raise exceptions.InvalidArgumentValue(
                        "Glue table does not exist in the catalog. Please pass the `path` argument to create it."
                    )
            elif path and catalog_path:
                if path.rstrip("/") != catalog_path.rstrip("/"):
                    raise exceptions.InvalidArgumentValue(
                        f"The specified path: {path}, does not match the existing Glue catalog table path: {catalog_path}"
                    )

            if (table_type == "GOVERNED") and (not transaction_id):
                _logger.debug("`transaction_id` not specified for GOVERNED table, starting transaction")
                transaction_id = lakeformation.start_transaction(
                    read_only=False,
                    boto3_session=boto3_session,
                )
                commit_trans = True

        df = _apply_dtype(df=df, dtype=dtype, catalog_table_input=catalog_table_input, mode=mode)
        schema: pa.Schema = _data_types.pyarrow_schema_from_pandas(
            df=df, index=index, ignore_cols=partition_cols, dtype=dtype
        )
        _logger.debug("Resolved pyarrow schema: \n%s", schema)

        if dataset is False:
            paths = self._write_to_s3(
                df,
                path=path,
                filename_prefix=filename_prefix,
                schema=schema,
                index=index,
                cpus=cpus,
                compression=compression,
                compression_ext=compression_ext,
                pyarrow_additional_kwargs=pyarrow_additional_kwargs,
                s3_client=s3_client,
                s3_additional_kwargs=s3_additional_kwargs,
                dtype=dtype,
                max_rows_by_file=max_rows_by_file,
                use_threads=use_threads,
            )
        else:
            columns_types: Dict[str, str] = {}
            partitions_types: Dict[str, str] = {}
            if (database is not None) and (table is not None):
                columns_types, partitions_types = _data_types.athena_types_from_pandas_partitioned(
                    df=df, index=index, partition_cols=partition_cols, dtype=dtype
                )
                if schema_evolution is False:
                    _utils.check_schema_changes(columns_types=columns_types, table_input=catalog_table_input, mode=mode)

                create_table_args: Dict[str, Any] = {
                    "database": database,
                    "table": table,
                    "path": path,
                    "columns_types": columns_types,
                    "table_type": table_type,
                    "partitions_types": partitions_types,
                    "bucketing_info": bucketing_info,
                    "compression": compression,
                    "description": description,
                    "parameters": parameters,
                    "columns_comments": columns_comments,
                    "boto3_session": boto3_session,
                    "mode": mode,
                    "transaction_id": transaction_id,
                    "catalog_versioning": catalog_versioning,
                    "athena_partition_projection_settings": athena_partition_projection_settings,
                    "catalog_id": catalog_id,
                    "catalog_table_input": catalog_table_input,
                }

                if (catalog_table_input is None) and (table_type == "GOVERNED"):
                    self._create_glue_table(**create_table_args)  # pylint: disable=protected-access
                    create_table_args[
                        "catalog_table_input"
                    ] = catalog._get_table_input(  # pylint: disable=protected-access
                        database=database,
                        table=table,
                        boto3_session=boto3_session,
                        transaction_id=transaction_id,
                        catalog_id=catalog_id,
                    )

            paths, partitions_values = _to_dataset(
                func=self._write_to_s3_func,
                concurrent_partitioning=concurrent_partitioning,
                df=df,
                path_root=path,  # type: ignore[arg-type]
                filename_prefix=filename_prefix,
                index=index,
                compression=compression,
                compression_ext=compression_ext,
                catalog_id=catalog_id,
                database=database,
                table=table,
                table_type=table_type,
                transaction_id=transaction_id,
                pyarrow_additional_kwargs=pyarrow_additional_kwargs,
                cpus=cpus,
                use_threads=use_threads,
                partition_cols=partition_cols,
                partitions_types=partitions_types,
                bucketing_info=bucketing_info,
                dtype=dtype,
                mode=mode,
                boto3_session=boto3_session,
                s3_additional_kwargs=s3_additional_kwargs,
                schema=schema,
                max_rows_by_file=max_rows_by_file,
            )
            if database and table:
                try:
                    self._create_glue_table(**create_table_args)  # pylint: disable=protected-access
                    if partitions_values and (regular_partitions is True) and (table_type != "GOVERNED"):
                        self._add_glue_partitions(
                            database=database,
                            table=table,
                            partitions_values=partitions_values,
                            bucketing_info=bucketing_info,
                            compression=compression,
                            boto3_session=boto3_session,
                            catalog_id=catalog_id,
                            columns_types=columns_types,
                        )
                    if commit_trans:
                        lakeformation.commit_transaction(
                            transaction_id=transaction_id,  # type: ignore[arg-type]
                            boto3_session=boto3_session,
                        )
                except Exception:
                    _logger.debug("Catalog write failed, cleaning up S3 objects (len(paths): %s).", len(paths))
                    delete_objects(
                        path=paths,
                        use_threads=use_threads,
                        boto3_session=boto3_session,
                        s3_additional_kwargs=s3_additional_kwargs,
                    )
                    raise

        return {"paths": paths, "partitions_values": partitions_values}
