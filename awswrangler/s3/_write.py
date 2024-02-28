"""Amazon CSV S3 Write Module (PRIVATE)."""

from __future__ import annotations

import logging
import uuid
from abc import ABC, abstractmethod
from enum import Enum
from typing import TYPE_CHECKING, Any, Callable, NamedTuple

import boto3
import pandas as pd
import pyarrow as pa

from awswrangler import _data_types, _utils, catalog, exceptions, typing
from awswrangler._distributed import EngineEnum
from awswrangler._utils import copy_df_shallow
from awswrangler.s3._delete import delete_objects
from awswrangler.s3._write_dataset import _to_dataset

if TYPE_CHECKING:
    from mypy_boto3_s3 import S3Client

_logger: logging.Logger = logging.getLogger(__name__)


_COMPRESSION_2_EXT: dict[str | None, str] = {
    None: "",
    "gzip": ".gz",
    "snappy": ".snappy",
    "bz2": ".bz2",
    "xz": ".xz",
    "zip": ".zip",
    "zstd": ".zstd",
}


def _extract_dtypes_from_table_input(table_input: dict[str, Any]) -> dict[str, str]:
    dtypes: dict[str, str] = {}
    for col in table_input["StorageDescriptor"]["Columns"]:
        dtypes[col["Name"]] = col["Type"]
    if "PartitionKeys" in table_input:
        for par in table_input["PartitionKeys"]:
            dtypes[par["Name"]] = par["Type"]
    return dtypes


def _apply_dtype(
    df: pd.DataFrame, dtype: dict[str, str], catalog_table_input: dict[str, Any] | None, mode: str
) -> pd.DataFrame:
    if mode in ("append", "overwrite_partitions"):
        if catalog_table_input is not None:
            catalog_types: dict[str, str] | None = _extract_dtypes_from_table_input(table_input=catalog_table_input)
            if catalog_types is not None:
                for k, v in catalog_types.items():
                    dtype[k] = v
    df = _data_types.cast_pandas_with_athena_types(df=df, dtype=dtype)
    return df


def _validate_args(
    df: pd.DataFrame,
    table: str | None,
    database: str | None,
    dataset: bool,
    path: str | None,
    partition_cols: list[str] | None,
    bucketing_info: typing.BucketingInfoTuple | None,
    mode: str | None,
    description: str | None,
    parameters: dict[str, str] | None,
    columns_comments: dict[str, str] | None,
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
    dtype: dict[str, str]
    partition_cols: list[str]
    bucketing_info: typing.BucketingInfoTuple | None


def _sanitize(
    df: pd.DataFrame,
    dtype: dict[str, str],
    partition_cols: list[str],
    bucketing_info: typing.BucketingInfoTuple | None = None,
) -> _SanitizeResult:
    df = catalog.sanitize_dataframe_columns_names(df=df)
    partition_cols = [catalog.sanitize_column_name(p) for p in partition_cols]
    if bucketing_info:
        bucketing_info = (
            [catalog.sanitize_column_name(bucketing_col) for bucketing_col in bucketing_info[0]],
            bucketing_info[1],
        )
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


def _get_write_table_args(pyarrow_additional_kwargs: dict[str, Any] | None = None) -> dict[str, Any]:
    write_table_args: dict[str, Any] = {}
    if pyarrow_additional_kwargs and "write_table_args" in pyarrow_additional_kwargs:
        write_table_args = pyarrow_additional_kwargs.pop("write_table_args")
    return write_table_args


def _get_file_path(
    path_root: str | None = None,
    path: str | None = None,
    filename_prefix: str | None = None,
    compression_ext: str = "",
    bucket_id: int | None = None,
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
    def _write_to_s3_func(self) -> Callable[..., list[str]]:
        pass

    @abstractmethod
    def _write_to_s3(
        self,
        df: pd.DataFrame,
        schema: pa.Schema,
        index: bool,
        compression: str | None,
        compression_ext: str,
        pyarrow_additional_kwargs: dict[str, Any],
        cpus: int,
        dtype: dict[str, str],
        s3_client: "S3Client" | None,
        s3_additional_kwargs: dict[str, str] | None,
        use_threads: bool | int,
        path: str | None = None,
        path_root: str | None = None,
        filename_prefix: str | None = None,
        max_rows_by_file: int | None = 0,
        bucketing: bool = False,
        encryption_configuration: typing.ArrowEncryptionConfiguration | None = None,
    ) -> list[str]:
        pass

    @abstractmethod
    def _create_glue_table(
        self,
        database: str,
        table: str,
        path: str,
        columns_types: dict[str, str],
        table_type: str | None,
        partitions_types: dict[str, str] | None,
        bucketing_info: typing.BucketingInfoTuple | None,
        catalog_id: str | None,
        compression: str | None,
        description: str | None,
        parameters: dict[str, str] | None,
        columns_comments: dict[str, str] | None,
        mode: str,
        catalog_versioning: bool,
        athena_partition_projection_settings: typing.AthenaPartitionProjectionSettings | None,
        boto3_session: boto3.Session | None,
        catalog_table_input: dict[str, Any] | None,
    ) -> None:
        pass

    @abstractmethod
    def _add_glue_partitions(
        self,
        database: str,
        table: str,
        partitions_values: dict[str, list[str]],
        bucketing_info: typing.BucketingInfoTuple | None = None,
        catalog_id: str | None = None,
        compression: str | None = None,
        boto3_session: boto3.Session | None = None,
        columns_types: dict[str, str] | None = None,
        partitions_parameters: dict[str, str] | None = None,
    ) -> None:
        pass

    def write(  # noqa: PLR0912,PLR0913
        self,
        df: pd.DataFrame,
        path: str | None,
        index: bool,
        compression: str | None,
        pyarrow_additional_kwargs: dict[str, Any],
        max_rows_by_file: int | None,
        use_threads: bool | int,
        boto3_session: boto3.Session | None,
        s3_additional_kwargs: dict[str, Any] | None,
        sanitize_columns: bool,
        dataset: bool,
        filename_prefix: str | None,
        partition_cols: list[str] | None,
        bucketing_info: typing.BucketingInfoTuple | None,
        concurrent_partitioning: bool,
        mode: str | None,
        catalog_versioning: bool,
        schema_evolution: bool,
        database: str | None,
        table: str | None,
        description: str | None,
        parameters: dict[str, str] | None,
        columns_comments: dict[str, str] | None,
        regular_partitions: bool,
        table_type: str | None,
        dtype: dict[str, str] | None,
        athena_partition_projection_settings: typing.AthenaPartitionProjectionSettings | None,
        catalog_id: str | None,
        compression_ext: str,
        encryption_configuration: typing.ArrowEncryptionConfiguration | None,
    ) -> typing._S3WriteDataReturnValue:
        # Initializing defaults
        partition_cols = partition_cols if partition_cols else []
        dtype = dtype if dtype else {}
        partitions_values: dict[str, list[str]] = {}
        mode = "append" if mode is None else mode

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
        catalog_table_input: dict[str, Any] | None = None
        if database is not None and table is not None:
            catalog_table_input = catalog._get_table_input(
                database=database,
                table=table,
                boto3_session=boto3_session,
                catalog_id=catalog_id,
            )
            catalog_path: str | None = None
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
                encryption_configuration=encryption_configuration,
            )
        else:
            columns_types: dict[str, str] = {}
            partitions_types: dict[str, str] = {}
            if (database is not None) and (table is not None):
                columns_types, partitions_types = _data_types.athena_types_from_pandas_partitioned(
                    df=df, index=index, partition_cols=partition_cols, dtype=dtype
                )
                if schema_evolution is False:
                    _utils.check_schema_changes(columns_types=columns_types, table_input=catalog_table_input, mode=mode)

                create_table_args: dict[str, Any] = {
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
                    "catalog_versioning": catalog_versioning,
                    "athena_partition_projection_settings": athena_partition_projection_settings,
                    "catalog_id": catalog_id,
                    "catalog_table_input": catalog_table_input,
                }

            paths, partitions_values = _to_dataset(
                func=self._write_to_s3_func,
                concurrent_partitioning=concurrent_partitioning,
                df=df,
                path_root=path,  # type: ignore[arg-type]
                filename_prefix=filename_prefix,
                index=index,
                compression=compression,
                compression_ext=compression_ext,
                pyarrow_additional_kwargs=pyarrow_additional_kwargs,
                cpus=cpus,
                use_threads=use_threads,
                partition_cols=partition_cols,
                bucketing_info=bucketing_info,
                dtype=dtype,
                mode=mode,
                boto3_session=boto3_session,
                s3_additional_kwargs=s3_additional_kwargs,
                schema=schema,
                max_rows_by_file=max_rows_by_file,
                encryption_configuration=encryption_configuration,
            )
            if database and table:
                try:
                    self._create_glue_table(**create_table_args)
                    if partitions_values and (regular_partitions is True):
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
