"""Amazon S3 Writer Delta Lake Module (PRIVATE)."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Iterable, Iterator, Literal

import boto3
import pandas as pd
import pyarrow as pa

from awswrangler import _data_types, _utils
from awswrangler._arrow import _df_to_table
from awswrangler.annotations import Experimental

if TYPE_CHECKING:
    try:
        import deltalake
    except ImportError:
        pass
else:
    deltalake = _utils.import_optional_dependency("deltalake")


def _set_default_storage_options_kwargs(
    boto3_session: boto3.Session | None,
    s3_additional_kwargs: dict[str, Any] | None,
    s3_allow_unsafe_rename: bool,
    lock_dynamodb_table: str | None = None,
) -> dict[str, Any]:
    defaults = {key.upper(): value for key, value in _utils.boto3_to_primitives(boto3_session=boto3_session).items()}
    defaults["AWS_REGION"] = defaults.pop("REGION_NAME")
    defaults["AWS_SESSION_TOKEN"] = "" if defaults["AWS_SESSION_TOKEN"] is None else defaults["AWS_SESSION_TOKEN"]

    s3_additional_kwargs = s3_additional_kwargs or {}

    s3_lock_arguments = {}
    if lock_dynamodb_table:
        s3_lock_arguments["AWS_S3_LOCKING_PROVIDER"] = "dynamodb"
        s3_lock_arguments["DELTA_DYNAMO_TABLE_NAME"] = lock_dynamodb_table

    return {
        **defaults,
        **s3_additional_kwargs,
        **s3_lock_arguments,
        "AWS_S3_ALLOW_UNSAFE_RENAME": "TRUE" if s3_allow_unsafe_rename else "FALSE",
    }


@_utils.check_optional_dependency(deltalake, "deltalake")
@Experimental
def to_deltalake(
    df: pd.DataFrame,
    path: str,
    index: bool = False,
    mode: Literal["error", "append", "overwrite", "ignore"] = "append",
    dtype: dict[str, str] | None = None,
    partition_cols: list[str] | None = None,
    schema_mode: Literal["overwrite"] | None = None,
    lock_dynamodb_table: str | None = None,
    s3_allow_unsafe_rename: bool = False,
    boto3_session: boto3.Session | None = None,
    s3_additional_kwargs: dict[str, str] | None = None,
) -> None:
    """Write a DataFrame to S3 as a DeltaLake table.

    This function requires the `deltalake package
    <https://delta-io.github.io/delta-rs/python>`__.

    Parameters
    ----------
    df
        `Pandas DataFrame <https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html>`_
    path
        S3 path for a directory where the DeltaLake table will be stored.
    index
        True to store the DataFrame index in file, otherwise False to ignore it.
    mode
        ``append`` (Default), ``overwrite``, ``ignore``, ``error``
    dtype
        Dictionary of columns names and Athena/Glue types to be casted.
        Useful when you have columns with undetermined or mixed data types.
        (e.g. ``{'col name':'bigint', 'col2 name': 'int'})``
    partition_cols
        List of columns to partition the table by. Only required when creating a new table.
    schema_mode
        If set to "overwrite", allows replacing the schema of the table. Set to "merge" to merge with existing schema.
    lock_dynamodb_table
        DynamoDB table to use as a locking provider.
        A locking mechanism is needed to prevent unsafe concurrent writes to a delta lake directory when writing to S3.
        If you don't want to use a locking mechanism, you can choose to set ``s3_allow_unsafe_rename`` to True.

        For information on how to set up the lock table,
        please check `this page <https://delta-io.github.io/delta-rs/usage/writing/writing-to-s3-with-locking-provider/#dynamodb>`_.
    s3_allow_unsafe_rename
        Allows using the default S3 backend without support for concurrent writers.
    boto3_session
        If None, the default boto3 session is used.
    pyarrow_additional_kwargs
        Forwarded to the Delta Table class for the storage options of the S3 backend.

    Examples
    --------
    Writing a Pandas DataFrame into a DeltaLake table in S3.

    >>> import awswrangler as wr
    >>> import pandas as pd
    >>> wr.s3.to_deltalake(
    ...     df=pd.DataFrame({"col": [1, 2, 3]}),
    ...     path="s3://bucket/prefix/",
    ...     lock_dynamodb_table="my-lock-table",
    ... )

    See Also
    --------
    deltalake.DeltaTable: Create a DeltaTable instance with the deltalake library.
    deltalake.write_deltalake: Write to a DeltaLake table.
    """
    dtype = dtype if dtype else {}

    schema: pa.Schema = _data_types.pyarrow_schema_from_pandas(df=df, index=index, ignore_cols=None, dtype=dtype)
    table: pa.Table = _df_to_table(df, schema, index, dtype)

    storage_options = _set_default_storage_options_kwargs(
        boto3_session=boto3_session,
        s3_additional_kwargs=s3_additional_kwargs,
        s3_allow_unsafe_rename=s3_allow_unsafe_rename,
        lock_dynamodb_table=lock_dynamodb_table,
    )
    deltalake.write_deltalake(
        table_or_uri=path,
        data=table,
        partition_by=partition_cols,
        mode=mode,
        schema_mode=schema_mode,
        storage_options=storage_options,
    )



def _df_iter_to_record_batch_reader(
    df_iter: Iterable[pd.DataFrame],
    *,
    index: bool,
    dtype: dict[str, str],
    target_schema: pa.Schema | None = None,
    batch_size: int | None = None,
) -> tuple[pa.RecordBatchReader, pa.Schema]:
    """
    Convert an iterable of Pandas DataFrames into a single Arrow RecordBatchReader
    suitable for a single delta-rs commit. The first *non-empty* DataFrame fixes the schema.

    Returns
    -------
    (reader, schema)
      reader: pa.RecordBatchReader streaming all chunks as Arrow batches
      schema: pa.Schema used for conversion
    """
    it = iter(df_iter)

    first_df: pd.DataFrame | None = None
    for df in it:
        if not df.empty:
            first_df = df
            break

    if first_df is None:
        empty_schema = pa.schema([])
        empty_reader = pa.RecordBatchReader.from_batches(empty_schema, [])
        return empty_reader, empty_schema

    schema = target_schema or _data_types.pyarrow_schema_from_pandas(
        df=first_df, index=index, ignore_cols=None, dtype=dtype
    )

    def batches() -> Iterator[pa.RecordBatch]:
        first_tbl: pa.Table = _df_to_table(first_df, schema, index, dtype)
        for b in (first_tbl.to_batches(batch_size) if batch_size is not None else first_tbl.to_batches()):
            yield b

        for df in it:
            if df.empty:
                continue
            tbl: pa.Table = _df_to_table(df, schema, index, dtype)
            for b in (tbl.to_batches(batch_size) if batch_size is not None else tbl.to_batches()):
                yield b

    reader = pa.RecordBatchReader.from_batches(schema, batches())
    return reader, schema


@_utils.check_optional_dependency(deltalake, "deltalake")
@Experimental
def to_deltalake_streaming(
    *,
    dfs: Iterable[pd.DataFrame],
    path: str,
    index: bool = False,
    mode: Literal["error", "append", "overwrite", "ignore"] = "append",
    dtype: dict[str, str] | None = None,
    partition_cols: list[str] | None = None,
    schema_mode: Literal["overwrite", "merge"] | None = None,
    lock_dynamodb_table: str | None = None,
    s3_allow_unsafe_rename: bool = False,
    boto3_session: boto3.Session | None = None,
    s3_additional_kwargs: dict[str, str] | None = None,
    batch_size: int | None = None,
    max_open_files: int | None = None,
    max_rows_per_file: int | None = None,
    target_file_size: int | None = None,
) -> None:
    """
    Write an iterable/generator of Pandas DataFrames to S3 as a Delta Lake table
    in a SINGLE atomic commit (one table version).

    Use this for large "restatements" that are produced in chunks. Semantics mirror
    `to_deltalake` (partitioning, schema handling, S3 locking, etc.).

    Notes
    -----
    - The schema is fixed by the first *non-empty* chunk (plus any `dtype` coercions).
    - All `partition_cols` must be present in every non-empty chunk.
    - Prefer `lock_dynamodb_table` over `s3_allow_unsafe_rename=True` on S3.
    """
    dtype = dtype or {}

    storage_options = _set_default_storage_options_kwargs(
        boto3_session=boto3_session,
        s3_additional_kwargs=s3_additional_kwargs,
        s3_allow_unsafe_rename=s3_allow_unsafe_rename,
        lock_dynamodb_table=lock_dynamodb_table,
    )

    reader, schema = _df_iter_to_record_batch_reader(
        df_iter=dfs,
        index=index,
        dtype=dtype,
        target_schema=None,
        batch_size=batch_size,
    )

    if len(schema) == 0:
        return

    deltalake.write_deltalake(
        table_or_uri=path,
        data=reader,
        partition_by=partition_cols,
        mode=mode,
        schema_mode=schema_mode,
        storage_options=storage_options,
        max_open_files=max_open_files,
        max_rows_per_file=max_rows_per_file,
        target_file_size=target_file_size,
    )