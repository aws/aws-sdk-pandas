"""Amazon S3 Writer Delta Lake Module (PRIVATE)."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Literal

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
    overwrite_schema: bool = False,
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
    df: pandas.DataFrame
        `Pandas DataFrame <https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html>`_
    path: str
        S3 path for a directory where the DeltaLake table will be stored.
    index: bool
        True to store the DataFrame index in file, otherwise False to ignore it.
    mode: str, optional
        ``append`` (Default), ``overwrite``, ``ignore``, ``error``
    dtype: dict[str, str], optional
        Dictionary of columns names and Athena/Glue types to be casted.
        Useful when you have columns with undetermined or mixed data types.
        (e.g. ``{'col name':'bigint', 'col2 name': 'int'})``
    partition_cols: list[str], optional
        List of columns to partition the table by. Only required when creating a new table.
    overwrite_schema: bool
        If True, allows updating the schema of the table.
    lock_dynamodb_table: str | None
        DynamoDB table to use as a locking provider.
        A locking mechanism is needed to prevent unsafe concurrent writes to a delta lake directory when writing to S3.
        If you don't want to use a locking mechanism, you can choose to set ``s3_allow_unsafe_rename`` to True.

        For information on how to set up the lock table,
        please check `this page <https://delta-io.github.io/delta-rs/usage/writing/writing-to-s3-with-locking-provider/#dynamodb>`_.
    s3_allow_unsafe_rename: bool
        Allows using the default S3 backend without support for concurrent writers.
    boto3_session: boto3.Session, optional
        Boto3 Session. If None, the default boto3 session is used.
    pyarrow_additional_kwargs: dict[str, Any], optional
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
        overwrite_schema=overwrite_schema,
        storage_options=storage_options,
    )
