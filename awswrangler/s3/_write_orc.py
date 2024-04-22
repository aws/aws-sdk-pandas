"""Amazon S3 ORC Write Module (PRIVATE)."""

from __future__ import annotations

import logging
import math
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any, Callable, Iterator, Literal, cast

import boto3
import pandas as pd
import pyarrow as pa

from awswrangler import _utils, catalog, exceptions, typing
from awswrangler._arrow import _df_to_table
from awswrangler._config import apply_configs
from awswrangler._distributed import engine
from awswrangler.catalog._create import _create_orc_table
from awswrangler.s3._fs import open_s3_object
from awswrangler.s3._write import (
    _get_chunk_file_path,
    _get_file_path,
    _get_write_table_args,
    _S3WriteStrategy,
    _validate_args,
)
from awswrangler.s3._write_concurrent import _WriteProxy
from awswrangler.typing import (
    AthenaPartitionProjectionSettings,
    BucketingInfoTuple,
    GlueTableSettings,
    _S3WriteDataReturnValue,
)

if TYPE_CHECKING:
    from mypy_boto3_s3 import S3Client
    from pyarrow.orc import ORCWriter

_logger: logging.Logger = logging.getLogger(__name__)


_COMPRESSION_2_EXT: dict[str | None, str] = {
    None: "",
    "snappy": ".snappy",
    "zlib": ".zlib",
    "lz4": ".lz4",
    "zstd": ".zstd",
}


@contextmanager
def _new_writer(
    file_path: str,
    compression: str | None,
    pyarrow_additional_kwargs: dict[str, Any] | None,
    s3_client: "S3Client",
    s3_additional_kwargs: dict[str, str] | None,
    use_threads: bool | int,
) -> Iterator["ORCWriter"]:
    from pyarrow.orc import ORCWriter

    writer: "ORCWriter" | None = None
    if not pyarrow_additional_kwargs:
        pyarrow_additional_kwargs = {}

    with open_s3_object(
        path=file_path,
        mode="wb",
        use_threads=use_threads,
        s3_additional_kwargs=s3_additional_kwargs,
        s3_client=s3_client,
    ) as f:
        try:
            writer = ORCWriter(
                where=f,
                compression="uncompressed" if compression is None else compression,
                **pyarrow_additional_kwargs,
            )
            yield writer
        finally:
            if writer is not None and writer.is_open is True:
                writer.close()


def _write_chunk(
    file_path: str,
    s3_client: "S3Client",
    s3_additional_kwargs: dict[str, str] | None,
    compression: str | None,
    pyarrow_additional_kwargs: dict[str, str],
    table: pa.Table,
    offset: int,
    chunk_size: int,
    use_threads: bool | int,
) -> list[str]:
    write_table_args = _get_write_table_args(pyarrow_additional_kwargs)
    with _new_writer(
        file_path=file_path,
        compression=compression,
        pyarrow_additional_kwargs=pyarrow_additional_kwargs,
        s3_client=s3_client,
        s3_additional_kwargs=s3_additional_kwargs,
        use_threads=use_threads,
    ) as writer:
        writer.write(table.slice(offset, chunk_size), **write_table_args)
    return [file_path]


def _to_orc_chunked(
    file_path: str,
    s3_client: "S3Client",
    s3_additional_kwargs: dict[str, str] | None,
    compression: str | None,
    pyarrow_additional_kwargs: dict[str, Any],
    table: pa.Table,
    max_rows_by_file: int,
    num_of_rows: int,
    cpus: int,
) -> list[str]:
    chunks: int = math.ceil(num_of_rows / max_rows_by_file)
    use_threads: bool | int = cpus > 1
    proxy: _WriteProxy = _WriteProxy(use_threads=use_threads)
    for chunk in range(chunks):
        offset: int = chunk * max_rows_by_file
        write_path: str = _get_chunk_file_path(chunk, file_path)
        proxy.write(
            func=_write_chunk,
            file_path=write_path,
            s3_client=s3_client,
            s3_additional_kwargs=s3_additional_kwargs,
            compression=compression,
            pyarrow_additional_kwargs=pyarrow_additional_kwargs,
            table=table,
            offset=offset,
            chunk_size=max_rows_by_file,
            use_threads=use_threads,
        )
    return proxy.close()  # blocking


@engine.dispatch_on_engine
def _to_orc(
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
    s3_client = s3_client if s3_client else _utils.client(service_name="s3")
    file_path = _get_file_path(
        path_root=path_root,
        path=path,
        filename_prefix=filename_prefix,
        compression_ext=compression_ext,
        extension=".orc",
    )
    table: pa.Table = _df_to_table(df, schema, index, dtype)
    if max_rows_by_file is not None and max_rows_by_file > 0:
        paths: list[str] = _to_orc_chunked(
            file_path=file_path,
            s3_client=s3_client,
            s3_additional_kwargs=s3_additional_kwargs,
            compression=compression,
            pyarrow_additional_kwargs=pyarrow_additional_kwargs,
            table=table,
            max_rows_by_file=max_rows_by_file,
            num_of_rows=df.shape[0],
            cpus=cpus,
        )
    else:
        write_table_args = _get_write_table_args(pyarrow_additional_kwargs)
        with _new_writer(
            file_path=file_path,
            compression=compression,
            pyarrow_additional_kwargs=pyarrow_additional_kwargs,
            s3_client=s3_client,
            s3_additional_kwargs=s3_additional_kwargs,
            use_threads=use_threads,
        ) as writer:
            writer.write(table, **write_table_args)
        paths = [file_path]
    return paths


class _S3ORCWriteStrategy(_S3WriteStrategy):
    @property
    def _write_to_s3_func(self) -> Callable[..., list[str]]:
        return _to_orc

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
        return _to_orc(
            df=df,
            schema=schema,
            index=index,
            compression=compression,
            compression_ext=compression_ext,
            pyarrow_additional_kwargs=pyarrow_additional_kwargs,
            cpus=cpus,
            dtype=dtype,
            s3_client=s3_client,
            s3_additional_kwargs=s3_additional_kwargs,
            use_threads=use_threads,
            path=path,
            path_root=path_root,
            filename_prefix=filename_prefix,
            max_rows_by_file=max_rows_by_file,
            bucketing=bucketing,
            encryption_configuration=encryption_configuration,
        )

    def _create_glue_table(
        self,
        database: str,
        table: str,
        path: str,
        columns_types: dict[str, str],
        table_type: str | None = None,
        partitions_types: dict[str, str] | None = None,
        bucketing_info: BucketingInfoTuple | None = None,
        catalog_id: str | None = None,
        compression: str | None = None,
        description: str | None = None,
        parameters: dict[str, str] | None = None,
        columns_comments: dict[str, str] | None = None,
        mode: str = "overwrite",
        catalog_versioning: bool = False,
        athena_partition_projection_settings: AthenaPartitionProjectionSettings | None = None,
        boto3_session: boto3.Session | None = None,
        catalog_table_input: dict[str, Any] | None = None,
    ) -> None:
        return _create_orc_table(
            database=database,
            table=table,
            path=path,
            columns_types=columns_types,
            table_type=table_type,
            partitions_types=partitions_types,
            bucketing_info=bucketing_info,
            catalog_id=catalog_id,
            compression=compression,
            description=description,
            parameters=parameters,
            columns_comments=columns_comments,
            mode=mode,
            catalog_versioning=catalog_versioning,
            athena_partition_projection_settings=athena_partition_projection_settings,
            boto3_session=boto3_session,
            catalog_table_input=catalog_table_input,
        )

    def _add_glue_partitions(
        self,
        database: str,
        table: str,
        partitions_values: dict[str, list[str]],
        bucketing_info: BucketingInfoTuple | None = None,
        catalog_id: str | None = None,
        compression: str | None = None,
        boto3_session: boto3.Session | None = None,
        columns_types: dict[str, str] | None = None,
        partitions_parameters: dict[str, str] | None = None,
    ) -> None:
        return catalog.add_orc_partitions(
            database=database,
            table=table,
            partitions_values=partitions_values,
            bucketing_info=bucketing_info,
            compression=compression,
            boto3_session=boto3_session,
            catalog_id=catalog_id,
            columns_types=columns_types,
            partitions_parameters=partitions_parameters,
        )


@_utils.validate_distributed_kwargs(
    unsupported_kwargs=["boto3_session", "s3_additional_kwargs"],
)
@apply_configs
def to_orc(
    df: pd.DataFrame,
    path: str | None = None,
    index: bool = False,
    compression: str | None = None,
    pyarrow_additional_kwargs: dict[str, Any] | None = None,
    max_rows_by_file: int | None = None,
    use_threads: bool | int = True,
    boto3_session: boto3.Session | None = None,
    s3_additional_kwargs: dict[str, Any] | None = None,
    sanitize_columns: bool = False,
    dataset: bool = False,
    filename_prefix: str | None = None,
    partition_cols: list[str] | None = None,
    bucketing_info: BucketingInfoTuple | None = None,
    concurrent_partitioning: bool = False,
    mode: Literal["append", "overwrite", "overwrite_partitions"] | None = None,
    catalog_versioning: bool = False,
    schema_evolution: bool = True,
    database: str | None = None,
    table: str | None = None,
    glue_table_settings: GlueTableSettings | None = None,
    dtype: dict[str, str] | None = None,
    athena_partition_projection_settings: typing.AthenaPartitionProjectionSettings | None = None,
    catalog_id: str | None = None,
) -> _S3WriteDataReturnValue:
    """Write ORC file or dataset on Amazon S3.

    The concept of Dataset goes beyond the simple idea of ordinary files and enable more
    complex features like partitioning and catalog integration (Amazon Athena/AWS Glue Catalog).

    Note
    ----
    This operation may mutate the original pandas DataFrame in-place. To avoid this behaviour
    please pass in a deep copy instead (i.e. `df.copy()`)

    Note
    ----
    If `database` and `table` arguments are passed, the table name and all column names
    will be automatically sanitized using `wr.catalog.sanitize_table_name` and `wr.catalog.sanitize_column_name`.
    Please, pass `sanitize_columns=True` to enforce this behaviour always.

    Note
    ----
    In case of `use_threads=True` the number of threads
    that will be spawned will be gotten from os.cpu_count().

    Parameters
    ----------
    df: pandas.DataFrame
        Pandas DataFrame https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html
    path : str, optional
        S3 path (for file e.g. ``s3://bucket/prefix/filename.orc``) (for dataset e.g. ``s3://bucket/prefix``).
        Required if dataset=False or when dataset=True and creating a new dataset
    index : bool
        True to store the DataFrame index in file, otherwise False to ignore it.
        Is not supported in conjunction with `max_rows_by_file` when running the library with Ray/Modin.
    compression: str, optional
        Compression style (``None``, ``snappy``, ``gzip``, ``zstd``).
    pyarrow_additional_kwargs: dict[str, Any], optional
        Additional parameters forwarded to pyarrow.
        e.g. pyarrow_additional_kwargs={'coerce_timestamps': 'ns', 'use_deprecated_int96_timestamps': False,
        'allow_truncated_timestamps'=False}
    max_rows_by_file : int
        Max number of rows in each file.
        Default is None i.e. don't split the files.
        (e.g. 33554432, 268435456)
        Is not supported in conjunction with `index=True` when running the library with Ray/Modin.
    use_threads : bool, int
        True to enable concurrent requests, False to disable multiple threads.
        If enabled os.cpu_count() will be used as the max number of threads.
        If integer is provided, specified number is used.
    boto3_session : boto3.Session(), optional
        Boto3 Session. The default boto3 session will be used if boto3_session receive None.
    s3_additional_kwargs: dict[str, Any], optional
        Forwarded to botocore requests.
        e.g. s3_additional_kwargs={'ServerSideEncryption': 'aws:kms', 'SSEKMSKeyId': 'YOUR_KMS_KEY_ARN'}
    sanitize_columns : bool
        True to sanitize columns names (using `wr.catalog.sanitize_table_name` and `wr.catalog.sanitize_column_name`)
        or False to keep it as is.
        True value behaviour is enforced if `database` and `table` arguments are passed.
    dataset : bool
        If True store a orc dataset instead of a ordinary file(s)
        If True, enable all follow arguments:
        partition_cols, mode, database, table, description, parameters, columns_comments, concurrent_partitioning,
        catalog_versioning, projection_params, catalog_id, schema_evolution.
    filename_prefix: str, optional
        If dataset=True, add a filename prefix to the output files.
    partition_cols: List[str], optional
        List of column names that will be used to create partitions. Only takes effect if dataset=True.
    bucketing_info: Tuple[List[str], int], optional
        Tuple consisting of the column names used for bucketing as the first element and the number of buckets as the
        second element.
        Only `str`, `int` and `bool` are supported as column data types for bucketing.
    concurrent_partitioning: bool
        If True will increase the parallelism level during the partitions writing. It will decrease the
        writing time and increase the memory usage.
        https://aws-sdk-pandas.readthedocs.io/en/3.7.3/tutorials/022%20-%20Writing%20Partitions%20Concurrently.html
    mode: str, optional
        ``append`` (Default), ``overwrite``, ``overwrite_partitions``. Only takes effect if dataset=True.
    catalog_versioning : bool
        If True and `mode="overwrite"`, creates an archived version of the table catalog before updating it.
    schema_evolution : bool
        If True allows schema evolution (new or missing columns), otherwise a exception will be raised. True by default.
        (Only considered if dataset=True and mode in ("append", "overwrite_partitions"))
        Related tutorial:
        https://aws-sdk-pandas.readthedocs.io/en/3.7.3/tutorials/014%20-%20Schema%20Evolution.html
    database : str, optional
        Glue/Athena catalog: Database name.
    table : str, optional
        Glue/Athena catalog: Table name.
    glue_table_settings: dict (GlueTableSettings), optional
        Settings for writing to the Glue table.
    dtype : Dict[str, str], optional
        Dictionary of columns names and Athena/Glue types to be casted.
        Useful when you have columns with undetermined or mixed data types.
        (e.g. {'col name': 'bigint', 'col2 name': 'int'})
    athena_partition_projection_settings: typing.AthenaPartitionProjectionSettings, optional
        Parameters of the Athena Partition Projection (https://docs.aws.amazon.com/athena/latest/ug/partition-projection.html).
        AthenaPartitionProjectionSettings is a `TypedDict`, meaning the passed parameter can be instantiated either as an
        instance of AthenaPartitionProjectionSettings or as a regular Python dict.

        Following projection parameters are supported:

        .. list-table:: Projection Parameters
           :header-rows: 1

           * - Name
             - Type
             - Description
           * - projection_types
             - Optional[Dict[str, str]]
             - Dictionary of partitions names and Athena projections types.
               Valid types: "enum", "integer", "date", "injected"
               https://docs.aws.amazon.com/athena/latest/ug/partition-projection-supported-types.html
               (e.g. {'col_name': 'enum', 'col2_name': 'integer'})
           * - projection_ranges
             - Optional[Dict[str, str]]
             - Dictionary of partitions names and Athena projections ranges.
               https://docs.aws.amazon.com/athena/latest/ug/partition-projection-supported-types.html
               (e.g. {'col_name': '0,10', 'col2_name': '-1,8675309'})
           * - projection_values
             - Optional[Dict[str, str]]
             - Dictionary of partitions names and Athena projections values.
               https://docs.aws.amazon.com/athena/latest/ug/partition-projection-supported-types.html
               (e.g. {'col_name': 'A,B,Unknown', 'col2_name': 'foo,boo,bar'})
           * - projection_intervals
             - Optional[Dict[str, str]]
             - Dictionary of partitions names and Athena projections intervals.
               https://docs.aws.amazon.com/athena/latest/ug/partition-projection-supported-types.html
               (e.g. {'col_name': '1', 'col2_name': '5'})
           * - projection_digits
             - Optional[Dict[str, str]]
             - Dictionary of partitions names and Athena projections digits.
               https://docs.aws.amazon.com/athena/latest/ug/partition-projection-supported-types.html
               (e.g. {'col_name': '1', 'col2_name': '2'})
           * - projection_formats
             - Optional[Dict[str, str]]
             - Dictionary of partitions names and Athena projections formats.
               https://docs.aws.amazon.com/athena/latest/ug/partition-projection-supported-types.html
               (e.g. {'col_date': 'yyyy-MM-dd', 'col2_timestamp': 'yyyy-MM-dd HH:mm:ss'})
           * - projection_storage_location_template
             - Optional[str]
             - Value which is allows Athena to properly map partition values if the S3 file locations do not follow
               a typical `.../column=value/...` pattern.
               https://docs.aws.amazon.com/athena/latest/ug/partition-projection-setting-up.html
               (e.g. s3://bucket/table_root/a=${a}/${b}/some_static_subdirectory/${c}/)
    catalog_id : str, optional
        The ID of the Data Catalog from which to retrieve Databases.
        If none is provided, the AWS account ID is used by default.

    Returns
    -------
    wr.typing._S3WriteDataReturnValue
        Dictionary with:
        'paths': List of all stored files paths on S3.
        'partitions_values': Dictionary of partitions added with keys as S3 path locations
        and values as a list of partitions values as str.

    Examples
    --------
    Writing single file

    >>> import awswrangler as wr
    >>> import pandas as pd
    >>> wr.s3.to_orc(
    ...     df=pd.DataFrame({'col': [1, 2, 3]}),
    ...     path='s3://bucket/prefix/my_file.orc',
    ... )
    {
        'paths': ['s3://bucket/prefix/my_file.orc'],
        'partitions_values': {}
    }

    Writing single file encrypted with a KMS key

    >>> import awswrangler as wr
    >>> import pandas as pd
    >>> wr.s3.to_orc(
    ...     df=pd.DataFrame({'col': [1, 2, 3]}),
    ...     path='s3://bucket/prefix/my_file.orc',
    ...     s3_additional_kwargs={
    ...         'ServerSideEncryption': 'aws:kms',
    ...         'SSEKMSKeyId': 'YOUR_KMS_KEY_ARN'
    ...     }
    ... )
    {
        'paths': ['s3://bucket/prefix/my_file.orc'],
        'partitions_values': {}
    }

    Writing partitioned dataset

    >>> import awswrangler as wr
    >>> import pandas as pd
    >>> wr.s3.to_orc(
    ...     df=pd.DataFrame({
    ...         'col': [1, 2, 3],
    ...         'col2': ['A', 'A', 'B']
    ...     }),
    ...     path='s3://bucket/prefix',
    ...     dataset=True,
    ...     partition_cols=['col2']
    ... )
    {
        'paths': ['s3://.../col2=A/x.orc', 's3://.../col2=B/y.orc'],
        'partitions_values: {
            's3://.../col2=A/': ['A'],
            's3://.../col2=B/': ['B']
        }
    }

    Writing partitioned dataset with partition projection

    >>> import awswrangler as wr
    >>> import pandas as pd
    >>> from datetime import datetime
    >>> dt = lambda x: datetime.strptime(x, "%Y-%m-%d").date()
    >>> wr.s3.to_orc(
    ...     df=pd.DataFrame({
    ...         "id": [1, 2, 3],
    ...         "value": [1000, 1001, 1002],
    ...         "category": ['A', 'B', 'C'],
    ...     }),
    ...     path='s3://bucket/prefix',
    ...     dataset=True,
    ...     partition_cols=['value', 'category'],
    ...     athena_partition_projection_settings={
    ...        "projection_types": {
    ...             "value": "integer",
    ...             "category": "enum",
    ...         },
    ...         "projection_ranges": {
    ...             "value": "1000,2000",
    ...             "category": "A,B,C",
    ...         },
    ...     },
    ... )
    {
        'paths': [
            's3://.../value=1000/category=A/x.snappy.orc', ...
        ],
        'partitions_values': {
            's3://.../value=1000/category=A/': [
                '1000',
                'A',
            ], ...
        }
    }

    Writing bucketed dataset

    >>> import awswrangler as wr
    >>> import pandas as pd
    >>> wr.s3.to_orc(
    ...     df=pd.DataFrame({
    ...         'col': [1, 2, 3],
    ...         'col2': ['A', 'A', 'B']
    ...     }),
    ...     path='s3://bucket/prefix',
    ...     dataset=True,
    ...     bucketing_info=(["col2"], 2)
    ... )
    {
        'paths': ['s3://.../x_bucket-00000.csv', 's3://.../col2=B/x_bucket-00001.csv'],
        'partitions_values: {}
    }

    Writing dataset to S3 with metadata on Athena/Glue Catalog.

    >>> import awswrangler as wr
    >>> import pandas as pd
    >>> wr.s3.to_orc(
    ...     df=pd.DataFrame({
    ...         'col': [1, 2, 3],
    ...         'col2': ['A', 'A', 'B']
    ...     }),
    ...     path='s3://bucket/prefix',
    ...     dataset=True,
    ...     partition_cols=['col2'],
    ...     database='default',  # Athena/Glue database
    ...     table='my_table'  # Athena/Glue table
    ... )
    {
        'paths': ['s3://.../col2=A/x.orc', 's3://.../col2=B/y.orc'],
        'partitions_values: {
            's3://.../col2=A/': ['A'],
            's3://.../col2=B/': ['B']
        }
    }

    """
    glue_table_settings = cast(
        GlueTableSettings,
        glue_table_settings if glue_table_settings else {},
    )

    table_type = glue_table_settings.get("table_type")
    description = glue_table_settings.get("description")
    parameters = glue_table_settings.get("parameters")
    columns_comments = glue_table_settings.get("columns_comments")
    regular_partitions = glue_table_settings.get("regular_partitions", True)

    _validate_args(
        df=df,
        table=table,
        database=database,
        dataset=dataset,
        path=path,
        partition_cols=partition_cols,
        bucketing_info=bucketing_info,
        mode=mode,
        description=description,
        parameters=parameters,
        columns_comments=columns_comments,
        execution_engine=engine.get(),
    )

    # Evaluating compression
    if _COMPRESSION_2_EXT.get(compression, None) is None:
        raise exceptions.InvalidCompression(
            f"{compression} is invalid, please use None, 'snappy', 'zlib', 'lz4' or 'zstd'."
        )
    compression_ext: str = _COMPRESSION_2_EXT[compression]

    # Pyarrow defaults
    if not pyarrow_additional_kwargs:
        pyarrow_additional_kwargs = {}

    strategy = _S3ORCWriteStrategy()
    return strategy.write(
        df=df,
        path=path,
        index=index,
        compression=compression,
        pyarrow_additional_kwargs=pyarrow_additional_kwargs,
        max_rows_by_file=max_rows_by_file,
        use_threads=use_threads,
        boto3_session=boto3_session,
        s3_additional_kwargs=s3_additional_kwargs,
        sanitize_columns=sanitize_columns,
        dataset=dataset,
        filename_prefix=filename_prefix,
        partition_cols=partition_cols,
        bucketing_info=bucketing_info,
        concurrent_partitioning=concurrent_partitioning,
        mode=mode,
        catalog_versioning=catalog_versioning,
        schema_evolution=schema_evolution,
        database=database,
        table=table,
        description=description,
        parameters=parameters,
        columns_comments=columns_comments,
        table_type=table_type,
        regular_partitions=regular_partitions,
        dtype=dtype,
        athena_partition_projection_settings=athena_partition_projection_settings,
        catalog_id=catalog_id,
        compression_ext=compression_ext,
        encryption_configuration=None,
    )
