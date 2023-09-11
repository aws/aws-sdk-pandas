"""Amazon PARQUET S3 Parquet Write Module (PRIVATE)."""

import logging
import math
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any, Callable, Dict, Iterator, List, Literal, Optional, Tuple, Union, cast

import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.lib
import pyarrow.parquet

from awswrangler import _utils, catalog, exceptions, typing
from awswrangler._arrow import _df_to_table
from awswrangler._config import apply_configs
from awswrangler._distributed import engine
from awswrangler.catalog._create import _create_parquet_table
from awswrangler.s3._fs import open_s3_object
from awswrangler.s3._read_parquet import _read_parquet_metadata
from awswrangler.s3._write import (
    _COMPRESSION_2_EXT,
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

_logger: logging.Logger = logging.getLogger(__name__)


@contextmanager
def _new_writer(
    file_path: str,
    compression: Optional[str],
    pyarrow_additional_kwargs: Optional[Dict[str, Any]],
    schema: pa.Schema,
    s3_client: "S3Client",
    s3_additional_kwargs: Optional[Dict[str, str]],
    use_threads: Union[bool, int],
) -> Iterator[pyarrow.parquet.ParquetWriter]:
    writer: Optional[pyarrow.parquet.ParquetWriter] = None
    if not pyarrow_additional_kwargs:
        pyarrow_additional_kwargs = {}
    if not pyarrow_additional_kwargs.get("coerce_timestamps"):
        pyarrow_additional_kwargs["coerce_timestamps"] = "ms"
    if "flavor" not in pyarrow_additional_kwargs:
        pyarrow_additional_kwargs["flavor"] = "spark"
    if "version" not in pyarrow_additional_kwargs:
        # By default, use version 1.0 logical type set to maximize compatibility
        pyarrow_additional_kwargs["version"] = "1.0"
    if not pyarrow_additional_kwargs.get("use_dictionary"):
        pyarrow_additional_kwargs["use_dictionary"] = True
    if not pyarrow_additional_kwargs.get("write_statistics"):
        pyarrow_additional_kwargs["write_statistics"] = True
    if not pyarrow_additional_kwargs.get("schema"):
        pyarrow_additional_kwargs["schema"] = schema

    with open_s3_object(
        path=file_path,
        mode="wb",
        use_threads=use_threads,
        s3_additional_kwargs=s3_additional_kwargs,
        s3_client=s3_client,
    ) as f:
        try:
            writer = pyarrow.parquet.ParquetWriter(
                where=f,
                compression="NONE" if compression is None else compression,
                **pyarrow_additional_kwargs,
            )
            yield writer
        finally:
            if writer is not None and writer.is_open is True:
                writer.close()


def _write_chunk(
    file_path: str,
    s3_client: "S3Client",
    s3_additional_kwargs: Optional[Dict[str, str]],
    compression: Optional[str],
    pyarrow_additional_kwargs: Dict[str, str],
    table: pa.Table,
    offset: int,
    chunk_size: int,
    use_threads: Union[bool, int],
) -> List[str]:
    write_table_args = _get_write_table_args(pyarrow_additional_kwargs)
    with _new_writer(
        file_path=file_path,
        compression=compression,
        pyarrow_additional_kwargs=pyarrow_additional_kwargs,
        schema=table.schema,
        s3_client=s3_client,
        s3_additional_kwargs=s3_additional_kwargs,
        use_threads=use_threads,
    ) as writer:
        writer.write_table(table.slice(offset, chunk_size), **write_table_args)
    return [file_path]


def _to_parquet_chunked(
    file_path: str,
    s3_client: "S3Client",
    s3_additional_kwargs: Optional[Dict[str, str]],
    compression: Optional[str],
    pyarrow_additional_kwargs: Dict[str, Any],
    table: pa.Table,
    max_rows_by_file: int,
    num_of_rows: int,
    cpus: int,
) -> List[str]:
    chunks: int = math.ceil(num_of_rows / max_rows_by_file)
    use_threads: Union[bool, int] = cpus > 1
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
def _to_parquet(  # pylint: disable=unused-argument
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
    s3_client = s3_client if s3_client else _utils.client(service_name="s3")
    file_path = _get_file_path(
        path_root=path_root,
        path=path,
        filename_prefix=filename_prefix,
        compression_ext=compression_ext,
        extension=".parquet",
    )
    table: pa.Table = _df_to_table(df, schema, index, dtype)
    if max_rows_by_file is not None and max_rows_by_file > 0:
        paths: List[str] = _to_parquet_chunked(
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
            schema=table.schema,
            s3_client=s3_client,
            s3_additional_kwargs=s3_additional_kwargs,
            use_threads=use_threads,
        ) as writer:
            writer.write_table(table, **write_table_args)
        paths = [file_path]
    return paths


class _S3ParquetWriteStrategy(_S3WriteStrategy):
    @property
    def _write_to_s3_func(self) -> Callable[..., List[str]]:
        return _to_parquet

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
        return _to_parquet(
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
        )

    def _create_glue_table(
        self,
        database: str,
        table: str,
        path: str,
        columns_types: Dict[str, str],
        table_type: Optional[str] = None,
        partitions_types: Optional[Dict[str, str]] = None,
        bucketing_info: Optional[BucketingInfoTuple] = None,
        catalog_id: Optional[str] = None,
        compression: Optional[str] = None,
        description: Optional[str] = None,
        parameters: Optional[Dict[str, str]] = None,
        columns_comments: Optional[Dict[str, str]] = None,
        mode: str = "overwrite",
        catalog_versioning: bool = False,
        transaction_id: Optional[str] = None,
        athena_partition_projection_settings: Optional[AthenaPartitionProjectionSettings] = None,
        boto3_session: Optional[boto3.Session] = None,
        catalog_table_input: Optional[Dict[str, Any]] = None,
    ) -> None:
        return _create_parquet_table(
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
            transaction_id=transaction_id,
            athena_partition_projection_settings=athena_partition_projection_settings,
            boto3_session=boto3_session,
            catalog_table_input=catalog_table_input,
        )

    def _add_glue_partitions(
        self,
        database: str,
        table: str,
        partitions_values: Dict[str, List[str]],
        bucketing_info: Optional[BucketingInfoTuple] = None,
        catalog_id: Optional[str] = None,
        compression: Optional[str] = None,
        boto3_session: Optional[boto3.Session] = None,
        columns_types: Optional[Dict[str, str]] = None,
        partitions_parameters: Optional[Dict[str, str]] = None,
    ) -> None:
        return catalog.add_parquet_partitions(
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


@apply_configs
@_utils.validate_distributed_kwargs(
    unsupported_kwargs=["boto3_session", "s3_additional_kwargs"],
)
def to_parquet(  # pylint: disable=too-many-arguments,too-many-locals,too-many-branches,too-many-statements
    df: pd.DataFrame,
    path: Optional[str] = None,
    index: bool = False,
    compression: Optional[str] = "snappy",
    pyarrow_additional_kwargs: Optional[Dict[str, Any]] = None,
    max_rows_by_file: Optional[int] = None,
    use_threads: Union[bool, int] = True,
    boto3_session: Optional[boto3.Session] = None,
    s3_additional_kwargs: Optional[Dict[str, Any]] = None,
    sanitize_columns: bool = False,
    dataset: bool = False,
    filename_prefix: Optional[str] = None,
    partition_cols: Optional[List[str]] = None,
    bucketing_info: Optional[BucketingInfoTuple] = None,
    concurrent_partitioning: bool = False,
    mode: Optional[Literal["append", "overwrite", "overwrite_partitions"]] = None,
    catalog_versioning: bool = False,
    schema_evolution: bool = True,
    database: Optional[str] = None,
    table: Optional[str] = None,
    glue_table_settings: Optional[GlueTableSettings] = None,
    dtype: Optional[Dict[str, str]] = None,
    athena_partition_projection_settings: Optional[typing.AthenaPartitionProjectionSettings] = None,
    catalog_id: Optional[str] = None,
) -> _S3WriteDataReturnValue:
    """Write Parquet file or dataset on Amazon S3.

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
        S3 path (for file e.g. ``s3://bucket/prefix/filename.parquet``) (for dataset e.g. ``s3://bucket/prefix``).
        Required if dataset=False or when dataset=True and creating a new dataset
    index : bool
        True to store the DataFrame index in file, otherwise False to ignore it.
        Is not supported in conjunction with `max_rows_by_file` when running the library with Ray/Modin.
    compression: str, optional
        Compression style (``None``, ``snappy``, ``gzip``, ``zstd``).
    pyarrow_additional_kwargs : Optional[Dict[str, Any]]
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
    s3_additional_kwargs : Optional[Dict[str, Any]]
        Forwarded to botocore requests.
        e.g. s3_additional_kwargs={'ServerSideEncryption': 'aws:kms', 'SSEKMSKeyId': 'YOUR_KMS_KEY_ARN'}
    sanitize_columns : bool
        True to sanitize columns names (using `wr.catalog.sanitize_table_name` and `wr.catalog.sanitize_column_name`)
        or False to keep it as is.
        True value behaviour is enforced if `database` and `table` arguments are passed.
    dataset : bool
        If True store a parquet dataset instead of a ordinary file(s)
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
        https://aws-sdk-pandas.readthedocs.io/en/3.4.0/tutorials/022%20-%20Writing%20Partitions%20Concurrently.html
    mode: str, optional
        ``append`` (Default), ``overwrite``, ``overwrite_partitions``. Only takes effect if dataset=True.
        For details check the related tutorial:
        https://aws-sdk-pandas.readthedocs.io/en/3.4.0/tutorials/004%20-%20Parquet%20Datasets.html
    catalog_versioning : bool
        If True and `mode="overwrite"`, creates an archived version of the table catalog before updating it.
    schema_evolution : bool
        If True allows schema evolution (new or missing columns), otherwise a exception will be raised. True by default.
        (Only considered if dataset=True and mode in ("append", "overwrite_partitions"))
        Related tutorial:
        https://aws-sdk-pandas.readthedocs.io/en/3.4.0/tutorials/014%20-%20Schema%20Evolution.html
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
        Parameters of the Athena Partition Projection
        (https://docs.aws.amazon.com/athena/latest/ug/partition-projection.html).
        AthenaPartitionProjectionSettings is a `TypedDict`, meaning the passed parameter can be instantiated either as
        an instance of AthenaPartitionProjectionSettings or as a regular Python dict.

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
    >>> wr.s3.to_parquet(
    ...     df=pd.DataFrame({'col': [1, 2, 3]}),
    ...     path='s3://bucket/prefix/my_file.parquet',
    ... )
    {
        'paths': ['s3://bucket/prefix/my_file.parquet'],
        'partitions_values': {}
    }

    Writing single file encrypted with a KMS key

    >>> import awswrangler as wr
    >>> import pandas as pd
    >>> wr.s3.to_parquet(
    ...     df=pd.DataFrame({'col': [1, 2, 3]}),
    ...     path='s3://bucket/prefix/my_file.parquet',
    ...     s3_additional_kwargs={
    ...         'ServerSideEncryption': 'aws:kms',
    ...         'SSEKMSKeyId': 'YOUR_KMS_KEY_ARN'
    ...     }
    ... )
    {
        'paths': ['s3://bucket/prefix/my_file.parquet'],
        'partitions_values': {}
    }

    Writing partitioned dataset

    >>> import awswrangler as wr
    >>> import pandas as pd
    >>> wr.s3.to_parquet(
    ...     df=pd.DataFrame({
    ...         'col': [1, 2, 3],
    ...         'col2': ['A', 'A', 'B']
    ...     }),
    ...     path='s3://bucket/prefix',
    ...     dataset=True,
    ...     partition_cols=['col2']
    ... )
    {
        'paths': ['s3://.../col2=A/x.parquet', 's3://.../col2=B/y.parquet'],
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
    >>> wr.s3.to_parquet(
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
            's3://.../value=1000/category=A/x.snappy.parquet', ...
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
    >>> wr.s3.to_parquet(
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
    >>> wr.s3.to_parquet(
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
        'paths': ['s3://.../col2=A/x.parquet', 's3://.../col2=B/y.parquet'],
        'partitions_values: {
            's3://.../col2=A/': ['A'],
            's3://.../col2=B/': ['B']
        }
    }

    Writing dataset to Glue governed table

    >>> import awswrangler as wr
    >>> import pandas as pd
    >>> wr.s3.to_parquet(
    ...     df=pd.DataFrame({
    ...         'col': [1, 2, 3],
    ...         'col2': ['A', 'A', 'B'],
    ...         'col3': [None, None, None]
    ...     }),
    ...     dataset=True,
    ...     mode='append',
    ...     database='default',  # Athena/Glue database
    ...     table='my_table',  # Athena/Glue table
    ...     glue_table_settings=wr.typing.GlueTableSettings(
    ...         table_type="GOVERNED",
    ...         transaction_id="xxx",
    ...     ),
    ... )
    {
        'paths': ['s3://.../x.parquet'],
        'partitions_values: {}
    }

    Writing dataset casting empty column data type

    >>> import awswrangler as wr
    >>> import pandas as pd
    >>> wr.s3.to_parquet(
    ...     df=pd.DataFrame({
    ...         'col': [1, 2, 3],
    ...         'col2': ['A', 'A', 'B'],
    ...         'col3': [None, None, None]
    ...     }),
    ...     path='s3://bucket/prefix',
    ...     dataset=True,
    ...     database='default',  # Athena/Glue database
    ...     table='my_table'  # Athena/Glue table
    ...     dtype={'col3': 'date'}
    ... )
    {
        'paths': ['s3://.../x.parquet'],
        'partitions_values: {}
    }

    """
    glue_table_settings = cast(
        GlueTableSettings,
        glue_table_settings if glue_table_settings else {},
    )

    table_type = glue_table_settings.get("table_type")
    transaction_id = glue_table_settings.get("transaction_id")
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
        raise exceptions.InvalidCompression(f"{compression} is invalid, please use None, 'snappy', 'gzip' or 'zstd'.")
    compression_ext: str = _COMPRESSION_2_EXT[compression]

    # Pyarrow defaults
    if not pyarrow_additional_kwargs:
        pyarrow_additional_kwargs = {}
    if not pyarrow_additional_kwargs.get("coerce_timestamps"):
        pyarrow_additional_kwargs["coerce_timestamps"] = "ms"
    if "flavor" not in pyarrow_additional_kwargs:
        pyarrow_additional_kwargs["flavor"] = "spark"

    strategy = _S3ParquetWriteStrategy()
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
        transaction_id=transaction_id,
        regular_partitions=regular_partitions,
        dtype=dtype,
        athena_partition_projection_settings=athena_partition_projection_settings,
        catalog_id=catalog_id,
        compression_ext=compression_ext,
    )


@apply_configs
@_utils.validate_distributed_kwargs(
    unsupported_kwargs=["boto3_session"],
)
def store_parquet_metadata(  # pylint: disable=too-many-arguments,too-many-locals
    path: str,
    database: str,
    table: str,
    catalog_id: Optional[str] = None,
    path_suffix: Optional[str] = None,
    path_ignore_suffix: Union[str, List[str], None] = None,
    ignore_empty: bool = True,
    ignore_null: bool = False,
    dtype: Optional[Dict[str, str]] = None,
    sampling: float = 1.0,
    dataset: bool = False,
    use_threads: Union[bool, int] = True,
    description: Optional[str] = None,
    parameters: Optional[Dict[str, str]] = None,
    columns_comments: Optional[Dict[str, str]] = None,
    compression: Optional[str] = None,
    mode: Literal["append", "overwrite"] = "overwrite",
    catalog_versioning: bool = False,
    regular_partitions: bool = True,
    athena_partition_projection_settings: Optional[typing.AthenaPartitionProjectionSettings] = None,
    s3_additional_kwargs: Optional[Dict[str, Any]] = None,
    boto3_session: Optional[boto3.Session] = None,
) -> Tuple[Dict[str, str], Optional[Dict[str, str]], Optional[Dict[str, List[str]]]]:
    """Infer and store parquet metadata on AWS Glue Catalog.

    Infer Apache Parquet file(s) metadata from a received S3 prefix
    And then stores it on AWS Glue Catalog including all inferred partitions
    (No need for 'MSCK REPAIR TABLE')

    The concept of Dataset goes beyond the simple idea of files and enables more
    complex features like partitioning and catalog integration (AWS Glue Catalog).

    This function accepts Unix shell-style wildcards in the path argument.
    * (matches everything), ? (matches any single character),
    [seq] (matches any character in seq), [!seq] (matches any character not in seq).
    If you want to use a path which includes Unix shell-style wildcard characters (`*, ?, []`),
    you can use `glob.escape(path)` before passing the path to this function.

    Note
    ----
    In case of `use_threads=True` the number of threads
    that will be spawned will be gotten from os.cpu_count().

    Parameters
    ----------
    path : str
        S3 prefix (accepts Unix shell-style wildcards) (e.g. s3://bucket/prefix).
    table : str
        Glue/Athena catalog: Table name.
    database : str
        AWS Glue Catalog database name.
    catalog_id : str, optional
        The ID of the Data Catalog from which to retrieve Databases.
        If none is provided, the AWS account ID is used by default.
    path_suffix: Union[str, List[str], None]
        Suffix or List of suffixes for filtering S3 keys.
    path_ignore_suffix: Union[str, List[str], None]
        Suffix or List of suffixes for S3 keys to be ignored.
    ignore_empty: bool
        Ignore files with 0 bytes.
    ignore_null: bool
        Ignore columns with null type.
    dtype : Dict[str, str], optional
        Dictionary of columns names and Athena/Glue types to be casted.
        Useful when you have columns with undetermined data types as partitions columns.
        (e.g. {'col name': 'bigint', 'col2 name': 'int'})
    sampling : float
        Random sample ratio of files that will have the metadata inspected.
        Must be `0.0 < sampling <= 1.0`.
        The higher, the more accurate.
        The lower, the faster.
    dataset: bool
        If True read a parquet dataset instead of simple file(s) loading all the related partitions as columns.
    use_threads : bool, int
        True to enable concurrent requests, False to disable multiple threads.
        If enabled os.cpu_count() will be used as the max number of threads.
        If integer is provided, specified number is used.
    description: str, optional
        Glue/Athena catalog: Table description
    parameters: Dict[str, str], optional
        Glue/Athena catalog: Key/value pairs to tag the table.
    columns_comments: Dict[str, str], optional
        Glue/Athena catalog:
        Columns names and the related comments (e.g. {'col0': 'Column 0.', 'col1': 'Column 1.', 'col2': 'Partition.'}).
    compression: str, optional
        Compression style (``None``, ``snappy``, ``gzip``, etc).
    mode: str
        'overwrite' to recreate any possible existing table or 'append' to keep any possible existing table.
    catalog_versioning : bool
        If True and `mode="overwrite"`, creates an archived version of the table catalog before updating it.
    regular_partitions : bool
        Create regular partitions (Non projected partitions) on Glue Catalog.
        Disable when you will work only with Partition Projection.
        Keep enabled even when working with projections is useful to keep
        Redshift Spectrum working with the regular partitions.
    athena_partition_projection_settings: typing.AthenaPartitionProjectionSettings, optional
        Parameters of the Athena Partition Projection
        (https://docs.aws.amazon.com/athena/latest/ug/partition-projection.html).
        AthenaPartitionProjectionSettings is a `TypedDict`, meaning the passed parameter can be instantiated either as
        an instance of AthenaPartitionProjectionSettings or as a regular Python dict.

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
    s3_additional_kwargs : Optional[Dict[str, Any]]
        Forwarded to botocore requests.
        e.g. s3_additional_kwargs={'ServerSideEncryption': 'aws:kms', 'SSEKMSKeyId': 'YOUR_KMS_KEY_ARN'}
    boto3_session : boto3.Session(), optional
        Boto3 Session. The default boto3 session will be used if boto3_session receive None.

    Returns
    -------
    Tuple[Dict[str, str], Optional[Dict[str, str]], Optional[Dict[str, List[str]]]]
        The metadata used to create the Glue Table.
        columns_types: Dictionary with keys as column names and values as
        data types (e.g. {'col0': 'bigint', 'col1': 'double'}). /
        partitions_types: Dictionary with keys as partition names
        and values as data types (e.g. {'col2': 'date'}). /
        partitions_values: Dictionary with keys as S3 path locations and values as a
        list of partitions values as str (e.g. {'s3://bucket/prefix/y=2020/m=10/': ['2020', '10']}).

    Examples
    --------
    Reading all Parquet files metadata under a prefix

    >>> import awswrangler as wr
    >>> columns_types, partitions_types, partitions_values = wr.s3.store_parquet_metadata(
    ...     path='s3://bucket/prefix/',
    ...     database='...',
    ...     table='...',
    ...     dataset=True
    ... )

    """
    columns_types: Dict[str, str]
    partitions_types: Optional[Dict[str, str]]
    partitions_values: Optional[Dict[str, List[str]]]
    columns_types, partitions_types, partitions_values = _read_parquet_metadata(
        path=path,
        dtype=dtype,
        sampling=sampling,
        dataset=dataset,
        path_suffix=path_suffix,
        path_ignore_suffix=path_ignore_suffix,
        ignore_empty=ignore_empty,
        ignore_null=ignore_null,
        use_threads=use_threads,
        s3_additional_kwargs=s3_additional_kwargs,
        boto3_session=boto3_session,
    )
    _logger.debug("Resolved columns_types: %s", columns_types)
    _logger.debug("Resolved partitions_types: %s", partitions_types)
    _logger.debug("Resolved partitions_values: %s", partitions_values)
    catalog.create_parquet_table(
        database=database,
        table=table,
        path=path,
        columns_types=columns_types,
        partitions_types=partitions_types,
        description=description,
        parameters=parameters,
        columns_comments=columns_comments,
        mode=mode,
        compression=compression,
        catalog_versioning=catalog_versioning,
        athena_partition_projection_settings=athena_partition_projection_settings,
        boto3_session=boto3_session,
        catalog_id=catalog_id,
    )
    if (partitions_types is not None) and (partitions_values is not None) and (regular_partitions is True):
        catalog.add_parquet_partitions(
            database=database,
            table=table,
            partitions_values=partitions_values,
            compression=compression,
            boto3_session=boto3_session,
            catalog_id=catalog_id,
            columns_types=columns_types,
        )
    return columns_types, partitions_types, partitions_values
