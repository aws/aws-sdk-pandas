"""Amazon S3 ORC Write Module (PRIVATE)."""

import logging
import math
import uuid
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any, Dict, Iterator, List, Literal, Optional, Union, cast

import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.orc

from awswrangler import _data_types, _utils, catalog, exceptions, lakeformation, typing
from awswrangler._arrow import _df_to_table
from awswrangler._config import apply_configs
from awswrangler._distributed import engine
from awswrangler._utils import copy_df_shallow
from awswrangler.catalog._create import _create_orc_table
from awswrangler.s3._delete import delete_objects
from awswrangler.s3._fs import open_s3_object
from awswrangler.s3._write import (
    _apply_dtype,
    _get_chunk_file_path,
    _get_file_path,
    _get_write_table_args,
    _sanitize,
    _validate_args,
)
from awswrangler.s3._write_concurrent import _WriteProxy
from awswrangler.s3._write_dataset import _to_dataset
from awswrangler.typing import BucketingInfoTuple, GlueTableSettings, _S3WriteDataReturnValue

if TYPE_CHECKING:
    from mypy_boto3_s3 import S3Client

_logger: logging.Logger = logging.getLogger(__name__)


_COMPRESSION_2_EXT: Dict[Optional[str], str] = {
    None: "",
    "snappy": ".snappy",
    "zlib": ".zlib",
    "lz4": ".lz4",
    "zstd": ".zstd",
}


@contextmanager
def _new_writer(
    file_path: str,
    compression: Optional[str],
    pyarrow_additional_kwargs: Optional[Dict[str, Any]],
    s3_client: "S3Client",
    s3_additional_kwargs: Optional[Dict[str, str]],
    use_threads: Union[bool, int],
) -> Iterator[pyarrow.orc.ORCWriter]:
    writer: Optional[pyarrow.orc.ORCWriter] = None
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
            writer = pyarrow.orc.ORCWriter(
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
        s3_client=s3_client,
        s3_additional_kwargs=s3_additional_kwargs,
        use_threads=use_threads,
    ) as writer:
        writer.write(table.slice(offset, chunk_size), **write_table_args)
    return [file_path]


def _to_orc_chunked(
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
def _to_orc(
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
        extension=".orc",
    )
    table: pa.Table = _df_to_table(df, schema, index, dtype)
    if max_rows_by_file is not None and max_rows_by_file > 0:
        paths: List[str] = _to_orc_chunked(
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


@_utils.validate_distributed_kwargs(
    unsupported_kwargs=["boto3_session", "s3_additional_kwargs"],
)
@apply_configs
def to_orc(  # pylint: disable=too-many-arguments,too-many-locals,too-many-branches,too-many-statements
    df: pd.DataFrame,
    path: Optional[str] = None,
    index: bool = False,
    compression: Optional[str] = None,
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
        https://aws-sdk-pandas.readthedocs.io/en/3.1.1/tutorials/022%20-%20Writing%20Partitions%20Concurrently.html
    mode: str, optional
        ``append`` (Default), ``overwrite``, ``overwrite_partitions``. Only takes effect if dataset=True.
    catalog_versioning : bool
        If True and `mode="overwrite"`, creates an archived version of the table catalog before updating it.
    schema_evolution : bool
        If True allows schema evolution (new or missing columns), otherwise a exception will be raised. True by default.
        (Only considered if dataset=True and mode in ("append", "overwrite_partitions"))
        Related tutorial:
        https://aws-sdk-pandas.readthedocs.io/en/3.1.1/tutorials/014%20-%20Schema%20Evolution.html
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

    Writing dataset to Glue governed table

    >>> import awswrangler as wr
    >>> import pandas as pd
    >>> wr.s3.to_orc(
    ...     df=pd.DataFrame({
    ...         'col': [1, 2, 3],
    ...         'col2': ['A', 'A', 'B'],
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
        'paths': ['s3://.../x.orc'],
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
        raise exceptions.InvalidCompression(
            f"{compression} is invalid, please use None, 'snappy', 'zlib', 'lz4' or 'zstd'."
        )
    compression_ext: str = _COMPRESSION_2_EXT[compression]

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

    # Pyarrow defaults
    if not pyarrow_additional_kwargs:
        pyarrow_additional_kwargs = {}

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
        paths = _to_orc(
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
                _create_orc_table(**create_table_args)  # pylint: disable=protected-access
                create_table_args["catalog_table_input"] = catalog._get_table_input(  # pylint: disable=protected-access
                    database=database,
                    table=table,
                    boto3_session=boto3_session,
                    transaction_id=transaction_id,
                    catalog_id=catalog_id,
                )

        paths, partitions_values = _to_dataset(
            func=_to_orc,
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
                _create_orc_table(**create_table_args)  # pylint: disable=protected-access
                if partitions_values and (regular_partitions is True) and (table_type != "GOVERNED"):
                    catalog.add_orc_partitions(
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
