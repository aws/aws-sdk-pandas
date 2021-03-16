"""Amazon PARQUET S3 Parquet Write Module (PRIVATE)."""

import logging
import math
import uuid
from contextlib import contextmanager
from typing import Any, Dict, Iterator, List, Optional, Tuple, Union

import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.lib
import pyarrow.parquet

from awswrangler import _data_types, _utils, catalog, exceptions
from awswrangler._config import apply_configs
from awswrangler.s3._delete import delete_objects
from awswrangler.s3._fs import open_s3_object
from awswrangler.s3._read_parquet import _read_parquet_metadata
from awswrangler.s3._write import _COMPRESSION_2_EXT, _apply_dtype, _sanitize, _validate_args
from awswrangler.s3._write_concurrent import _WriteProxy
from awswrangler.s3._write_dataset import _to_dataset

_logger: logging.Logger = logging.getLogger(__name__)


def _check_schema_changes(columns_types: Dict[str, str], table_input: Optional[Dict[str, Any]], mode: str) -> None:
    if (table_input is not None) and (mode in ("append", "overwrite_partitions")):
        catalog_cols: Dict[str, str] = {x["Name"]: x["Type"] for x in table_input["StorageDescriptor"]["Columns"]}
        for c, t in columns_types.items():
            if c not in catalog_cols:
                raise exceptions.InvalidArgumentValue(
                    f"Schema change detected: New column {c} with type {t}. "
                    "Please pass schema_evolution=True to allow new columns "
                    "behaviour."
                )
            if t != catalog_cols[c]:  # Data type change detected!
                raise exceptions.InvalidArgumentValue(
                    f"Schema change detected: Data type change on column {c} "
                    f"(Old type: {catalog_cols[c]} / New type {t})."
                )


def _get_file_path(file_counter: int, file_path: str) -> str:
    slash_index: int = file_path.rfind("/")
    dot_index: int = file_path.find(".", slash_index)
    file_index: str = "_" + str(file_counter)
    if dot_index == -1:
        file_path = file_path + file_index
    else:
        file_path = file_path[:dot_index] + file_index + file_path[dot_index:]
    return file_path


@contextmanager
def _new_writer(
    file_path: str,
    compression: Optional[str],
    schema: pa.Schema,
    boto3_session: boto3.Session,
    s3_additional_kwargs: Optional[Dict[str, str]],
    use_threads: bool,
) -> Iterator[pyarrow.parquet.ParquetWriter]:
    writer: Optional[pyarrow.parquet.ParquetWriter] = None
    with open_s3_object(
        path=file_path,
        mode="wb",
        use_threads=use_threads,
        s3_additional_kwargs=s3_additional_kwargs,
        boto3_session=boto3_session,
    ) as f:
        try:
            writer = pyarrow.parquet.ParquetWriter(
                where=f,
                write_statistics=True,
                use_dictionary=True,
                coerce_timestamps="ms",
                compression="NONE" if compression is None else compression,
                flavor="spark",
                schema=schema,
            )
            yield writer
        finally:
            if writer is not None and writer.is_open is True:
                writer.close()


def _write_chunk(
    file_path: str,
    boto3_session: Optional[boto3.Session],
    s3_additional_kwargs: Optional[Dict[str, str]],
    compression: Optional[str],
    table: pa.Table,
    offset: int,
    chunk_size: int,
    use_threads: bool,
) -> List[str]:
    with _new_writer(
        file_path=file_path,
        compression=compression,
        schema=table.schema,
        boto3_session=boto3_session,
        s3_additional_kwargs=s3_additional_kwargs,
        use_threads=use_threads,
    ) as writer:
        writer.write_table(table.slice(offset, chunk_size))
    return [file_path]


def _to_parquet_chunked(
    file_path: str,
    boto3_session: Optional[boto3.Session],
    s3_additional_kwargs: Optional[Dict[str, str]],
    compression: Optional[str],
    table: pa.Table,
    max_rows_by_file: int,
    num_of_rows: int,
    cpus: int,
) -> List[str]:
    chunks: int = math.ceil(num_of_rows / max_rows_by_file)
    use_threads: bool = cpus > 1
    proxy: _WriteProxy = _WriteProxy(use_threads=use_threads)
    for chunk in range(chunks):
        offset: int = chunk * max_rows_by_file
        write_path: str = _get_file_path(chunk, file_path)
        proxy.write(
            func=_write_chunk,
            file_path=write_path,
            boto3_session=boto3_session,
            s3_additional_kwargs=s3_additional_kwargs,
            compression=compression,
            table=table,
            offset=offset,
            chunk_size=max_rows_by_file,
            use_threads=use_threads,
        )
    return proxy.close()  # blocking


def _to_parquet(
    df: pd.DataFrame,
    schema: pa.Schema,
    index: bool,
    compression: Optional[str],
    compression_ext: str,
    cpus: int,
    dtype: Dict[str, str],
    boto3_session: Optional[boto3.Session],
    s3_additional_kwargs: Optional[Dict[str, str]],
    use_threads: bool,
    path: Optional[str] = None,
    path_root: Optional[str] = None,
    filename: Optional[str] = None,
    max_rows_by_file: Optional[int] = 0,
) -> List[str]:
    if path is None and path_root is not None:
        if filename is None:
            filename = uuid.uuid4().hex
        file_path: str = f"{path_root}{filename}{compression_ext}.parquet"
    elif path is not None and path_root is None:
        file_path = path
    else:
        raise RuntimeError("path and path_root received at the same time.")
    _logger.debug("file_path: %s", file_path)
    table: pa.Table = pyarrow.Table.from_pandas(df=df, schema=schema, nthreads=cpus, preserve_index=index, safe=True)
    for col_name, col_type in dtype.items():
        if col_name in table.column_names:
            col_index = table.column_names.index(col_name)
            pyarrow_dtype = _data_types.athena2pyarrow(col_type)
            field = pa.field(name=col_name, type=pyarrow_dtype)
            table = table.set_column(col_index, field, table.column(col_name).cast(pyarrow_dtype))
            _logger.debug("Casting column %s (%s) to %s (%s)", col_name, col_index, col_type, pyarrow_dtype)
    if max_rows_by_file is not None and max_rows_by_file > 0:
        paths: List[str] = _to_parquet_chunked(
            file_path=file_path,
            boto3_session=boto3_session,
            s3_additional_kwargs=s3_additional_kwargs,
            compression=compression,
            table=table,
            max_rows_by_file=max_rows_by_file,
            num_of_rows=df.shape[0],
            cpus=cpus,
        )
    else:
        with _new_writer(
            file_path=file_path,
            compression=compression,
            schema=table.schema,
            boto3_session=boto3_session,
            s3_additional_kwargs=s3_additional_kwargs,
            use_threads=use_threads,
        ) as writer:
            writer.write_table(table)
        paths = [file_path]
    return paths


@apply_configs
def to_parquet(  # pylint: disable=too-many-arguments,too-many-locals
    df: pd.DataFrame,
    path: Optional[str] = None,
    index: bool = False,
    compression: Optional[str] = "snappy",
    max_rows_by_file: Optional[int] = None,
    use_threads: bool = True,
    boto3_session: Optional[boto3.Session] = None,
    s3_additional_kwargs: Optional[Dict[str, Any]] = None,
    sanitize_columns: bool = False,
    dataset: bool = False,
    partition_cols: Optional[List[str]] = None,
    bucketing_info: Optional[Tuple[List[str], int]] = None,
    concurrent_partitioning: bool = False,
    mode: Optional[str] = None,
    catalog_versioning: bool = False,
    schema_evolution: bool = True,
    database: Optional[str] = None,
    table: Optional[str] = None,
    dtype: Optional[Dict[str, str]] = None,
    description: Optional[str] = None,
    parameters: Optional[Dict[str, str]] = None,
    columns_comments: Optional[Dict[str, str]] = None,
    regular_partitions: bool = True,
    projection_enabled: bool = False,
    projection_types: Optional[Dict[str, str]] = None,
    projection_ranges: Optional[Dict[str, str]] = None,
    projection_values: Optional[Dict[str, str]] = None,
    projection_intervals: Optional[Dict[str, str]] = None,
    projection_digits: Optional[Dict[str, str]] = None,
    catalog_id: Optional[str] = None,
) -> Dict[str, Union[List[str], Dict[str, List[str]]]]:
    """Write Parquet file or dataset on Amazon S3.

    The concept of Dataset goes beyond the simple idea of ordinary files and enable more
    complex features like partitioning and catalog integration (Amazon Athena/AWS Glue Catalog).

    Note
    ----
    If `database` and `table` arguments are passed, the table name and all column names
    will be automatically sanitized using `wr.catalog.sanitize_table_name` and `wr.catalog.sanitize_column_name`.
    Please, pass `sanitize_columns=True` to enforce this behaviour always.

    Note
    ----
    On `append` mode, the `parameters` will be upsert on an existing table.

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
    compression: str, optional
        Compression style (``None``, ``snappy``, ``gzip``).
    max_rows_by_file : int
        Max number of rows in each file.
        Default is None i.e. dont split the files.
        (e.g. 33554432, 268435456)
    use_threads : bool
        True to enable concurrent requests, False to disable multiple threads.
        If enabled os.cpu_count() will be used as the max number of threads.
    boto3_session : boto3.Session(), optional
        Boto3 Session. The default boto3 session will be used if boto3_session receive None.
    s3_additional_kwargs : Optional[Dict[str, Any]]
        Forward to botocore requests. Valid parameters: "ACL", "Metadata", "ServerSideEncryption", "StorageClass",
        "SSECustomerAlgorithm", "SSECustomerKey", "SSEKMSKeyId", "SSEKMSEncryptionContext", "Tagging",
        "RequestPayer", "ExpectedBucketOwner".
        e.g. s3_additional_kwargs={'ServerSideEncryption': 'aws:kms', 'SSEKMSKeyId': 'YOUR_KMS_KEY_ARN'}
    sanitize_columns : bool
        True to sanitize columns names (using `wr.catalog.sanitize_table_name` and `wr.catalog.sanitize_column_name`)
        or False to keep it as is.
        True value behaviour is enforced if `database` and `table` arguments are passed.
    dataset : bool
        If True store a parquet dataset instead of a ordinary file(s)
        If True, enable all follow arguments:
        partition_cols, mode, database, table, description, parameters, columns_comments, concurrent_partitioning,
        catalog_versioning, projection_enabled, projection_types, projection_ranges, projection_values,
        projection_intervals, projection_digits, catalog_id, schema_evolution.
    partition_cols: List[str], optional
        List of column names that will be used to create partitions. Only takes effect if dataset=True.
    bucketing_info: Tuple[List[str], int], optional
        Tuple consisting of the column names used for bucketing as the first element and the number of buckets as the
        second element.
        Only `str`, `int` and `bool` are supported as column data types for bucketing.
    concurrent_partitioning: bool
        If True will increase the parallelism level during the partitions writing. It will decrease the
        writing time and increase the memory usage.
        https://aws-data-wrangler.readthedocs.io/en/2.6.0/tutorials/022%20-%20Writing%20Partitions%20Concurrently.html
    mode: str, optional
        ``append`` (Default), ``overwrite``, ``overwrite_partitions``. Only takes effect if dataset=True.
        For details check the related tutorial:
        https://aws-data-wrangler.readthedocs.io/en/2.6.0/stubs/awswrangler.s3.to_parquet.html#awswrangler.s3.to_parquet
    catalog_versioning : bool
        If True and `mode="overwrite"`, creates an archived version of the table catalog before updating it.
    schema_evolution : bool
        If True allows schema evolution (new or missing columns), otherwise a exception will be raised.
        (Only considered if dataset=True and mode in ("append", "overwrite_partitions"))
        Related tutorial:
        https://aws-data-wrangler.readthedocs.io/en/2.6.0/tutorials/014%20-%20Schema%20Evolution.html
    database : str, optional
        Glue/Athena catalog: Database name.
    table : str, optional
        Glue/Athena catalog: Table name.
    dtype : Dict[str, str], optional
        Dictionary of columns names and Athena/Glue types to be casted.
        Useful when you have columns with undetermined or mixed data types.
        (e.g. {'col name': 'bigint', 'col2 name': 'int'})
    description : str, optional
        Glue/Athena catalog: Table description
    parameters : Dict[str, str], optional
        Glue/Athena catalog: Key/value pairs to tag the table.
    columns_comments : Dict[str, str], optional
        Glue/Athena catalog:
        Columns names and the related comments (e.g. {'col0': 'Column 0.', 'col1': 'Column 1.', 'col2': 'Partition.'}).
    regular_partitions : bool
        Create regular partitions (Non projected partitions) on Glue Catalog.
        Disable when you will work only with Partition Projection.
        Keep enabled even when working with projections is useful to keep
        Redshift Spectrum working with the regular partitions.
    projection_enabled : bool
        Enable Partition Projection on Athena (https://docs.aws.amazon.com/athena/latest/ug/partition-projection.html)
    projection_types : Optional[Dict[str, str]]
        Dictionary of partitions names and Athena projections types.
        Valid types: "enum", "integer", "date", "injected"
        https://docs.aws.amazon.com/athena/latest/ug/partition-projection-supported-types.html
        (e.g. {'col_name': 'enum', 'col2_name': 'integer'})
    projection_ranges: Optional[Dict[str, str]]
        Dictionary of partitions names and Athena projections ranges.
        https://docs.aws.amazon.com/athena/latest/ug/partition-projection-supported-types.html
        (e.g. {'col_name': '0,10', 'col2_name': '-1,8675309'})
    projection_values: Optional[Dict[str, str]]
        Dictionary of partitions names and Athena projections values.
        https://docs.aws.amazon.com/athena/latest/ug/partition-projection-supported-types.html
        (e.g. {'col_name': 'A,B,Unknown', 'col2_name': 'foo,boo,bar'})
    projection_intervals: Optional[Dict[str, str]]
        Dictionary of partitions names and Athena projections intervals.
        https://docs.aws.amazon.com/athena/latest/ug/partition-projection-supported-types.html
        (e.g. {'col_name': '1', 'col2_name': '5'})
    projection_digits: Optional[Dict[str, str]]
        Dictionary of partitions names and Athena projections digits.
        https://docs.aws.amazon.com/athena/latest/ug/partition-projection-supported-types.html
        (e.g. {'col_name': '1', 'col2_name': '2'})
    catalog_id : str, optional
        The ID of the Data Catalog from which to retrieve Databases.
        If none is provided, the AWS account ID is used by default.

    Returns
    -------
    Dict[str, Union[List[str], Dict[str, List[str]]]]
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
    )

    # Evaluating compression
    if _COMPRESSION_2_EXT.get(compression, None) is None:
        raise exceptions.InvalidCompression(f"{compression} is invalid, please use None, 'snappy' or 'gzip'.")
    compression_ext: str = _COMPRESSION_2_EXT[compression]

    # Initializing defaults
    partition_cols = partition_cols if partition_cols else []
    dtype = dtype if dtype else {}
    partitions_values: Dict[str, List[str]] = {}
    mode = "append" if mode is None else mode
    cpus: int = _utils.ensure_cpu_count(use_threads=use_threads)
    session: boto3.Session = _utils.ensure_session(session=boto3_session)

    # Sanitize table to respect Athena's standards
    if (sanitize_columns is True) or (database is not None and table is not None):
        df, dtype, partition_cols = _sanitize(df=df, dtype=dtype, partition_cols=partition_cols)

    # Evaluating dtype
    catalog_table_input: Optional[Dict[str, Any]] = None
    if database is not None and table is not None:
        catalog_table_input = catalog._get_table_input(  # pylint: disable=protected-access
            database=database, table=table, boto3_session=session, catalog_id=catalog_id
        )
        catalog_path = catalog_table_input["StorageDescriptor"]["Location"] if catalog_table_input else None
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
    _logger.debug("schema: \n%s", schema)

    if dataset is False:
        paths = _to_parquet(
            df=df,
            path=path,
            schema=schema,
            index=index,
            cpus=cpus,
            compression=compression,
            compression_ext=compression_ext,
            boto3_session=session,
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
                _check_schema_changes(columns_types=columns_types, table_input=catalog_table_input, mode=mode)
        paths, partitions_values = _to_dataset(
            func=_to_parquet,
            concurrent_partitioning=concurrent_partitioning,
            df=df,
            path_root=path,  # type: ignore
            index=index,
            compression=compression,
            compression_ext=compression_ext,
            cpus=cpus,
            use_threads=use_threads,
            partition_cols=partition_cols,
            bucketing_info=bucketing_info,
            dtype=dtype,
            mode=mode,
            boto3_session=session,
            s3_additional_kwargs=s3_additional_kwargs,
            schema=schema,
            max_rows_by_file=max_rows_by_file,
        )
        if (database is not None) and (table is not None):
            try:
                catalog._create_parquet_table(  # pylint: disable=protected-access
                    database=database,
                    table=table,
                    path=path,  # type: ignore
                    columns_types=columns_types,
                    partitions_types=partitions_types,
                    bucketing_info=bucketing_info,
                    compression=compression,
                    description=description,
                    parameters=parameters,
                    columns_comments=columns_comments,
                    boto3_session=session,
                    mode=mode,
                    catalog_versioning=catalog_versioning,
                    projection_enabled=projection_enabled,
                    projection_types=projection_types,
                    projection_ranges=projection_ranges,
                    projection_values=projection_values,
                    projection_intervals=projection_intervals,
                    projection_digits=projection_digits,
                    catalog_id=catalog_id,
                    catalog_table_input=catalog_table_input,
                )
                if partitions_values and (regular_partitions is True):
                    _logger.debug("partitions_values:\n%s", partitions_values)
                    catalog.add_parquet_partitions(
                        database=database,
                        table=table,
                        partitions_values=partitions_values,
                        bucketing_info=bucketing_info,
                        compression=compression,
                        boto3_session=session,
                        catalog_id=catalog_id,
                        columns_types=columns_types,
                    )
            except Exception:
                _logger.debug("Catalog write failed, cleaning up S3 (paths: %s).", paths)
                delete_objects(
                    path=paths,
                    use_threads=use_threads,
                    boto3_session=session,
                    s3_additional_kwargs=s3_additional_kwargs,
                )
                raise
    return {"paths": paths, "partitions_values": partitions_values}


@apply_configs
def store_parquet_metadata(  # pylint: disable=too-many-arguments
    path: str,
    database: str,
    table: str,
    catalog_id: Optional[str] = None,
    path_suffix: Optional[str] = None,
    path_ignore_suffix: Optional[str] = None,
    ignore_empty: bool = True,
    dtype: Optional[Dict[str, str]] = None,
    sampling: float = 1.0,
    dataset: bool = False,
    use_threads: bool = True,
    description: Optional[str] = None,
    parameters: Optional[Dict[str, str]] = None,
    columns_comments: Optional[Dict[str, str]] = None,
    compression: Optional[str] = None,
    mode: str = "overwrite",
    catalog_versioning: bool = False,
    regular_partitions: bool = True,
    projection_enabled: bool = False,
    projection_types: Optional[Dict[str, str]] = None,
    projection_ranges: Optional[Dict[str, str]] = None,
    projection_values: Optional[Dict[str, str]] = None,
    projection_intervals: Optional[Dict[str, str]] = None,
    projection_digits: Optional[Dict[str, str]] = None,
    s3_additional_kwargs: Optional[Dict[str, Any]] = None,
    boto3_session: Optional[boto3.Session] = None,
) -> Tuple[Dict[str, str], Optional[Dict[str, str]], Optional[Dict[str, List[str]]]]:
    """Infer and store parquet metadata on AWS Glue Catalog.

    Infer Apache Parquet file(s) metadata from from a received S3 prefix or list of S3 objects paths
    And then stores it on AWS Glue Catalog including all inferred partitions
    (No need of 'MCSK REPAIR TABLE')

    The concept of Dataset goes beyond the simple idea of files and enable more
    complex features like partitioning and catalog integration (AWS Glue Catalog).

    This function accepts Unix shell-style wildcards in the path argument.
    * (matches everything), ? (matches any single character),
    [seq] (matches any character in seq), [!seq] (matches any character not in seq).
    If you want to use a path which includes Unix shell-style wildcard characters (`*, ?, []`),
    you can use `glob.escape(path)` before passing the path to this function.

    Note
    ----
    On `append` mode, the `parameters` will be upsert on an existing table.

    Note
    ----
    In case of `use_threads=True` the number of threads
    that will be spawned will be gotten from os.cpu_count().

    Parameters
    ----------
    path : Union[str, List[str]]
        S3 prefix (accepts Unix shell-style wildcards)
        (e.g. s3://bucket/prefix) or list of S3 objects paths (e.g. [s3://bucket/key0, s3://bucket/key1]).
        database : str
        Glue/Athena catalog: Database name.
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
    use_threads : bool
        True to enable concurrent requests, False to disable multiple threads.
        If enabled os.cpu_count() will be used as the max number of threads.
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
    projection_enabled : bool
        Enable Partition Projection on Athena (https://docs.aws.amazon.com/athena/latest/ug/partition-projection.html)
    projection_types : Optional[Dict[str, str]]
        Dictionary of partitions names and Athena projections types.
        Valid types: "enum", "integer", "date", "injected"
        https://docs.aws.amazon.com/athena/latest/ug/partition-projection-supported-types.html
        (e.g. {'col_name': 'enum', 'col2_name': 'integer'})
    projection_ranges: Optional[Dict[str, str]]
        Dictionary of partitions names and Athena projections ranges.
        https://docs.aws.amazon.com/athena/latest/ug/partition-projection-supported-types.html
        (e.g. {'col_name': '0,10', 'col2_name': '-1,8675309'})
    projection_values: Optional[Dict[str, str]]
        Dictionary of partitions names and Athena projections values.
        https://docs.aws.amazon.com/athena/latest/ug/partition-projection-supported-types.html
        (e.g. {'col_name': 'A,B,Unknown', 'col2_name': 'foo,boo,bar'})
    projection_intervals: Optional[Dict[str, str]]
        Dictionary of partitions names and Athena projections intervals.
        https://docs.aws.amazon.com/athena/latest/ug/partition-projection-supported-types.html
        (e.g. {'col_name': '1', 'col2_name': '5'})
    projection_digits: Optional[Dict[str, str]]
        Dictionary of partitions names and Athena projections digits.
        https://docs.aws.amazon.com/athena/latest/ug/partition-projection-supported-types.html
        (e.g. {'col_name': '1', 'col2_name': '2'})
    s3_additional_kwargs : Optional[Dict[str, Any]]
        Forward to botocore requests. Valid parameters: "ACL", "Metadata", "ServerSideEncryption", "StorageClass",
        "SSECustomerAlgorithm", "SSECustomerKey", "SSEKMSKeyId", "SSEKMSEncryptionContext", "Tagging",
        "RequestPayer", "ExpectedBucketOwner".
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
    session: boto3.Session = _utils.ensure_session(session=boto3_session)
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
        use_threads=use_threads,
        s3_additional_kwargs=s3_additional_kwargs,
        boto3_session=session,
    )
    _logger.debug("columns_types: %s", columns_types)
    _logger.debug("partitions_types: %s", partitions_types)
    _logger.debug("partitions_values: %s", partitions_values)
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
        projection_enabled=projection_enabled,
        projection_types=projection_types,
        projection_ranges=projection_ranges,
        projection_values=projection_values,
        projection_intervals=projection_intervals,
        projection_digits=projection_digits,
        boto3_session=session,
        catalog_id=catalog_id,
    )
    if (partitions_types is not None) and (partitions_values is not None) and (regular_partitions is True):
        catalog.add_parquet_partitions(
            database=database,
            table=table,
            partitions_values=partitions_values,
            compression=compression,
            boto3_session=session,
            catalog_id=catalog_id,
            columns_types=columns_types,
        )
    return columns_types, partitions_types, partitions_values
