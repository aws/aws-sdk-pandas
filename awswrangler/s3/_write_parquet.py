"""Amazon PARQUET S3 Write Module (PRIVATE)."""

import logging
import uuid
from typing import Dict, List, Optional, Tuple, Union

import boto3  # type: ignore
import pandas as pd  # type: ignore
import pyarrow as pa  # type: ignore
import pyarrow.lib  # type: ignore
import pyarrow.parquet  # type: ignore
import s3fs  # type: ignore

from awswrangler import _data_types, _utils, catalog, exceptions
from awswrangler.s3._delete import delete_objects
from awswrangler.s3._read_parquet import _read_parquet_metadata
from awswrangler.s3._write import _COMPRESSION_2_EXT
from awswrangler.s3._write_concurrent import _write_proxy, _WriteProxy

_logger: logging.Logger = logging.getLogger(__name__)


def _to_parquet_file(
    df: pd.DataFrame,
    path: str,
    schema: pa.Schema,
    index: bool,
    compression: Optional[str],
    cpus: int,
    dtype: Dict[str, str],
    boto3_session: Optional[boto3.Session],
    s3_additional_kwargs: Optional[Dict[str, str]],
) -> str:
    _logger.debug("path: %s", path)
    table: pa.Table = pyarrow.Table.from_pandas(df=df, schema=schema, nthreads=cpus, preserve_index=index, safe=True)
    for col_name, col_type in dtype.items():
        if col_name in table.column_names:
            col_index = table.column_names.index(col_name)
            pyarrow_dtype = _data_types.athena2pyarrow(col_type)
            field = pa.field(name=col_name, type=pyarrow_dtype)
            table = table.set_column(col_index, field, table.column(col_name).cast(pyarrow_dtype))
            _logger.debug("Casting column %s (%s) to %s (%s)", col_name, col_index, col_type, pyarrow_dtype)
    fs: s3fs.S3FileSystem = _utils.get_fs(
        block_size=33_554_432, session=boto3_session, s3_additional_kwargs=s3_additional_kwargs  # 32 MB (32 * 2**20)
    )
    with pyarrow.parquet.ParquetWriter(
        where=path,
        write_statistics=True,
        use_dictionary=True,
        filesystem=fs,
        coerce_timestamps="ms",
        compression=compression,
        flavor="spark",
        schema=table.schema,
    ) as writer:
        writer.write_table(table)
    return path


def _to_parquet_dataset(
    df: pd.DataFrame,
    path: str,
    index: bool,
    compression: Optional[str],
    compression_ext: str,
    cpus: int,
    use_threads: bool,
    mode: str,
    dtype: Dict[str, str],
    partition_cols: Optional[List[str]],
    boto3_session: Optional[boto3.Session],
    s3_additional_kwargs: Optional[Dict[str, str]],
) -> Tuple[List[str], Dict[str, List[str]]]:
    paths: List[str] = []
    partitions_values: Dict[str, List[str]] = {}
    path = path if path[-1] == "/" else f"{path}/"
    if mode not in ["append", "overwrite", "overwrite_partitions"]:
        raise exceptions.InvalidArgumentValue(
            f"{mode} is a invalid mode, please use append, overwrite or overwrite_partitions."
        )
    if (mode == "overwrite") or ((mode == "overwrite_partitions") and (not partition_cols)):
        delete_objects(path=path, use_threads=use_threads, boto3_session=boto3_session)
    df = _data_types.cast_pandas_with_athena_types(df=df, dtype=dtype)
    schema: pa.Schema = _data_types.pyarrow_schema_from_pandas(
        df=df, index=index, ignore_cols=partition_cols, dtype=dtype
    )
    _logger.debug("schema: \n%s", schema)
    if not partition_cols:
        file_path: str = f"{path}{uuid.uuid4().hex}{compression_ext}.parquet"
        _to_parquet_file(
            df=df,
            schema=schema,
            path=file_path,
            index=index,
            compression=compression,
            cpus=cpus,
            boto3_session=boto3_session,
            s3_additional_kwargs=s3_additional_kwargs,
            dtype=dtype,
        )
        paths.append(file_path)
    else:
        proxy: _WriteProxy
        with _write_proxy(use_threads=use_threads) as proxy:
            for keys, subgroup in df.groupby(by=partition_cols, observed=True):
                subgroup = subgroup.drop(partition_cols, axis="columns")
                keys = (keys,) if not isinstance(keys, tuple) else keys
                subdir = "/".join([f"{name}={val}" for name, val in zip(partition_cols, keys)])
                prefix: str = f"{path}{subdir}/"
                if mode == "overwrite_partitions":
                    delete_objects(path=prefix, use_threads=use_threads, boto3_session=boto3_session)
                file_path = f"{prefix}{uuid.uuid4().hex}{compression_ext}.parquet"
                proxy.write(
                    func=_to_parquet_file,
                    boto3_session=boto3_session,
                    df=subgroup,
                    schema=schema,
                    path=file_path,
                    index=index,
                    compression=compression,
                    cpus=cpus,
                    dtype=dtype,
                    s3_additional_kwargs=s3_additional_kwargs,
                )
                paths.append(file_path)
                partitions_values[prefix] = [str(k) for k in keys]
    return paths, partitions_values


def to_parquet(  # pylint: disable=too-many-arguments,too-many-locals
    df: pd.DataFrame,
    path: str,
    index: bool = False,
    compression: Optional[str] = "snappy",
    use_threads: bool = True,
    boto3_session: Optional[boto3.Session] = None,
    s3_additional_kwargs: Optional[Dict[str, str]] = None,
    sanitize_columns: bool = False,
    dataset: bool = False,
    partition_cols: Optional[List[str]] = None,
    mode: Optional[str] = None,
    catalog_versioning: bool = False,
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
) -> Dict[str, Union[List[str], Dict[str, List[str]]]]:
    """Write Parquet file or dataset on Amazon S3.

    The concept of Dataset goes beyond the simple idea of files and enable more
    complex features like partitioning, casting and catalog integration (Amazon Athena/AWS Glue Catalog).

    Note
    ----
    If `dataset=True` The table name and all column names will be automatically sanitized using
    `wr.catalog.sanitize_table_name` and `wr.catalog.sanitize_column_name`.
    Please, pass `sanitize_columns=True` to force the same behaviour for `dataset=False`.

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
    path : str
        S3 path (for file e.g. ``s3://bucket/prefix/filename.parquet``) (for dataset e.g. ``s3://bucket/prefix``).
    index : bool
        True to store the DataFrame index in file, otherwise False to ignore it.
    compression: str, optional
        Compression style (``None``, ``snappy``, ``gzip``).
    use_threads : bool
        True to enable concurrent requests, False to disable multiple threads.
        If enabled os.cpu_count() will be used as the max number of threads.
    boto3_session : boto3.Session(), optional
        Boto3 Session. The default boto3 session will be used if boto3_session receive None.
    s3_additional_kwargs:
        Forward to s3fs, useful for server side encryption
        https://s3fs.readthedocs.io/en/latest/#serverside-encryption
    sanitize_columns : bool
        True to sanitize columns names or False to keep it as is.
        True value is forced if `dataset=True`.
    dataset : bool
        If True store a parquet dataset instead of a single file.
        If True, enable all follow arguments:
        partition_cols, mode, database, table, description, parameters, columns_comments, .
    partition_cols: List[str], optional
        List of column names that will be used to create partitions. Only takes effect if dataset=True.
    mode: str, optional
        ``append`` (Default), ``overwrite``, ``overwrite_partitions``. Only takes effect if dataset=True.
    catalog_versioning : bool
        If True and `mode="overwrite"`, creates an archived version of the table catalog before updating it.
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
    ...         'SSEKMSKeyId': 'YOUR_KMY_KEY_ARN'
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
    if (database is None) ^ (table is None):
        raise exceptions.InvalidArgumentCombination(
            "Please pass database and table arguments to be able to store the metadata into the Athena/Glue Catalog."
        )
    if df.empty is True:
        raise exceptions.EmptyDataFrame()

    partition_cols = partition_cols if partition_cols else []
    dtype = dtype if dtype else {}
    partitions_values: Dict[str, List[str]] = {}

    # Sanitize table to respect Athena's standards
    if (sanitize_columns is True) or (dataset is True):
        df = catalog.sanitize_dataframe_columns_names(df=df)
        partition_cols = [catalog.sanitize_column_name(p) for p in partition_cols]
        dtype = {catalog.sanitize_column_name(k): v.lower() for k, v in dtype.items()}
        _utils.check_duplicated_columns(df=df)

    session: boto3.Session = _utils.ensure_session(session=boto3_session)
    cpus: int = _utils.ensure_cpu_count(use_threads=use_threads)
    compression_ext: Optional[str] = _COMPRESSION_2_EXT.get(compression, None)
    if compression_ext is None:
        raise exceptions.InvalidCompression(f"{compression} is invalid, please use None, snappy or gzip.")
    if dataset is False:
        if path.endswith("/"):
            raise exceptions.InvalidArgumentValue(
                "If <dataset=False>, the argument <path> should be a object path, not a directory."
            )
        if partition_cols:
            raise exceptions.InvalidArgumentCombination("Please, pass dataset=True to be able to use partition_cols.")
        if mode is not None:
            raise exceptions.InvalidArgumentCombination("Please pass dataset=True to be able to use mode.")
        if any(arg is not None for arg in (database, table, description, parameters)):
            raise exceptions.InvalidArgumentCombination(
                "Please pass dataset=True to be able to use any one of these "
                "arguments: database, table, description, parameters, "
                "columns_comments."
            )
        df = _data_types.cast_pandas_with_athena_types(df=df, dtype=dtype)
        schema: pa.Schema = _data_types.pyarrow_schema_from_pandas(
            df=df, index=index, ignore_cols=partition_cols, dtype=dtype
        )
        _logger.debug("schema: \n%s", schema)
        paths = [
            _to_parquet_file(
                df=df,
                path=path,
                schema=schema,
                index=index,
                compression=compression,
                cpus=cpus,
                boto3_session=boto3_session,
                s3_additional_kwargs=s3_additional_kwargs,
                dtype=dtype,
            )
        ]
    else:
        mode = "append" if mode is None else mode
        if (
            (mode in ("append", "overwrite_partitions")) and (database is not None) and (table is not None)
        ):  # Fetching Catalog Types
            catalog_types: Optional[Dict[str, str]] = catalog.get_table_types(
                database=database, table=table, boto3_session=session
            )
            if catalog_types is not None:
                for k, v in catalog_types.items():
                    dtype[k] = v
        paths, partitions_values = _to_parquet_dataset(
            df=df,
            path=path,
            index=index,
            compression=compression,
            compression_ext=compression_ext,
            cpus=cpus,
            use_threads=use_threads,
            partition_cols=partition_cols,
            dtype=dtype,
            mode=mode,
            boto3_session=boto3_session,
            s3_additional_kwargs=s3_additional_kwargs,
        )
        if (database is not None) and (table is not None):
            columns_types, partitions_types = _data_types.athena_types_from_pandas_partitioned(
                df=df, index=index, partition_cols=partition_cols, dtype=dtype
            )
            catalog.create_parquet_table(
                database=database,
                table=table,
                path=path,
                columns_types=columns_types,
                partitions_types=partitions_types,
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
            )
            if partitions_values and (regular_partitions is True):
                _logger.debug("partitions_values:\n%s", partitions_values)
                catalog.add_parquet_partitions(
                    database=database,
                    table=table,
                    partitions_values=partitions_values,
                    compression=compression,
                    boto3_session=session,
                )
    return {"paths": paths, "partitions_values": partitions_values}


def store_parquet_metadata(  # pylint: disable=too-many-arguments
    path: str,
    database: str,
    table: str,
    path_suffix: Optional[str] = None,
    path_ignore_suffix: Optional[str] = None,
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
    s3_additional_kwargs: Optional[Dict[str, str]] = None,
    boto3_session: Optional[boto3.Session] = None,
) -> Tuple[Dict[str, str], Optional[Dict[str, str]], Optional[Dict[str, List[str]]]]:
    """Infer and store parquet metadata on AWS Glue Catalog.

    Infer Apache Parquet file(s) metadata from from a received S3 prefix or list of S3 objects paths
    And then stores it on AWS Glue Catalog including all inferred partitions
    (No need of 'MCSK REPAIR TABLE')

    The concept of Dataset goes beyond the simple idea of files and enable more
    complex features like partitioning and catalog integration (AWS Glue Catalog).

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
        S3 prefix (e.g. s3://bucket/prefix) or list of S3 objects paths (e.g. [s3://bucket/key0, s3://bucket/key1]).
    path_suffix: Union[str, List[str], None]
        Suffix or List of suffixes for filtering S3 keys.
    path_ignore_suffix: Union[str, List[str], None]
        Suffix or List of suffixes for S3 keys to be ignored.
    database : str
        Glue/Athena catalog: Database name.
    table : str
        Glue/Athena catalog: Table name.
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
    s3_additional_kwargs:
        Forward to s3fs, useful for server side encryption
        https://s3fs.readthedocs.io/en/latest/#serverside-encryption
    boto3_session : boto3.Session(), optional
        Boto3 Session. The default boto3 session will be used if boto3_session receive None.

    Returns
    -------
    Tuple[Dict[str, str], Optional[Dict[str, str]], Optional[Dict[str, List[str]]]]
        The metadata used to create the Glue Table.
        columns_types: Dictionary with keys as column names and vales as
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
        catalog_versioning=catalog_versioning,
        projection_enabled=projection_enabled,
        projection_types=projection_types,
        projection_ranges=projection_ranges,
        projection_values=projection_values,
        projection_intervals=projection_intervals,
        projection_digits=projection_digits,
        boto3_session=session,
    )
    if (partitions_types is not None) and (partitions_values is not None) and (regular_partitions is True):
        catalog.add_parquet_partitions(
            database=database,
            table=table,
            partitions_values=partitions_values,
            compression=compression,
            boto3_session=session,
        )
    return columns_types, partitions_types, partitions_values
