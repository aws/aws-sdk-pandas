"""Amazon S3 Text Write Module (PRIVATE)."""

import csv
import logging
import uuid
from distutils.version import LooseVersion
from typing import Any, Dict, List, Optional, Tuple, Union

import boto3
import pandas as pd
from pandas.io.common import infer_compression

from awswrangler import _data_types, _utils, catalog, exceptions, lakeformation
from awswrangler._config import apply_configs
from awswrangler.s3._delete import delete_objects
from awswrangler.s3._fs import open_s3_object
from awswrangler.s3._write import _COMPRESSION_2_EXT, _apply_dtype, _sanitize, _validate_args
from awswrangler.s3._write_dataset import _to_dataset

_logger: logging.Logger = logging.getLogger(__name__)


def _get_write_details(path: str, pandas_kwargs: Dict[str, Any]) -> Tuple[str, Optional[str], Optional[str]]:
    if pandas_kwargs.get("compression", "infer") == "infer":
        pandas_kwargs["compression"] = infer_compression(path, compression="infer")
    mode: str = "w" if pandas_kwargs.get("compression") is None else "wb"
    encoding: Optional[str] = pandas_kwargs.get("encoding", "utf-8")
    newline: Optional[str] = pandas_kwargs.get("lineterminator", "")
    return mode, encoding, newline


def _to_text(
    file_format: str,
    df: pd.DataFrame,
    use_threads: Union[bool, int],
    boto3_session: Optional[boto3.Session],
    s3_additional_kwargs: Optional[Dict[str, str]],
    path: Optional[str] = None,
    path_root: Optional[str] = None,
    filename_prefix: Optional[str] = uuid.uuid4().hex,
    **pandas_kwargs: Any,
) -> List[str]:
    if df.empty is True:
        raise exceptions.EmptyDataFrame("DataFrame cannot be empty.")
    if path is None and path_root is not None:
        file_path: str = (
            f"{path_root}{filename_prefix}.{file_format}{_COMPRESSION_2_EXT.get(pandas_kwargs.get('compression'))}"
        )
    elif path is not None and path_root is None:
        file_path = path
    else:
        raise RuntimeError("path and path_root received at the same time.")

    mode, encoding, newline = _get_write_details(path=file_path, pandas_kwargs=pandas_kwargs)
    with open_s3_object(
        path=file_path,
        mode=mode,
        use_threads=use_threads,
        s3_additional_kwargs=s3_additional_kwargs,
        boto3_session=boto3_session,
        encoding=encoding,
        newline=newline,
    ) as f:
        _logger.debug("pandas_kwargs: %s", pandas_kwargs)
        if file_format == "csv":
            df.to_csv(f, mode=mode, **pandas_kwargs)
        elif file_format == "json":
            df.to_json(f, **pandas_kwargs)
    return [file_path]


@apply_configs
def to_csv(  # pylint: disable=too-many-arguments,too-many-locals,too-many-statements,too-many-branches
    df: pd.DataFrame,
    path: Optional[str] = None,
    sep: str = ",",
    index: bool = True,
    columns: Optional[List[str]] = None,
    use_threads: Union[bool, int] = True,
    boto3_session: Optional[boto3.Session] = None,
    s3_additional_kwargs: Optional[Dict[str, Any]] = None,
    sanitize_columns: bool = False,
    dataset: bool = False,
    filename_prefix: Optional[str] = None,
    partition_cols: Optional[List[str]] = None,
    bucketing_info: Optional[Tuple[List[str], int]] = None,
    concurrent_partitioning: bool = False,
    mode: Optional[str] = None,
    catalog_versioning: bool = False,
    schema_evolution: bool = False,
    database: Optional[str] = None,
    table: Optional[str] = None,
    table_type: Optional[str] = None,
    transaction_id: Optional[str] = None,
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
    projection_formats: Optional[Dict[str, str]] = None,
    projection_storage_location_template: Optional[str] = None,
    catalog_id: Optional[str] = None,
    **pandas_kwargs: Any,
) -> Dict[str, Union[List[str], Dict[str, List[str]]]]:
    """Write CSV file or dataset on Amazon S3.

    The concept of Dataset goes beyond the simple idea of ordinary files and enable more
    complex features like partitioning and catalog integration (Amazon Athena/AWS Glue Catalog).

    Note
    ----
    If database` and `table` arguments are passed, the table name and all column names
    will be automatically sanitized using `wr.catalog.sanitize_table_name` and `wr.catalog.sanitize_column_name`.
    Please, pass `sanitize_columns=True` to enforce this behaviour always.

    Note
    ----
    If `table` and `database` arguments are passed, `pandas_kwargs` will be ignored due
    restrictive quoting, date_format, escapechar and encoding required by Athena/Glue Catalog.

    Note
    ----
    Compression: The minimum acceptable version to achieve it is Pandas 1.2.0 that requires Python >= 3.7.1.

    Note
    ----
    In case of `use_threads=True` the number of threads
    that will be spawned will be gotten from os.cpu_count().

    Parameters
    ----------
    df: pandas.DataFrame
        Pandas DataFrame https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html
    path : str, optional
        Amazon S3 path (e.g. s3://bucket/prefix/filename.csv) (for dataset e.g. ``s3://bucket/prefix``).
        Required if dataset=False or when creating a new dataset
    sep : str
        String of length 1. Field delimiter for the output file.
    index : bool
        Write row names (index).
    columns : Optional[List[str]]
        Columns to write.
    use_threads : bool, int
        True to enable concurrent requests, False to disable multiple threads.
        If enabled os.cpu_count() will be used as the max number of threads.
        If integer is provided, specified number is used.
    boto3_session : boto3.Session(), optional
        Boto3 Session. The default boto3 Session will be used if boto3_session receive None.
    s3_additional_kwargs : Optional[Dict[str, Any]]
        Forwarded to botocore requests.
        e.g. s3_additional_kwargs={'ServerSideEncryption': 'aws:kms', 'SSEKMSKeyId': 'YOUR_KMS_KEY_ARN'}
    sanitize_columns : bool
        True to sanitize columns names or False to keep it as is.
        True value is forced if `dataset=True`.
    dataset : bool
        If True store as a dataset instead of ordinary file(s)
        If True, enable all follow arguments:
        partition_cols, mode, database, table, description, parameters, columns_comments, concurrent_partitioning,
        catalog_versioning, projection_enabled, projection_types, projection_ranges, projection_values,
        projection_intervals, projection_digits, catalog_id, schema_evolution.
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
        https://aws-sdk-pandas.readthedocs.io/en/2.18.0/tutorials/022%20-%20Writing%20Partitions%20Concurrently.html
    mode : str, optional
        ``append`` (Default), ``overwrite``, ``overwrite_partitions``. Only takes effect if dataset=True.
        For details check the related tutorial:
        https://aws-sdk-pandas.readthedocs.io/en/2.18.0/stubs/awswrangler.s3.to_parquet.html#awswrangler.s3.to_parquet
    catalog_versioning : bool
        If True and `mode="overwrite"`, creates an archived version of the table catalog before updating it.
    schema_evolution : bool
        If True allows schema evolution (new or missing columns), otherwise a exception will be raised.
        (Only considered if dataset=True and mode in ("append", "overwrite_partitions")). False by default.
        Related tutorial:
        https://aws-sdk-pandas.readthedocs.io/en/2.18.0/tutorials/014%20-%20Schema%20Evolution.html
    database : str, optional
        Glue/Athena catalog: Database name.
    table : str, optional
        Glue/Athena catalog: Table name.
    table_type: str, optional
        The type of the Glue Table. Set to EXTERNAL_TABLE if None
    transaction_id: str, optional
        The ID of the transaction when writing to a Governed Table.
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
    projection_formats: Optional[Dict[str, str]]
        Dictionary of partitions names and Athena projections formats.
        https://docs.aws.amazon.com/athena/latest/ug/partition-projection-supported-types.html
        (e.g. {'col_date': 'yyyy-MM-dd', 'col2_timestamp': 'yyyy-MM-dd HH:mm:ss'})
    projection_storage_location_template: Optional[str]
        Value which is allows Athena to properly map partition values if the S3 file locations do not follow
        a typical `.../column=value/...` pattern.
        https://docs.aws.amazon.com/athena/latest/ug/partition-projection-setting-up.html
        (e.g. s3://bucket/table_root/a=${a}/${b}/some_static_subdirectory/${c}/)
    catalog_id : str, optional
        The ID of the Data Catalog from which to retrieve Databases.
        If none is provided, the AWS account ID is used by default.
    pandas_kwargs :
        KEYWORD arguments forwarded to pandas.DataFrame.to_csv(). You can NOT pass `pandas_kwargs` explicit, just add
        valid Pandas arguments in the function call and awswrangler will accept it.
        e.g. wr.s3.to_csv(df, path, sep='|', na_rep='NULL', decimal=',')
        https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.to_csv.html

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
    >>> wr.s3.to_csv(
    ...     df=pd.DataFrame({'col': [1, 2, 3]}),
    ...     path='s3://bucket/prefix/my_file.csv',
    ... )
    {
        'paths': ['s3://bucket/prefix/my_file.csv'],
        'partitions_values': {}
    }

    Writing single file with pandas_kwargs

    >>> import awswrangler as wr
    >>> import pandas as pd
    >>> wr.s3.to_csv(
    ...     df=pd.DataFrame({'col': [1, 2, 3]}),
    ...     path='s3://bucket/prefix/my_file.csv',
    ...     sep='|',
    ...     na_rep='NULL',
    ...     decimal=','
    ... )
    {
        'paths': ['s3://bucket/prefix/my_file.csv'],
        'partitions_values': {}
    }

    Writing single file encrypted with a KMS key

    >>> import awswrangler as wr
    >>> import pandas as pd
    >>> wr.s3.to_csv(
    ...     df=pd.DataFrame({'col': [1, 2, 3]}),
    ...     path='s3://bucket/prefix/my_file.csv',
    ...     s3_additional_kwargs={
    ...         'ServerSideEncryption': 'aws:kms',
    ...         'SSEKMSKeyId': 'YOUR_KMS_KEY_ARN'
    ...     }
    ... )
    {
        'paths': ['s3://bucket/prefix/my_file.csv'],
        'partitions_values': {}
    }

    Writing partitioned dataset

    >>> import awswrangler as wr
    >>> import pandas as pd
    >>> wr.s3.to_csv(
    ...     df=pd.DataFrame({
    ...         'col': [1, 2, 3],
    ...         'col2': ['A', 'A', 'B']
    ...     }),
    ...     path='s3://bucket/prefix',
    ...     dataset=True,
    ...     partition_cols=['col2']
    ... )
    {
        'paths': ['s3://.../col2=A/x.csv', 's3://.../col2=B/y.csv'],
        'partitions_values: {
            's3://.../col2=A/': ['A'],
            's3://.../col2=B/': ['B']
        }
    }

    Writing bucketed dataset

    >>> import awswrangler as wr
    >>> import pandas as pd
    >>> wr.s3.to_csv(
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
    >>> wr.s3.to_csv(
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
        'paths': ['s3://.../col2=A/x.csv', 's3://.../col2=B/y.csv'],
        'partitions_values: {
            's3://.../col2=A/': ['A'],
            's3://.../col2=B/': ['B']
        }
    }

    Writing dataset to Glue governed table

    >>> import awswrangler as wr
    >>> import pandas as pd
    >>> wr.s3.to_csv(
    ...     df=pd.DataFrame({
    ...         'col': [1, 2, 3],
    ...         'col2': ['A', 'A', 'B'],
    ...         'col3': [None, None, None]
    ...     }),
    ...     dataset=True,
    ...     mode='append',
    ...     database='default',  # Athena/Glue database
    ...     table='my_table',  # Athena/Glue table
    ...     table_type='GOVERNED',
    ...     transaction_id="xxx",
    ... )
    {
        'paths': ['s3://.../x.csv'],
        'partitions_values: {}
    }

    Writing dataset casting empty column data type

    >>> import awswrangler as wr
    >>> import pandas as pd
    >>> wr.s3.to_csv(
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
        'paths': ['s3://.../x.csv'],
        'partitions_values: {}
    }

    """
    if "pandas_kwargs" in pandas_kwargs:
        raise exceptions.InvalidArgument(
            "You can NOT pass `pandas_kwargs` explicit, just add valid "
            "Pandas arguments in the function call and awswrangler will accept it."
            "e.g. wr.s3.to_csv(df, path, sep='|', na_rep='NULL', decimal=',', compression='gzip')"
        )
    if pandas_kwargs.get("compression") and str(pd.__version__) < LooseVersion("1.2.0"):
        raise exceptions.InvalidArgument(
            f"CSV compression on S3 is not supported for Pandas version {pd.__version__}. "
            "The minimum acceptable version to achive it is Pandas 1.2.0 that requires Python >=3.7.1."
        )
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

    # Initializing defaults
    partition_cols = partition_cols if partition_cols else []
    dtype = dtype if dtype else {}
    partitions_values: Dict[str, List[str]] = {}
    mode = "append" if mode is None else mode
    commit_trans: bool = False
    if transaction_id:
        table_type = "GOVERNED"
    filename_prefix = filename_prefix + uuid.uuid4().hex if filename_prefix else uuid.uuid4().hex
    session: boto3.Session = _utils.ensure_session(session=boto3_session)

    # Sanitize table to respect Athena's standards
    if (sanitize_columns is True) or (database is not None and table is not None):
        df, dtype, partition_cols = _sanitize(df=df, dtype=dtype, partition_cols=partition_cols)

    # Evaluating dtype
    catalog_table_input: Optional[Dict[str, Any]] = None
    if database and table:
        catalog_table_input = catalog._get_table_input(  # pylint: disable=protected-access
            database=database, table=table, boto3_session=session, transaction_id=transaction_id, catalog_id=catalog_id
        )

        catalog_path: Optional[str] = None
        if catalog_table_input:
            table_type = catalog_table_input["TableType"]
            catalog_path = catalog_table_input.get("StorageDescriptor", {}).get("Location")
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
        if pandas_kwargs.get("compression") not in ("gzip", "bz2", None):
            raise exceptions.InvalidArgumentCombination(
                "If database and table are given, you must use one of these compressions: gzip, bz2 or None."
            )
        if (table_type == "GOVERNED") and (not transaction_id):
            _logger.debug("`transaction_id` not specified for GOVERNED table, starting transaction")
            transaction_id = lakeformation.start_transaction(read_only=False, boto3_session=boto3_session)
            commit_trans = True

    df = _apply_dtype(df=df, dtype=dtype, catalog_table_input=catalog_table_input, mode=mode)

    paths: List[str] = []
    if dataset is False:
        pandas_kwargs["sep"] = sep
        pandas_kwargs["index"] = index
        pandas_kwargs["columns"] = columns
        _to_text(
            file_format="csv",
            df=df,
            use_threads=use_threads,
            path=path,
            boto3_session=session,
            s3_additional_kwargs=s3_additional_kwargs,
            **pandas_kwargs,
        )
        paths = [path]  # type: ignore
    else:
        compression: Optional[str] = pandas_kwargs.get("compression", None)
        if database and table:
            quoting: Optional[int] = csv.QUOTE_NONE
            escapechar: Optional[str] = "\\"
            header: Union[bool, List[str]] = pandas_kwargs.get("header", False)
            date_format: Optional[str] = "%Y-%m-%d %H:%M:%S.%f"
            pd_kwargs: Dict[str, Any] = {}
        else:
            quoting = pandas_kwargs.get("quoting", None)
            escapechar = pandas_kwargs.get("escapechar", None)
            header = pandas_kwargs.get("header", True)
            date_format = pandas_kwargs.get("date_format", None)
            pd_kwargs = pandas_kwargs.copy()
            pd_kwargs.pop("quoting", None)
            pd_kwargs.pop("escapechar", None)
            pd_kwargs.pop("header", None)
            pd_kwargs.pop("date_format", None)
            pd_kwargs.pop("compression", None)

        df = df[columns] if columns else df

        columns_types: Dict[str, str] = {}
        partitions_types: Dict[str, str] = {}

        if database and table:
            columns_types, partitions_types = _data_types.athena_types_from_pandas_partitioned(
                df=df, index=index, partition_cols=partition_cols, dtype=dtype, index_left=True
            )
            if schema_evolution is False:
                _utils.check_schema_changes(columns_types=columns_types, table_input=catalog_table_input, mode=mode)

            if (catalog_table_input is None) and (table_type == "GOVERNED"):
                catalog._create_csv_table(  # pylint: disable=protected-access
                    database=database,
                    table=table,
                    path=path,
                    columns_types=columns_types,
                    table_type=table_type,
                    partitions_types=partitions_types,
                    bucketing_info=bucketing_info,
                    description=description,
                    parameters=parameters,
                    columns_comments=columns_comments,
                    boto3_session=session,
                    mode=mode,
                    transaction_id=transaction_id,
                    schema_evolution=schema_evolution,
                    catalog_versioning=catalog_versioning,
                    sep=sep,
                    projection_enabled=projection_enabled,
                    projection_types=projection_types,
                    projection_ranges=projection_ranges,
                    projection_values=projection_values,
                    projection_intervals=projection_intervals,
                    projection_digits=projection_digits,
                    projection_formats=projection_formats,
                    projection_storage_location_template=projection_storage_location_template,
                    catalog_table_input=catalog_table_input,
                    catalog_id=catalog_id,
                    compression=pandas_kwargs.get("compression"),
                    skip_header_line_count=None,
                    serde_library=None,
                    serde_parameters=None,
                )
                catalog_table_input = catalog._get_table_input(  # pylint: disable=protected-access
                    database=database,
                    table=table,
                    boto3_session=session,
                    transaction_id=transaction_id,
                    catalog_id=catalog_id,
                )

        paths, partitions_values = _to_dataset(
            func=_to_text,
            concurrent_partitioning=concurrent_partitioning,
            df=df,
            path_root=path,  # type: ignore
            index=index,
            sep=sep,
            compression=compression,
            catalog_id=catalog_id,
            database=database,
            table=table,
            table_type=table_type,
            transaction_id=transaction_id,
            filename_prefix=filename_prefix,
            use_threads=use_threads,
            partition_cols=partition_cols,
            partitions_types=partitions_types,
            bucketing_info=bucketing_info,
            mode=mode,
            boto3_session=session,
            s3_additional_kwargs=s3_additional_kwargs,
            file_format="csv",
            quoting=quoting,
            escapechar=escapechar,
            header=header,
            date_format=date_format,
            **pd_kwargs,
        )
        if database and table:
            try:
                serde_info: Dict[str, Any] = {}
                if catalog_table_input:
                    serde_info = catalog_table_input["StorageDescriptor"]["SerdeInfo"]
                serde_library: Optional[str] = serde_info.get("SerializationLibrary", None)
                serde_parameters: Optional[Dict[str, str]] = serde_info.get("Parameters", None)
                catalog._create_csv_table(  # pylint: disable=protected-access
                    database=database,
                    table=table,
                    path=path,
                    columns_types=columns_types,
                    table_type=table_type,
                    partitions_types=partitions_types,
                    bucketing_info=bucketing_info,
                    description=description,
                    parameters=parameters,
                    columns_comments=columns_comments,
                    boto3_session=session,
                    mode=mode,
                    transaction_id=transaction_id,
                    catalog_versioning=catalog_versioning,
                    schema_evolution=schema_evolution,
                    sep=sep,
                    projection_enabled=projection_enabled,
                    projection_types=projection_types,
                    projection_ranges=projection_ranges,
                    projection_values=projection_values,
                    projection_intervals=projection_intervals,
                    projection_digits=projection_digits,
                    projection_formats=projection_formats,
                    projection_storage_location_template=projection_storage_location_template,
                    catalog_table_input=catalog_table_input,
                    catalog_id=catalog_id,
                    compression=pandas_kwargs.get("compression"),
                    skip_header_line_count=True if header else None,
                    serde_library=serde_library,
                    serde_parameters=serde_parameters,
                )
                if partitions_values and (regular_partitions is True) and (table_type != "GOVERNED"):
                    _logger.debug("partitions_values:\n%s", partitions_values)
                    catalog.add_csv_partitions(
                        database=database,
                        table=table,
                        partitions_values=partitions_values,
                        bucketing_info=bucketing_info,
                        boto3_session=session,
                        sep=sep,
                        serde_library=serde_library,
                        serde_parameters=serde_parameters,
                        catalog_id=catalog_id,
                        columns_types=columns_types,
                        compression=pandas_kwargs.get("compression"),
                    )
                if commit_trans:
                    lakeformation.commit_transaction(
                        transaction_id=transaction_id, boto3_session=boto3_session  # type: ignore
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


def to_json(  # pylint: disable=too-many-arguments,too-many-locals,too-many-statements,too-many-branches
    df: pd.DataFrame,
    path: Optional[str] = None,
    index: bool = True,
    columns: Optional[List[str]] = None,
    use_threads: Union[bool, int] = True,
    boto3_session: Optional[boto3.Session] = None,
    s3_additional_kwargs: Optional[Dict[str, Any]] = None,
    sanitize_columns: bool = False,
    dataset: bool = False,
    filename_prefix: Optional[str] = None,
    partition_cols: Optional[List[str]] = None,
    bucketing_info: Optional[Tuple[List[str], int]] = None,
    concurrent_partitioning: bool = False,
    mode: Optional[str] = None,
    catalog_versioning: bool = False,
    schema_evolution: bool = True,
    database: Optional[str] = None,
    table: Optional[str] = None,
    table_type: Optional[str] = None,
    transaction_id: Optional[str] = None,
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
    projection_formats: Optional[Dict[str, str]] = None,
    projection_storage_location_template: Optional[str] = None,
    catalog_id: Optional[str] = None,
    **pandas_kwargs: Any,
) -> Union[List[str], Dict[str, Union[List[str], Dict[str, List[str]]]]]:
    """Write JSON file on Amazon S3.

    Note
    ----
    In case of `use_threads=True` the number of threads
    that will be spawned will be gotten from os.cpu_count().

    Note
    ----
    Compression: The minimum acceptable version to achive it is Pandas 1.2.0 that requires Python >= 3.7.1.

    Parameters
    ----------
    df: pandas.DataFrame
        Pandas DataFrame https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html
    path : str
        Amazon S3 path (e.g. s3://bucket/filename.json).
    index : bool
        Write row names (index).
    columns : Optional[List[str]]
        Columns to write.
    use_threads : bool, int
        True to enable concurrent requests, False to disable multiple threads.
        If enabled os.cpu_count() will be used as the max number of threads.
        If integer is provided, specified number is used.
    boto3_session : boto3.Session(), optional
        Boto3 Session. The default boto3 Session will be used if boto3_session receive None.
    s3_additional_kwargs : Optional[Dict[str, Any]]
        Forwarded to botocore requests.
        e.g. s3_additional_kwargs={'ServerSideEncryption': 'aws:kms', 'SSEKMSKeyId': 'YOUR_KMS_KEY_ARN'}
    sanitize_columns : bool
        True to sanitize columns names or False to keep it as is.
        True value is forced if `dataset=True`.
    dataset : bool
        If True store as a dataset instead of ordinary file(s)
        If True, enable all follow arguments:
        partition_cols, mode, database, table, description, parameters, columns_comments, concurrent_partitioning,
        catalog_versioning, projection_enabled, projection_types, projection_ranges, projection_values,
        projection_intervals, projection_digits, catalog_id, schema_evolution.
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
        https://aws-sdk-pandas.readthedocs.io/en/2.18.0/tutorials/022%20-%20Writing%20Partitions%20Concurrently.html
    mode : str, optional
        ``append`` (Default), ``overwrite``, ``overwrite_partitions``. Only takes effect if dataset=True.
        For details check the related tutorial:
        https://aws-sdk-pandas.readthedocs.io/en/2.18.0/stubs/awswrangler.s3.to_parquet.html#awswrangler.s3.to_parquet
    catalog_versioning : bool
        If True and `mode="overwrite"`, creates an archived version of the table catalog before updating it.
    schema_evolution : bool
        If True allows schema evolution (new or missing columns), otherwise a exception will be raised.
        (Only considered if dataset=True and mode in ("append", "overwrite_partitions"))
        Related tutorial:
        https://aws-sdk-pandas.readthedocs.io/en/2.18.0/tutorials/014%20-%20Schema%20Evolution.html
    database : str, optional
        Glue/Athena catalog: Database name.
    table : str, optional
        Glue/Athena catalog: Table name.
    table_type: str, optional
        The type of the Glue Table. Set to EXTERNAL_TABLE if None
    transaction_id: str, optional
        The ID of the transaction when writing to a Governed Table.
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
    projection_formats: Optional[Dict[str, str]]
        Dictionary of partitions names and Athena projections formats.
        https://docs.aws.amazon.com/athena/latest/ug/partition-projection-supported-types.html
        (e.g. {'col_date': 'yyyy-MM-dd', 'col2_timestamp': 'yyyy-MM-dd HH:mm:ss'})
    projection_storage_location_template: Optional[str]
        Value which is allows Athena to properly map partition values if the S3 file locations do not follow
        a typical `.../column=value/...` pattern.
        https://docs.aws.amazon.com/athena/latest/ug/partition-projection-setting-up.html
        (e.g. s3://bucket/table_root/a=${a}/${b}/some_static_subdirectory/${c}/)
    catalog_id : str, optional
        The ID of the Data Catalog from which to retrieve Databases.
        If none is provided, the AWS account ID is used by default.
    pandas_kwargs:
        KEYWORD arguments forwarded to pandas.DataFrame.to_json(). You can NOT pass `pandas_kwargs` explicit, just add
        valid Pandas arguments in the function call and awswrangler will accept it.
        e.g. wr.s3.to_json(df, path, lines=True, date_format='iso')
        https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.to_json.html

    Returns
    -------
    List[str]
        List of written files.

    Examples
    --------
    Writing JSON file

    >>> import awswrangler as wr
    >>> import pandas as pd
    >>> wr.s3.to_json(
    ...     df=pd.DataFrame({'col': [1, 2, 3]}),
    ...     path='s3://bucket/filename.json',
    ... )

    Writing JSON file using pandas_kwargs

    >>> import awswrangler as wr
    >>> import pandas as pd
    >>> wr.s3.to_json(
    ...     df=pd.DataFrame({'col': [1, 2, 3]}),
    ...     path='s3://bucket/filename.json',
    ...     lines=True,
    ...     date_format='iso'
    ... )

    Writing CSV file encrypted with a KMS key

    >>> import awswrangler as wr
    >>> import pandas as pd
    >>> wr.s3.to_json(
    ...     df=pd.DataFrame({'col': [1, 2, 3]}),
    ...     path='s3://bucket/filename.json',
    ...     s3_additional_kwargs={
    ...         'ServerSideEncryption': 'aws:kms',
    ...         'SSEKMSKeyId': 'YOUR_KMS_KEY_ARN'
    ...     }
    ... )

    """
    if "pandas_kwargs" in pandas_kwargs:
        raise exceptions.InvalidArgument(
            "You can NOT pass `pandas_kwargs` explicit, just add valid "
            "Pandas arguments in the function call and awswrangler will accept it."
            "e.g. wr.s3.to_json(df, path, lines=True, date_format='iso')"
        )
    if pandas_kwargs.get("compression") and str(pd.__version__) < LooseVersion("1.2.0"):
        raise exceptions.InvalidArgument(
            f"JSON compression on S3 is not supported for Pandas version {pd.__version__}. "
            "The minimum acceptable version to achive it is Pandas 1.2.0 that requires Python >=3.7.1."
        )

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

    # Initializing defaults
    partition_cols = partition_cols if partition_cols else []
    dtype = dtype if dtype else {}
    partitions_values: Dict[str, List[str]] = {}
    mode = "append" if mode is None else mode
    commit_trans: bool = False
    if transaction_id:
        table_type = "GOVERNED"
    filename_prefix = filename_prefix + uuid.uuid4().hex if filename_prefix else uuid.uuid4().hex
    session: boto3.Session = _utils.ensure_session(session=boto3_session)

    # Sanitize table to respect Athena's standards
    if (sanitize_columns is True) or (database is not None and table is not None):
        df, dtype, partition_cols = _sanitize(df=df, dtype=dtype, partition_cols=partition_cols)

    # Evaluating dtype
    catalog_table_input: Optional[Dict[str, Any]] = None

    if database and table:
        catalog_table_input = catalog._get_table_input(  # pylint: disable=protected-access
            database=database, table=table, boto3_session=session, transaction_id=transaction_id, catalog_id=catalog_id
        )
        catalog_path: Optional[str] = None
        if catalog_table_input:
            table_type = catalog_table_input["TableType"]
            catalog_path = catalog_table_input.get("StorageDescriptor", {}).get("Location")
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
        if pandas_kwargs.get("compression") not in ("gzip", "bz2", None):
            raise exceptions.InvalidArgumentCombination(
                "If database and table are given, you must use one of these compressions: gzip, bz2 or None."
            )
        if (table_type == "GOVERNED") and (not transaction_id):
            _logger.debug("`transaction_id` not specified for GOVERNED table, starting transaction")
            transaction_id = lakeformation.start_transaction(read_only=False, boto3_session=boto3_session)
            commit_trans = True

    df = _apply_dtype(df=df, dtype=dtype, catalog_table_input=catalog_table_input, mode=mode)

    if dataset is False:
        return _to_text(
            file_format="json",
            df=df,
            path=path,
            use_threads=use_threads,
            boto3_session=session,
            s3_additional_kwargs=s3_additional_kwargs,
            **pandas_kwargs,
        )

    compression: Optional[str] = pandas_kwargs.pop("compression", None)
    df = df[columns] if columns else df

    columns_types: Dict[str, str] = {}
    partitions_types: Dict[str, str] = {}

    if database and table:
        columns_types, partitions_types = _data_types.athena_types_from_pandas_partitioned(
            df=df, index=index, partition_cols=partition_cols, dtype=dtype
        )
        if schema_evolution is False:
            _utils.check_schema_changes(columns_types=columns_types, table_input=catalog_table_input, mode=mode)

        if (catalog_table_input is None) and (table_type == "GOVERNED"):
            catalog._create_json_table(  # pylint: disable=protected-access
                database=database,
                table=table,
                path=path,  # type: ignore
                columns_types=columns_types,
                table_type=table_type,
                partitions_types=partitions_types,
                bucketing_info=bucketing_info,
                description=description,
                parameters=parameters,
                columns_comments=columns_comments,
                boto3_session=session,
                mode=mode,
                transaction_id=transaction_id,
                catalog_versioning=catalog_versioning,
                schema_evolution=schema_evolution,
                projection_enabled=projection_enabled,
                projection_types=projection_types,
                projection_ranges=projection_ranges,
                projection_values=projection_values,
                projection_intervals=projection_intervals,
                projection_digits=projection_digits,
                projection_formats=projection_formats,
                projection_storage_location_template=projection_storage_location_template,
                catalog_table_input=catalog_table_input,
                catalog_id=catalog_id,
                compression=compression,
                serde_library=None,
                serde_parameters=None,
            )
            catalog_table_input = catalog._get_table_input(  # pylint: disable=protected-access
                database=database,
                table=table,
                boto3_session=session,
                transaction_id=transaction_id,
                catalog_id=catalog_id,
            )

    paths, partitions_values = _to_dataset(
        func=_to_text,
        concurrent_partitioning=concurrent_partitioning,
        df=df,
        path_root=path,  # type: ignore
        filename_prefix=filename_prefix,
        index=index,
        compression=compression,
        catalog_id=catalog_id,
        database=database,
        table=table,
        table_type=table_type,
        transaction_id=transaction_id,
        use_threads=use_threads,
        partition_cols=partition_cols,
        partitions_types=partitions_types,
        bucketing_info=bucketing_info,
        mode=mode,
        boto3_session=session,
        s3_additional_kwargs=s3_additional_kwargs,
        file_format="json",
        **pandas_kwargs,
    )
    if database and table:
        try:
            serde_info: Dict[str, Any] = {}
            if catalog_table_input:
                serde_info = catalog_table_input["StorageDescriptor"]["SerdeInfo"]
            serde_library: Optional[str] = serde_info.get("SerializationLibrary", None)
            serde_parameters: Optional[Dict[str, str]] = serde_info.get("Parameters", None)
            catalog._create_json_table(  # pylint: disable=protected-access
                database=database,
                table=table,
                path=path,  # type: ignore
                columns_types=columns_types,
                table_type=table_type,
                partitions_types=partitions_types,
                bucketing_info=bucketing_info,
                description=description,
                parameters=parameters,
                columns_comments=columns_comments,
                boto3_session=session,
                mode=mode,
                transaction_id=transaction_id,
                catalog_versioning=catalog_versioning,
                schema_evolution=schema_evolution,
                projection_enabled=projection_enabled,
                projection_types=projection_types,
                projection_ranges=projection_ranges,
                projection_values=projection_values,
                projection_intervals=projection_intervals,
                projection_digits=projection_digits,
                projection_formats=projection_formats,
                projection_storage_location_template=projection_storage_location_template,
                catalog_table_input=catalog_table_input,
                catalog_id=catalog_id,
                compression=compression,
                serde_library=serde_library,
                serde_parameters=serde_parameters,
            )
            if partitions_values and (regular_partitions is True) and (table_type != "GOVERNED"):
                _logger.debug("partitions_values:\n%s", partitions_values)
                catalog.add_json_partitions(
                    database=database,
                    table=table,
                    partitions_values=partitions_values,
                    bucketing_info=bucketing_info,
                    boto3_session=session,
                    serde_library=serde_library,
                    serde_parameters=serde_parameters,
                    catalog_id=catalog_id,
                    columns_types=columns_types,
                    compression=compression,
                )
                if commit_trans:
                    lakeformation.commit_transaction(
                        transaction_id=transaction_id, boto3_session=boto3_session  # type: ignore
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
