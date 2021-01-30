"""Amazon S3 Text Write Module (PRIVATE)."""

import csv
import logging
import uuid
from distutils.version import LooseVersion
from typing import Any, Dict, List, Optional, Tuple, Union

import boto3
import pandas as pd
from pandas.io.common import infer_compression

from awswrangler import _data_types, _utils, catalog, exceptions
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
    encoding: Optional[str] = pandas_kwargs.get("encoding", None)
    newline: Optional[str] = pandas_kwargs.get("lineterminator", "")
    return mode, encoding, newline


def _to_text(
    file_format: str,
    df: pd.DataFrame,
    use_threads: bool,
    boto3_session: Optional[boto3.Session],
    s3_additional_kwargs: Optional[Dict[str, str]],
    path: Optional[str] = None,
    path_root: Optional[str] = None,
    filename: Optional[str] = None,
    **pandas_kwargs: Any,
) -> List[str]:
    if df.empty is True:
        raise exceptions.EmptyDataFrame()
    if path is None and path_root is not None:
        if filename is None:
            filename = uuid.uuid4().hex
        file_path: str = (
            f"{path_root}{filename}.{file_format}{_COMPRESSION_2_EXT.get(pandas_kwargs.get('compression'))}"
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
def to_csv(  # pylint: disable=too-many-arguments,too-many-locals,too-many-statements
    df: pd.DataFrame,
    path: str,
    sep: str = ",",
    index: bool = True,
    columns: Optional[List[str]] = None,
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
    Compression: The minimum acceptable version to achive it is Pandas 1.2.0 that requires Python >= 3.7.1.

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
        Amazon S3 path (e.g. s3://bucket/filename.csv).
    sep : str
        String of length 1. Field delimiter for the output file.
    index : bool
        Write row names (index).
    columns : Optional[List[str]]
        Columns to write.
    use_threads : bool
        True to enable concurrent requests, False to disable multiple threads.
        If enabled os.cpu_count() will be used as the max number of threads.
    boto3_session : boto3.Session(), optional
        Boto3 Session. The default boto3 Session will be used if boto3_session receive None.
    s3_additional_kwargs : Optional[Dict[str, Any]]
        Forward to botocore requests. Valid parameters: "ACL", "Metadata", "ServerSideEncryption", "StorageClass",
        "SSECustomerAlgorithm", "SSECustomerKey", "SSEKMSKeyId", "SSEKMSEncryptionContext", "Tagging", "RequestPayer".
        e.g. s3_additional_kwargs={'ServerSideEncryption': 'aws:kms', 'SSEKMSKeyId': 'YOUR_KMS_KEY_ARN'}
    sanitize_columns : bool
        True to sanitize columns names or False to keep it as is.
        True value is forced if `dataset=True`.
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
        https://github.com/awslabs/aws-data-wrangler/blob/main/tutorials/022%20-%20Writing%20Partitions%20Concurrently.ipynb
    mode : str, optional
        ``append`` (Default), ``overwrite``, ``overwrite_partitions``. Only takes effect if dataset=True.
        For details check the related tutorial:
        https://aws-data-wrangler.readthedocs.io/en/stable/stubs/awswrangler.s3.to_parquet.html#awswrangler.s3.to_parquet
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
    catalog_id : str, optional
        The ID of the Data Catalog from which to retrieve Databases.
        If none is provided, the AWS account ID is used by default.
    pandas_kwargs :
        KEYWORD arguments forwarded to pandas.DataFrame.to_csv(). You can NOT pass `pandas_kwargs` explicit, just add
        valid Pandas arguments in the function call and Wrangler will accept it.
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
            "Pandas arguments in the function call and Wrangler will accept it."
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
    session: boto3.Session = _utils.ensure_session(session=boto3_session)

    # Sanitize table to respect Athena's standards
    if (sanitize_columns is True) or (database is not None and table is not None):
        df, dtype, partition_cols = _sanitize(df=df, dtype=dtype, partition_cols=partition_cols)

    # Evaluating dtype
    catalog_table_input: Optional[Dict[str, Any]] = None
    if database and table:
        catalog_table_input = catalog._get_table_input(  # pylint: disable=protected-access
            database=database, table=table, boto3_session=session, catalog_id=catalog_id
        )
        if pandas_kwargs.get("compression") not in ("gzip", "bz2", None):
            raise exceptions.InvalidArgumentCombination(
                "If database and table are given, you must use one of these compressions: gzip, bz2 or None."
            )

    df = _apply_dtype(df=df, dtype=dtype, catalog_table_input=catalog_table_input, mode=mode)

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
        paths = [path]
    else:
        if database and table:
            quoting: Optional[int] = csv.QUOTE_NONE
            escapechar: Optional[str] = "\\"
            header: Union[bool, List[str]] = False
            date_format: Optional[str] = "%Y-%m-%d %H:%M:%S.%f"
            pd_kwargs: Dict[str, Any] = {}
            compression: Optional[str] = pandas_kwargs.get("compression", None)
        else:
            quoting = pandas_kwargs.get("quoting", None)
            escapechar = pandas_kwargs.get("escapechar", None)
            header = pandas_kwargs.get("header", True)
            date_format = pandas_kwargs.get("date_format", None)
            compression = pandas_kwargs.get("compression", None)
            pd_kwargs = pandas_kwargs.copy()
            pd_kwargs.pop("quoting", None)
            pd_kwargs.pop("escapechar", None)
            pd_kwargs.pop("header", None)
            pd_kwargs.pop("date_format", None)
            pd_kwargs.pop("compression", None)

        df = df[columns] if columns else df
        paths, partitions_values = _to_dataset(
            func=_to_text,
            concurrent_partitioning=concurrent_partitioning,
            df=df,
            path_root=path,
            index=index,
            sep=sep,
            compression=compression,
            use_threads=use_threads,
            partition_cols=partition_cols,
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
                columns_types, partitions_types = _data_types.athena_types_from_pandas_partitioned(
                    df=df, index=index, partition_cols=partition_cols, dtype=dtype, index_left=True
                )
                catalog._create_csv_table(  # pylint: disable=protected-access
                    database=database,
                    table=table,
                    path=path,
                    columns_types=columns_types,
                    partitions_types=partitions_types,
                    bucketing_info=bucketing_info,
                    description=description,
                    parameters=parameters,
                    columns_comments=columns_comments,
                    boto3_session=session,
                    mode=mode,
                    catalog_versioning=catalog_versioning,
                    sep=sep,
                    projection_enabled=projection_enabled,
                    projection_types=projection_types,
                    projection_ranges=projection_ranges,
                    projection_values=projection_values,
                    projection_intervals=projection_intervals,
                    projection_digits=projection_digits,
                    catalog_table_input=catalog_table_input,
                    catalog_id=catalog_id,
                    compression=pandas_kwargs.get("compression"),
                    skip_header_line_count=None,
                )
                if partitions_values and (regular_partitions is True):
                    _logger.debug("partitions_values:\n%s", partitions_values)
                    catalog.add_csv_partitions(
                        database=database,
                        table=table,
                        partitions_values=partitions_values,
                        bucketing_info=bucketing_info,
                        boto3_session=session,
                        sep=sep,
                        catalog_id=catalog_id,
                        columns_types=columns_types,
                        compression=pandas_kwargs.get("compression"),
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


def to_json(
    df: pd.DataFrame,
    path: str,
    boto3_session: Optional[boto3.Session] = None,
    s3_additional_kwargs: Optional[Dict[str, Any]] = None,
    use_threads: bool = True,
    **pandas_kwargs: Any,
) -> List[str]:
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
        Amazon S3 path (e.g. s3://bucket/filename.csv).
    boto3_session : boto3.Session(), optional
        Boto3 Session. The default boto3 Session will be used if boto3_session receive None.
    s3_additional_kwargs : Optional[Dict[str, Any]]
        Forward to botocore requests. Valid parameters: "ACL", "Metadata", "ServerSideEncryption", "StorageClass",
        "SSECustomerAlgorithm", "SSECustomerKey", "SSEKMSKeyId", "SSEKMSEncryptionContext", "Tagging", "RequestPayer".
        e.g. s3_additional_kwargs={'ServerSideEncryption': 'aws:kms', 'SSEKMSKeyId': 'YOUR_KMS_KEY_ARN'}
    use_threads : bool
        True to enable concurrent requests, False to disable multiple threads.
        If enabled os.cpu_count() will be used as the max number of threads.
    pandas_kwargs:
        KEYWORD arguments forwarded to pandas.DataFrame.to_json(). You can NOT pass `pandas_kwargs` explicit, just add
        valid Pandas arguments in the function call and Wrangler will accept it.
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
            "Pandas arguments in the function call and Wrangler will accept it."
            "e.g. wr.s3.to_json(df, path, lines=True, date_format='iso')"
        )
    if pandas_kwargs.get("compression") and str(pd.__version__) < LooseVersion("1.2.0"):
        raise exceptions.InvalidArgument(
            f"JSON compression on S3 is not supported for Pandas version {pd.__version__}. "
            "The minimum acceptable version to achive it is Pandas 1.2.0 that requires Python >=3.7.1."
        )
    return _to_text(
        file_format="json",
        df=df,
        path=path,
        use_threads=use_threads,
        boto3_session=boto3_session,
        s3_additional_kwargs=s3_additional_kwargs,
        **pandas_kwargs,
    )
