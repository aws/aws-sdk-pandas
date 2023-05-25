"""Amazon Redshift Read Module (PRIVATE)."""
import logging
from typing import Any, Dict, Iterator, List, Literal, Optional, Tuple, Union

import boto3
import pyarrow as pa

import awswrangler.pandas as pd
from awswrangler import _databases as _db_utils
from awswrangler import _utils, exceptions, s3
from awswrangler._config import apply_configs
from awswrangler._distributed import EngineEnum, engine

from ._connect import _validate_connection
from ._utils import _make_s3_auth_string

redshift_connector = _utils.import_optional_dependency("redshift_connector")

_logger: logging.Logger = logging.getLogger(__name__)


def _read_parquet_iterator(
    path: str,
    keep_files: bool,
    use_threads: Union[bool, int],
    chunked: Union[bool, int],
    dtype_backend: Literal["numpy_nullable", "pyarrow"],
    boto3_session: Optional[boto3.Session],
    s3_additional_kwargs: Optional[Dict[str, str]],
    pyarrow_additional_kwargs: Optional[Dict[str, Any]],
) -> Iterator[pd.DataFrame]:
    dfs: Iterator[pd.DataFrame] = s3.read_parquet(
        path=path,
        chunked=chunked,
        dataset=False,
        use_threads=use_threads,
        dtype_backend=dtype_backend,
        boto3_session=boto3_session,
        s3_additional_kwargs=s3_additional_kwargs,
        pyarrow_additional_kwargs=pyarrow_additional_kwargs,
    )
    yield from dfs
    if keep_files is False:
        s3.delete_objects(
            path=path, use_threads=use_threads, boto3_session=boto3_session, s3_additional_kwargs=s3_additional_kwargs
        )


@apply_configs
@_utils.check_optional_dependency(redshift_connector, "redshift_connector")
def read_sql_query(
    sql: str,
    con: "redshift_connector.Connection",
    index_col: Optional[Union[str, List[str]]] = None,
    params: Optional[Union[List[Any], Tuple[Any, ...], Dict[Any, Any]]] = None,
    dtype_backend: Literal["numpy_nullable", "pyarrow"] = "numpy_nullable",
    chunksize: Optional[int] = None,
    dtype: Optional[Dict[str, pa.DataType]] = None,
    safe: bool = True,
    timestamp_as_object: bool = False,
) -> Union[pd.DataFrame, Iterator[pd.DataFrame]]:
    """Return a DataFrame corresponding to the result set of the query string.

    Note
    ----
    For large extractions (1K+ rows) consider the function **wr.redshift.unload()**.

    Parameters
    ----------
    sql : str
        SQL query.
    con : redshift_connector.Connection
        Use redshift_connector.connect() to use "
        "credentials directly or wr.redshift.connect() to fetch it from the Glue Catalog.
    index_col : Union[str, List[str]], optional
        Column(s) to set as index(MultiIndex).
    params :  Union[List, Tuple, Dict], optional
        List of parameters to pass to execute method.
        The syntax used to pass parameters is database driver dependent.
        Check your database driver documentation for which of the five syntax styles,
        described in PEP 249’s paramstyle, is supported.
    dtype_backend: str, optional
        Which dtype_backend to use, e.g. whether a DataFrame should have NumPy arrays,
        nullable dtypes are used for all dtypes that have a nullable implementation when
        “numpy_nullable” is set, pyarrow is used for all dtypes if “pyarrow” is set.

        The dtype_backends are still experimential. The "pyarrow" backend is only supported with Pandas 2.0 or above.
    chunksize : int, optional
        If specified, return an iterator where chunksize is the number of rows to include in each chunk.
    dtype : Dict[str, pyarrow.DataType], optional
        Specifying the datatype for columns.
        The keys should be the column names and the values should be the PyArrow types.
    safe : bool
        Check for overflows or other unsafe data type conversions.
    timestamp_as_object : bool
        Cast non-nanosecond timestamps (np.datetime64) to objects.

    Returns
    -------
    Union[pandas.DataFrame, Iterator[pandas.DataFrame]]
        Result as Pandas DataFrame(s).

    Examples
    --------
    Reading from Redshift using a Glue Catalog Connections

    >>> import awswrangler as wr
    >>> con = wr.redshift.connect("MY_GLUE_CONNECTION")
    >>> df = wr.redshift.read_sql_query(
    ...     sql="SELECT * FROM public.my_table",
    ...     con=con
    ... )
    >>> con.close()

    """
    _validate_connection(con=con)
    return _db_utils.read_sql_query(
        sql=sql,
        con=con,
        index_col=index_col,
        params=params,
        chunksize=chunksize,
        dtype=dtype,
        safe=safe,
        timestamp_as_object=timestamp_as_object,
        dtype_backend=dtype_backend,
    )


@apply_configs
@_utils.check_optional_dependency(redshift_connector, "redshift_connector")
def read_sql_table(
    table: str,
    con: "redshift_connector.Connection",
    schema: Optional[str] = None,
    index_col: Optional[Union[str, List[str]]] = None,
    params: Optional[Union[List[Any], Tuple[Any, ...], Dict[Any, Any]]] = None,
    dtype_backend: Literal["numpy_nullable", "pyarrow"] = "numpy_nullable",
    chunksize: Optional[int] = None,
    dtype: Optional[Dict[str, pa.DataType]] = None,
    safe: bool = True,
    timestamp_as_object: bool = False,
) -> Union[pd.DataFrame, Iterator[pd.DataFrame]]:
    """Return a DataFrame corresponding the table.

    Note
    ----
    For large extractions (1K+ rows) consider the function **wr.redshift.unload()**.

    Parameters
    ----------
    table : str
        Table name.
    con : redshift_connector.Connection
        Use redshift_connector.connect() to use "
        "credentials directly or wr.redshift.connect() to fetch it from the Glue Catalog.
    schema : str, optional
        Name of SQL schema in database to query (if database flavor supports this).
        Uses default schema if None (default).
    index_col : Union[str, List[str]], optional
        Column(s) to set as index(MultiIndex).
    params :  Union[List, Tuple, Dict], optional
        List of parameters to pass to execute method.
        The syntax used to pass parameters is database driver dependent.
        Check your database driver documentation for which of the five syntax styles,
        described in PEP 249's paramstyle, is supported.
    dtype_backend: str, optional
        Which dtype_backend to use, e.g. whether a DataFrame should have NumPy arrays,
        nullable dtypes are used for all dtypes that have a nullable implementation when
        “numpy_nullable” is set, pyarrow is used for all dtypes if “pyarrow” is set.

        The dtype_backends are still experimential. The "pyarrow" backend is only supported with Pandas 2.0 or above.
    chunksize : int, optional
        If specified, return an iterator where chunksize is the number of rows to include in each chunk.
    dtype : Dict[str, pyarrow.DataType], optional
        Specifying the datatype for columns.
        The keys should be the column names and the values should be the PyArrow types.
    safe : bool
        Check for overflows or other unsafe data type conversions.
    timestamp_as_object : bool
        Cast non-nanosecond timestamps (np.datetime64) to objects.

    Returns
    -------
    Union[pandas.DataFrame, Iterator[pandas.DataFrame]]
        Result as Pandas DataFrame(s).

    Examples
    --------
    Reading from Redshift using a Glue Catalog Connections

    >>> import awswrangler as wr
    >>> con = wr.redshift.connect("MY_GLUE_CONNECTION")
    >>> df = wr.redshift.read_sql_table(
    ...     table="my_table",
    ...     schema="public",
    ...     con=con
    ... )
    >>> con.close()

    """
    sql: str = f'SELECT * FROM "{table}"' if schema is None else f'SELECT * FROM "{schema}"."{table}"'
    return read_sql_query(
        sql=sql,
        con=con,
        index_col=index_col,
        params=params,
        chunksize=chunksize,
        dtype=dtype,
        safe=safe,
        timestamp_as_object=timestamp_as_object,
        dtype_backend=dtype_backend,
    )


@_utils.check_optional_dependency(redshift_connector, "redshift_connector")
def unload_to_files(
    sql: str,
    path: str,
    con: "redshift_connector.Connection",
    iam_role: Optional[str] = None,
    aws_access_key_id: Optional[str] = None,
    aws_secret_access_key: Optional[str] = None,
    aws_session_token: Optional[str] = None,
    region: Optional[str] = None,
    unload_format: Optional[Literal["CSV", "PARQUET"]] = None,
    max_file_size: Optional[float] = None,
    kms_key_id: Optional[str] = None,
    manifest: bool = False,
    partition_cols: Optional[List[str]] = None,
    boto3_session: Optional[boto3.Session] = None,
) -> None:
    """Unload Parquet files on s3 from a Redshift query result (Through the UNLOAD command).

    https://docs.aws.amazon.com/redshift/latest/dg/r_UNLOAD.html

    Note
    ----
    In case of `use_threads=True` the number of threads
    that will be spawned will be gotten from os.cpu_count().

    Parameters
    ----------
    sql: str
        SQL query.
    path : Union[str, List[str]]
        S3 path to write stage files (e.g. s3://bucket_name/any_name/)
    con : redshift_connector.Connection
        Use redshift_connector.connect() to use "
        "credentials directly or wr.redshift.connect() to fetch it from the Glue Catalog.
    iam_role : str, optional
        AWS IAM role with the related permissions.
    aws_access_key_id : str, optional
        The access key for your AWS account.
    aws_secret_access_key : str, optional
        The secret key for your AWS account.
    aws_session_token : str, optional
        The session key for your AWS account. This is only needed when you are using temporary credentials.
    region : str, optional
        Specifies the AWS Region where the target Amazon S3 bucket is located.
        REGION is required for UNLOAD to an Amazon S3 bucket that isn't in the
        same AWS Region as the Amazon Redshift cluster. By default, UNLOAD
        assumes that the target Amazon S3 bucket is located in the same AWS
        Region as the Amazon Redshift cluster.
    unload_format: str, optional
        Format of the unloaded S3 objects from the query.
        Valid values: "CSV", "PARQUET". Case sensitive. Defaults to PARQUET.
    max_file_size : float, optional
        Specifies the maximum size (MB) of files that UNLOAD creates in Amazon S3.
        Specify a decimal value between 5.0 MB and 6200.0 MB. If None, the default
        maximum file size is 6200.0 MB.
    kms_key_id : str, optional
        Specifies the key ID for an AWS Key Management Service (AWS KMS) key to be
        used to encrypt data files on Amazon S3.
    manifest : bool
        Unload a manifest file on S3.
    partition_cols: List[str], optional
        Specifies the partition keys for the unload operation.
    boto3_session : boto3.Session(), optional
        Boto3 Session. The default boto3 session will be used if boto3_session receive None.

    Returns
    -------
    None

    Examples
    --------
    >>> import awswrangler as wr
    >>> con = wr.redshift.connect("MY_GLUE_CONNECTION")
    >>> wr.redshift.unload_to_files(
    ...     sql="SELECT * FROM public.mytable",
    ...     path="s3://bucket/extracted_parquet_files/",
    ...     con=con,
    ...     iam_role="arn:aws:iam::XXX:role/XXX"
    ... )
    >>> con.close()


    """
    _logger.debug("Unloading to S3 path: %s", path)
    if unload_format not in [None, "CSV", "PARQUET"]:
        raise exceptions.InvalidArgumentValue("<unload_format> argument must be 'CSV' or 'PARQUET'")
    with con.cursor() as cursor:
        format_str: str = unload_format or "PARQUET"
        partition_str: str = f"\nPARTITION BY ({','.join(partition_cols)})" if partition_cols else ""
        manifest_str: str = "\nmanifest" if manifest is True else ""
        region_str: str = f"\nREGION AS '{region}'" if region is not None else ""
        if not max_file_size and engine.get() == EngineEnum.RAY:
            _logger.warning(
                "Unload `MAXFILESIZE` is not specified. "
                "Defaulting to `512.0 MB` corresponding to the recommended Ray target block size."
            )
            max_file_size = 512.0
        max_file_size_str: str = f"\nMAXFILESIZE AS {max_file_size} MB" if max_file_size is not None else ""
        kms_key_id_str: str = f"\nKMS_KEY_ID '{kms_key_id}'" if kms_key_id is not None else ""

        auth_str: str = _make_s3_auth_string(
            iam_role=iam_role,
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            aws_session_token=aws_session_token,
            boto3_session=boto3_session,
        )

        # Escape quotation marks in SQL
        sql = sql.replace("'", "''")

        unload_sql = (
            f"UNLOAD ('{sql}')\n"
            f"TO '{path}'\n"
            f"{auth_str}"
            "ALLOWOVERWRITE\n"
            "PARALLEL ON\n"
            f"FORMAT {format_str}\n"
            "ENCRYPTED"
            f"{kms_key_id_str}"
            f"{partition_str}"
            f"{region_str}"
            f"{max_file_size_str}"
            f"{manifest_str};"
        )
        _logger.debug("Executing unload query:\n%s", unload_sql)
        cursor.execute(unload_sql)


@_utils.validate_distributed_kwargs(
    unsupported_kwargs=["boto3_session", "s3_additional_kwargs"],
)
@apply_configs
@_utils.check_optional_dependency(redshift_connector, "redshift_connector")
def unload(
    sql: str,
    path: str,
    con: "redshift_connector.Connection",
    iam_role: Optional[str] = None,
    aws_access_key_id: Optional[str] = None,
    aws_secret_access_key: Optional[str] = None,
    aws_session_token: Optional[str] = None,
    region: Optional[str] = None,
    max_file_size: Optional[float] = None,
    kms_key_id: Optional[str] = None,
    dtype_backend: Literal["numpy_nullable", "pyarrow"] = "numpy_nullable",
    chunked: Union[bool, int] = False,
    keep_files: bool = False,
    use_threads: Union[bool, int] = True,
    boto3_session: Optional[boto3.Session] = None,
    s3_additional_kwargs: Optional[Dict[str, str]] = None,
    pyarrow_additional_kwargs: Optional[Dict[str, Any]] = None,
) -> Union[pd.DataFrame, Iterator[pd.DataFrame]]:
    """Load Pandas DataFrame from a Amazon Redshift query result using Parquet files on s3 as stage.

    This is a **HIGH** latency and **HIGH** throughput alternative to
    `wr.redshift.read_sql_query()`/`wr.redshift.read_sql_table()` to extract large
    Amazon Redshift data into a Pandas DataFrames through the **UNLOAD command**.

    This strategy has more overhead and requires more IAM privileges
    than the regular `wr.redshift.read_sql_query()`/`wr.redshift.read_sql_table()` function,
    so it is only recommended to fetch 1k+ rows at once.

    https://docs.aws.amazon.com/redshift/latest/dg/r_UNLOAD.html

    Note
    ----
    ``Batching`` (`chunked` argument) (Memory Friendly):

    Will enable the function to return an Iterable of DataFrames instead of a regular DataFrame.

    There are two batching strategies on awswrangler:

    - If **chunked=True**, depending on the size of the data, one or more data frames are returned per file.
      Unlike **chunked=INTEGER**, rows from different files are not be mixed in the resulting data frames.

    - If **chunked=INTEGER**, awswrangler iterates on the data by number of rows (equal to the received INTEGER).

    `P.S.` `chunked=True` is faster and uses less memory while `chunked=INTEGER` is more precise
    in the number of rows for each DataFrame.


    Note
    ----
    In case of `use_threads=True` the number of threads
    that will be spawned will be gotten from os.cpu_count().

    Parameters
    ----------
    sql : str
        SQL query.
    path : Union[str, List[str]]
        S3 path to write stage files (e.g. s3://bucket_name/any_name/)
    con : redshift_connector.Connection
        Use redshift_connector.connect() to use "
        "credentials directly or wr.redshift.connect() to fetch it from the Glue Catalog.
    iam_role : str, optional
        AWS IAM role with the related permissions.
    aws_access_key_id : str, optional
        The access key for your AWS account.
    aws_secret_access_key : str, optional
        The secret key for your AWS account.
    aws_session_token : str, optional
        The session key for your AWS account. This is only needed when you are using temporary credentials.
    region : str, optional
        Specifies the AWS Region where the target Amazon S3 bucket is located.
        REGION is required for UNLOAD to an Amazon S3 bucket that isn't in the
        same AWS Region as the Amazon Redshift cluster. By default, UNLOAD
        assumes that the target Amazon S3 bucket is located in the same AWS
        Region as the Amazon Redshift cluster.
    max_file_size : float, optional
        Specifies the maximum size (MB) of files that UNLOAD creates in Amazon S3.
        Specify a decimal value between 5.0 MB and 6200.0 MB. If None, the default
        maximum file size is 6200.0 MB.
    kms_key_id : str, optional
        Specifies the key ID for an AWS Key Management Service (AWS KMS) key to be
        used to encrypt data files on Amazon S3.
    keep_files : bool
        Should keep stage files?
    dtype_backend: str, optional
        Which dtype_backend to use, e.g. whether a DataFrame should have NumPy arrays,
        nullable dtypes are used for all dtypes that have a nullable implementation when
        “numpy_nullable” is set, pyarrow is used for all dtypes if “pyarrow” is set.

        The dtype_backends are still experimential. The "pyarrow" backend is only supported with Pandas 2.0 or above.
    chunked : Union[int, bool]
        If passed will split the data in a Iterable of DataFrames (Memory friendly).
        If `True` awswrangler iterates on the data by files in the most efficient way without guarantee of chunksize.
        If an `INTEGER` is passed awswrangler will iterate on the data by number of rows equal the received INTEGER.
    use_threads : bool, int
        True to enable concurrent requests, False to disable multiple threads.
        If enabled os.cpu_count() will be used as the max number of threads.
        If integer is provided, specified number is used.
    boto3_session : boto3.Session(), optional
        Boto3 Session. The default boto3 session will be used if boto3_session receive None.
    s3_additional_kwargs : Dict[str, str], optional
        Forward to botocore requests.
    pyarrow_additional_kwargs : Dict[str, Any], optional
        Forwarded to `to_pandas` method converting from PyArrow tables to Pandas DataFrame.
        Valid values include "split_blocks", "self_destruct", "ignore_metadata".
        e.g. pyarrow_additional_kwargs={'split_blocks': True}.

    Returns
    -------
    Union[pandas.DataFrame, Iterator[pandas.DataFrame]]
        Result as Pandas DataFrame(s).

    Examples
    --------
    >>> import awswrangler as wr
    >>> con = wr.redshift.connect("MY_GLUE_CONNECTION")
    >>> df = wr.redshift.unload(
    ...     sql="SELECT * FROM public.mytable",
    ...     path="s3://bucket/extracted_parquet_files/",
    ...     con=con,
    ...     iam_role="arn:aws:iam::XXX:role/XXX"
    ... )
    >>> con.close()

    """
    path = path if path.endswith("/") else f"{path}/"
    unload_to_files(
        sql=sql,
        path=path,
        con=con,
        iam_role=iam_role,
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        aws_session_token=aws_session_token,
        region=region,
        max_file_size=max_file_size,
        kms_key_id=kms_key_id,
        manifest=False,
        boto3_session=boto3_session,
    )
    if chunked is False:
        df: pd.DataFrame = s3.read_parquet(
            path=path,
            chunked=chunked,
            dataset=False,
            use_threads=use_threads,
            dtype_backend=dtype_backend,
            boto3_session=boto3_session,
            s3_additional_kwargs=s3_additional_kwargs,
            pyarrow_additional_kwargs=pyarrow_additional_kwargs,
        )
        if keep_files is False:
            _logger.debug("Deleting objects in S3 path: %s", path)
            s3.delete_objects(
                path=path,
                use_threads=use_threads,
                boto3_session=boto3_session,
                s3_additional_kwargs=s3_additional_kwargs,
            )
        return df
    return _read_parquet_iterator(
        path=path,
        chunked=chunked,
        use_threads=use_threads,
        dtype_backend=dtype_backend,
        boto3_session=boto3_session,
        s3_additional_kwargs=s3_additional_kwargs,
        keep_files=keep_files,
        pyarrow_additional_kwargs=pyarrow_additional_kwargs,
    )
