"""Amazon Athena Module."""

import ast
import csv
import logging
import re
import time
import unicodedata
from datetime import date
from decimal import Decimal
from typing import Any, Dict, Iterator, List, Optional, Tuple, Union

import boto3  # type: ignore
import pandas as pd  # type: ignore
import pyarrow as pa  # type: ignore

from awswrangler import _data_types, _utils, catalog, exceptions, s3

_logger: logging.Logger = logging.getLogger(__name__)

_QUERY_WAIT_POLLING_DELAY: float = 0.2  # SECONDS


def _normalize_name(name: str) -> str:
    name = "".join(c for c in unicodedata.normalize("NFD", name) if unicodedata.category(c) != "Mn")
    name = name.replace("{", "_")
    name = name.replace("}", "_")
    name = name.replace("]", "_")
    name = name.replace("[", "_")
    name = name.replace(")", "_")
    name = name.replace("(", "_")
    name = name.replace(" ", "_")
    name = name.replace("-", "_")
    name = name.replace(".", "_")
    name = re.sub("(.)([A-Z][a-z]+)", r"\1_\2", name)
    name = re.sub("([a-z0-9])([A-Z])", r"\1_\2", name)
    name = name.lower()
    name = re.sub(r"(_)\1+", "\\1", name)  # remove repeated underscores
    name = name[1:] if name.startswith("_") else name  # remove trailing underscores
    name = name[:-1] if name.endswith("_") else name  # remove trailing underscores
    return name


def normalize_column_name(column: str) -> str:
    """Convert the column name to be compatible with Amazon Athena.

    https://docs.aws.amazon.com/athena/latest/ug/tables-databases-columns-names.html

    Parameters
    ----------
    column : str
        Column name.

    Returns
    -------
    str
        Normalized column name.

    Examples
    --------
    >>> import awswrangler as wr
    >>> wr.athena.normalize_column_name("MyNewColumn")
    "my_new_column"

    """
    return _normalize_name(name=column)


def normalize_table_name(table: str) -> str:
    """Convert the table name to be compatible with Amazon Athena.

    https://docs.aws.amazon.com/athena/latest/ug/tables-databases-columns-names.html

    Parameters
    ----------
    table : str
        Table name.

    Returns
    -------
    str
        Normalized table name.

    Examples
    --------
    >>> import awswrangler as wr
    >>> wr.athena.normalize_table_name("MyNewTable")
    "my_new_table"

    """
    return _normalize_name(name=table)


def get_query_columns_types(query_execution_id: str, boto3_session: Optional[boto3.Session] = None) -> Dict[str, str]:
    """Get the data type of all columns queried.

    https://docs.aws.amazon.com/athena/latest/ug/data-types.html

    Parameters
    ----------
    query_execution_id : str
        Athena query execution ID.
    boto3_session : boto3.Session(), optional
        Boto3 Session. The default boto3 session will be used if boto3_session receive None.

    Returns
    -------
    Dict[str, str]
        Dictionary with all data types.

    Examples
    --------
    >>> import awswrangler as wr
    >>> wr.athena.get_query_columns_types("query-execution-id")
    {"col0": "int", "col1": "double"}

    """
    client_athena: boto3.client = _utils.client(service_name="s3", session=boto3_session)
    response: Dict = client_athena.get_query_results(QueryExecutionId=query_execution_id, MaxResults=1)
    col_info: List[Dict[str, str]] = response["ResultSet"]["ResultSetMetadata"]["ColumnInfo"]
    return {x["Name"]: x["Type"] for x in col_info}


def create_athena_bucket(boto3_session: Optional[boto3.Session] = None) -> str:
    """Create the default Athena bucket if it doesn't exist.

    Parameters
    ----------
    boto3_session : boto3.Session(), optional
        Boto3 Session. The default boto3 session will be used if boto3_session receive None.

    Returns
    -------
    str
        Bucket s3 path (E.g. s3://aws-athena-query-results-ACCOUNT-REGION/)

    Examples
    --------
    >>> import awswrangler as wr
    >>> wr.athena.create_athena_bucket()
    "s3://aws-athena-query-results-ACCOUNT-REGION/"

    """
    session: boto3.Session = _utils.ensure_session(session=boto3_session)
    account_id: str = _utils.client(service_name="sts", session=session).get_caller_identity().get("Account")
    region_name: str = str(session.region_name).lower()
    s3_output = f"s3://aws-athena-query-results-{account_id}-{region_name}/"
    s3_resource = session.resource("s3")
    s3_resource.Bucket(s3_output)
    return s3_output


def start_query_execution(
    sql: str,
    database: Optional[str] = None,
    s3_output: Optional[str] = None,
    workgroup: Optional[str] = None,
    encryption: Optional[str] = None,
    kms_key: Optional[str] = None,
    boto3_session: Optional[boto3.Session] = None,
) -> str:
    """Start a SQL Query against AWS Athena.

    Note
    ----
    Create the default Athena bucket if it doesn't exist and s3_output is None.
    (E.g. s3://aws-athena-query-results-ACCOUNT-REGION/)

    Parameters
    ----------
    sql : str
        SQL query.
    database : str, optional
        AWS Glue/Athena database name.
    s3_output : str, optional
        AWS S3 path.
    workgroup : str, optional
        Athena workgroup.
    encryption : str, optional
        None, 'SSE_S3', 'SSE_KMS', 'CSE_KMS'.
    kms_key : str, optional
        For SSE-KMS and CSE-KMS , this is the KMS key ARN or ID.
    boto3_session : boto3.Session(), optional
        Boto3 Session. The default boto3 session will be used if boto3_session receive None.

    Returns
    -------
    str
        Query execution ID

    Examples
    --------
    >>> import awswrangler as wr
    >>> query_exec_id = wr.athena.start_query_execution(sql="...", database="...")

    """
    args: Dict[str, Any] = {"QueryString": sql}
    session: boto3.Session = _utils.ensure_session(session=boto3_session)

    # s3_output
    if s3_output is None:
        s3_output = create_athena_bucket(boto3_session=session)
    args["ResultConfiguration"] = {"OutputLocation": s3_output}

    # encryption
    if encryption is not None:
        args["ResultConfiguration"]["EncryptionConfiguration"] = {"EncryptionOption": encryption}
        if kms_key is not None:
            args["ResultConfiguration"]["EncryptionConfiguration"]["KmsKey"] = kms_key

    # database
    if database is not None:
        args["QueryExecutionContext"] = {"Database": database}

    # workgroup
    if workgroup is not None:
        args["WorkGroup"] = workgroup

    _logger.debug(f"args: {args}")
    client_athena: boto3.client = _utils.client(service_name="athena", session=session)
    response = client_athena.start_query_execution(**args)
    return response["QueryExecutionId"]


def wait_query(query_execution_id: str, boto3_session: Optional[boto3.Session] = None) -> Dict[str, Any]:
    """Wait for the query end.

    Parameters
    ----------
    query_execution_id : str
        Athena query execution ID.
    boto3_session : boto3.Session(), optional
        Boto3 Session. The default boto3 session will be used if boto3_session receive None.

    Returns
    -------
    Dict[str, Any]
        Dictionary with the get_query_execution response.

    Examples
    --------
    >>> import awswrangler as wr
    >>> res = wr.athena.wait_query(query_execution_id="query-execution-id")

    """
    final_states: List[str] = ["FAILED", "SUCCEEDED", "CANCELLED"]
    client_athena: boto3.client = _utils.client(service_name="athena", session=boto3_session)
    response: Dict[str, Any] = client_athena.get_query_execution(QueryExecutionId=query_execution_id)
    state: str = response["QueryExecution"]["Status"]["State"]
    while state not in final_states:
        time.sleep(_QUERY_WAIT_POLLING_DELAY)
        response = client_athena.get_query_execution(QueryExecutionId=query_execution_id)
        state = response["QueryExecution"]["Status"]["State"]
    _logger.debug(f"state: {state}")
    _logger.debug(f"StateChangeReason: {response['QueryExecution']['Status'].get('StateChangeReason')}")
    if state == "FAILED":
        raise exceptions.QueryFailed(response["QueryExecution"]["Status"].get("StateChangeReason"))
    if state == "CANCELLED":
        raise exceptions.QueryCancelled(response["QueryExecution"]["Status"].get("StateChangeReason"))
    return response


def repair_table(
    table: str,
    database: Optional[str] = None,
    s3_output: Optional[str] = None,
    workgroup: Optional[str] = None,
    encryption: Optional[str] = None,
    kms_key: Optional[str] = None,
    boto3_session: Optional[boto3.Session] = None,
) -> str:
    """Run the Hive's metastore consistency check: "MSCK REPAIR TABLE table;".

    Recovers partitions and data associated with partitions.
    Use this statement when you add partitions to the catalog.
    It is possible it will take some time to add all partitions.
    If this operation times out, it will be in an incomplete state
    where only a few partitions are added to the catalog.

    Note
    ----
    Create the default Athena bucket if it doesn't exist and s3_output is None.
    (E.g. s3://aws-athena-query-results-ACCOUNT-REGION/)

    Parameters
    ----------
    table : str
        Table name.
    database : str, optional
        AWS Glue/Athena database name.
    s3_output : str, optional
        AWS S3 path.
    workgroup : str, optional
        Athena workgroup.
    encryption : str, optional
        None, 'SSE_S3', 'SSE_KMS', 'CSE_KMS'.
    kms_key : str, optional
        For SSE-KMS and CSE-KMS , this is the KMS key ARN or ID.
    boto3_session : boto3.Session(), optional
        Boto3 Session. The default boto3 session will be used if boto3_session receive None.

    Returns
    -------
    str
        Query final state ('SUCCEEDED', 'FAILED', 'CANCELLED').

    Examples
    --------
    >>> import awswrangler as wr
    >>> query_final_state = wr.athena.repair_table(table="...", database="...")

    """
    query = f"MSCK REPAIR TABLE {table};"
    session: boto3.Session = _utils.ensure_session(session=boto3_session)
    query_id = start_query_execution(
        sql=query,
        database=database,
        s3_output=s3_output,
        workgroup=workgroup,
        encryption=encryption,
        kms_key=kms_key,
        boto3_session=session,
    )
    response: Dict[str, Any] = wait_query(query_execution_id=query_id, boto3_session=session)
    return response["QueryExecution"]["Status"]["State"]


def _extract_ctas_manifest_paths(path: str, boto3_session: Optional[boto3.Session] = None) -> List[str]:
    """Get the list of paths of the generated files."""
    bucket_name, key_path = _utils.parse_path(path)
    client_s3: boto3.client = _utils.client(service_name="s3", session=boto3_session)
    body: bytes = client_s3.get_object(Bucket=bucket_name, Key=key_path)["Body"].read()
    return [x for x in body.decode("utf-8").split("\n") if x != ""]


def _list_parser(value: str) -> List[Union[int, float, str, None]]:
    """Parser for lists serialized as string."""
    # try resolve with a simple literal_eval
    try:
        return ast.literal_eval(value)
    except ValueError:
        pass  # keep trying

    # sanity check
    if len(value) <= 1:
        return []

    items: List[Union[None, str]] = [None if x == "null" else x for x in value[1:-1].split(", ")]
    array_type: Optional[type] = None

    # check if all values are integers
    for item in items:
        if item is not None:
            try:
                int(item)  # type: ignore
            except ValueError:
                break
    else:
        array_type = int

    # check if all values are floats
    if array_type is None:
        for item in items:
            if item is not None:
                try:
                    float(item)  # type: ignore
                except ValueError:
                    break
        else:
            array_type = float

    # check if all values are strings
    array_type = str if array_type is None else array_type

    return [array_type(x) if x is not None else None for x in items]


def _get_query_metadata(
    query_execution_id: str, boto3_session: Optional[boto3.Session] = None
) -> Tuple[Dict[str, str], List[str], List[str], Dict[str, Any]]:
    """Get query metadata."""
    cols_types: Dict[str, str] = get_query_columns_types(
        query_execution_id=query_execution_id, boto3_session=boto3_session
    )
    _logger.debug(f"cols_types: {cols_types}")
    dtype: Dict[str, str] = {}
    parse_timestamps: List[str] = []
    parse_dates: List[str] = []
    converters: Dict[str, Any] = {}
    col_name: str
    col_type: str
    for col_name, col_type in cols_types.items():
        pandas_type: str = _data_types.athena2pandas(dtype=col_type)
        if pandas_type in ["datetime64", "date"]:
            parse_timestamps.append(col_name)
            if pandas_type == "date":
                parse_dates.append(col_name)
        elif pandas_type == "list":
            converters[col_name] = _list_parser
        elif pandas_type == "bool":
            _logger.debug(f"Ignoring bool column: {col_name}")
        elif pandas_type == "decimal":
            converters[col_name] = lambda x: Decimal(str(x)) if str(x) != "" else None
        else:
            dtype[col_name] = pandas_type
    _logger.debug(f"dtype: {dtype}")
    _logger.debug(f"parse_timestamps: {parse_timestamps}")
    _logger.debug(f"parse_dates: {parse_dates}")
    _logger.debug(f"converters: {converters}")
    return dtype, parse_timestamps, parse_dates, converters


def _apply_dates_to_iterator(iterator: Iterator[pd.DataFrame], parse_dates: List[str]) -> Iterator[pd.DataFrame]:
    """Apply date cast to a iterator of DataFrames."""
    for df in iterator:
        if len(df.index) > 0:
            for col in parse_dates:
                df[col] = df[col].dt.date.replace(to_replace={pd.NaT: None})
        yield df


def read_sql_query(  # pylint: disable=too-many-branches,too-many-locals
    sql: str,
    database: str,
    ctas_approach: bool = True,
    chunksize: Optional[int] = None,
    s3_output: Optional[str] = None,
    workgroup: Optional[str] = None,
    encryption: Optional[str] = None,
    kms_key: Optional[str] = None,
    use_threads: bool = True,
    boto3_session: Optional[boto3.Session] = None,
) -> Union[pd.DataFrame, Iterator[pd.DataFrame]]:
    """Execute any SQL query on AWS Athena and return the results as a Pandas DataFrame.

    There are two approaches to be defined through ctas_approach parameter:

    1 - `ctas_approach=True` (`Default`):
    Wrap the query with a CTAS and then reads the table data as parquet directly from s3.
    PROS: Faster and has a better handle of nested types
    CONS: Can't use chunksize and must have create and drop table permissions on Glue.

    2 - `ctas_approach False`:
    Does a regular query on Athena and parse the regular CSV result on s3.
    PROS: Accepts chunksize argument.
    CONS: Slower (But stills faster than other libraries that uses the Athena API)
    and does not handle nested types so well

    Note
    ----
    If ctas_approach is False and chunksize is passed, then a iterator of DataFrames is returned.

    Note
    ----
    If ctas_approach is True, chunksize will be ignored.

    Note
    ----
    Create the default Athena bucket if it doesn't exist and s3_output is None.
    (E.g. s3://aws-athena-query-results-ACCOUNT-REGION/)

    Note
    ----
    In case of ``use_threads=True`` the number of process that will be spawned will be get from os.cpu_count().

    Parameters
    ----------
    sql : str
        SQL query.
    database : str
        AWS Glue/Athena database name.
    ctas_approach: bool
        Wraps the query using a CTAS, and read the resulted parquet data on S3.
        If false, read the regular CSV on S3.
    chunksize: int, optional
        If specified, return an iterator where chunksize is the number of rows to include in each chunk.
    s3_output : str, optional
        AWS S3 path.
    workgroup : str, optional
        Athena workgroup.
    encryption : str, optional
        None, 'SSE_S3', 'SSE_KMS', 'CSE_KMS'.
    kms_key : str, optional
        For SSE-KMS and CSE-KMS , this is the KMS key ARN or ID.
    use_threads : bool
        True to enable concurrent requests, False to disable multiple threads.
        If enabled os.cpu_count() will be used as the max number of threads.
    boto3_session : boto3.Session(), optional
        Boto3 Session. The default boto3 session will be used if boto3_session receive None.

    Returns
    -------
    Union[pd.DataFrame, Iterator[pd.DataFrame]]
        Pandas DataFrame or Iterator of Pandas DataFrames if chunksize is passed.

    Examples
    --------
    >>> import awswrangler as wr
    >>> df = wr.athena.read_sql_query(sql="...", database="...")

    """
    session: boto3.Session = _utils.ensure_session(session=boto3_session)
    if s3_output is None:
        _s3_output: str = create_athena_bucket(boto3_session=session)
    else:
        _s3_output = s3_output
    _s3_output = _s3_output[:-1] if _s3_output[-1] == "/" else _s3_output
    name: str = ""
    if ctas_approach is True:
        name = f"temp_table_{pa.compat.guid()}"
        path: str = f"{_s3_output}/{name}"
        sql = (
            f"CREATE TABLE {name}\n"
            f"WITH(\n"
            f"    format = 'Parquet',\n"
            f"    parquet_compression = 'SNAPPY',\n"
            f"    external_location = '{path}'\n"
            f") AS\n"
            f"{sql}"
        )
    _logger.debug(f"sql: {sql}")
    query_id: str = start_query_execution(
        sql=sql,
        database=database,
        s3_output=_s3_output,
        workgroup=workgroup,
        encryption=encryption,
        kms_key=kms_key,
        boto3_session=session,
    )
    _logger.debug(f"query_id: {query_id}")
    query_response: Dict[str, Any] = wait_query(query_execution_id=query_id, boto3_session=session)
    if query_response["QueryExecution"]["Status"]["State"] in ["FAILED", "CANCELLED"]:
        reason: str = query_response["QueryExecution"]["Status"]["StateChangeReason"]
        message_error: str = f"Query error: {reason}"
        raise exceptions.AthenaQueryError(message_error)
    df: pd.DataFrame
    if ctas_approach is True:
        catalog.delete_table_if_exists(database=database, table=name, boto3_session=session)
        manifest_path: str = f"{_s3_output}/tables/{query_id}-manifest.csv"
        paths: List[str] = _extract_ctas_manifest_paths(path=manifest_path, boto3_session=session)
        _logger.debug(f"paths: {paths}")
        if not paths:
            df = pd.DataFrame()
        else:
            df = s3.read_parquet(path=paths, use_threads=use_threads, boto3_session=session)
        s3.delete_objects(path=[manifest_path] + paths, use_threads=use_threads, boto3_session=session)
        return df
    dtype, parse_timestamps, parse_dates, converters = _get_query_metadata(
        query_execution_id=query_id, boto3_session=session
    )
    path = f"{_s3_output}{query_id}.csv"
    _logger.debug("Start CSV reading...")
    ret = s3.read_csv(
        path=path,
        dtype=dtype,
        parse_dates=parse_timestamps,
        converters=converters,
        quoting=csv.QUOTE_ALL,
        keep_default_na=False,
        na_values=[""],
        chunksize=chunksize,
    )
    _logger.debug("Start type casting...")
    if chunksize is None:
        if len(ret.index) > 0:
            for col in parse_dates:
                if str(ret[col].dtype) == "object":
                    ret[col] = ret[col].apply(lambda x: date(*[int(y) for y in x.split("-")]))
                else:
                    ret[col] = ret[col].dt.date.replace(to_replace={pd.NaT: None})
        return ret
    return _apply_dates_to_iterator(iterator=ret, parse_dates=parse_dates)
