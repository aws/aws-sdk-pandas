"""Amazon Clean Rooms Module hosting read_* functions."""

import logging
from typing import Any, Dict, Iterator, Optional, Union

import boto3

import awswrangler.pandas as pd
from awswrangler import _utils, s3
from awswrangler._sql_formatter import _process_sql_params
from awswrangler.cleanrooms._utils import wait_query

_logger: logging.Logger = logging.getLogger(__name__)


def _delete_after_iterate(
    dfs: Iterator[pd.DataFrame], keep_files: bool, kwargs: Dict[str, Any]
) -> Iterator[pd.DataFrame]:
    for df in dfs:
        yield df
    if keep_files is False:
        s3.delete_objects(**kwargs)


def read_sql_query(
    sql: str,
    membership_id: str,
    output_bucket: str,
    output_prefix: str,
    keep_files: bool = True,
    params: Optional[Dict[str, Any]] = None,
    chunksize: Optional[Union[int, bool]] = None,
    use_threads: Union[bool, int] = True,
    boto3_session: Optional[boto3.Session] = None,
    pyarrow_additional_kwargs: Optional[Dict[str, Any]] = None,
) -> Union[Iterator[pd.DataFrame], pd.DataFrame]:
    """Execute Clean Rooms Protected SQL query and return the results as a Pandas DataFrame.

    Parameters
    ----------
    sql : str
        SQL query
    membership_id : str
        Membership ID
    output_bucket : str
        S3 output bucket name
    output_prefix : str
        S3 output prefix
    keep_files : bool, optional
        Whether files in S3 output bucket/prefix are retained. 'True' by default
    params : Dict[str, any], optional
        Dict of parameters used for constructing the SQL query. Only named parameters are supported.
        The dict must be in the form {'name': 'value'} and the SQL query must contain
        `:name`. Note that for varchar columns and similar, you must surround the value in single quotes
    chunksize : Union[int, bool], optional
        If passed, the data is split into an iterable of DataFrames (Memory friendly).
        If `True` an iterable of DataFrames is returned without guarantee of chunksize.
        If an `INTEGER` is passed, an iterable of DataFrames is returned with maximum rows
        equal to the received INTEGER
    use_threads : Union[bool, int], optional
        True to enable concurrent requests, False to disable multiple threads.
        If enabled os.cpu_count() is used as the maximum number of threads.
        If integer is provided, specified number is used
    boto3_session : boto3.Session, optional
        Boto3 Session. If None, the default boto3 session is used
    pyarrow_additional_kwargs : Optional[Dict[str, Any]]
        Forwarded to `to_pandas` method converting from PyArrow tables to Pandas DataFrame.
        Valid values include "split_blocks", "self_destruct", "ignore_metadata".
        e.g. pyarrow_additional_kwargs={'split_blocks': True}

    Returns
    -------
    Union[Iterator[pd.DataFrame], pd.DataFrame]
        Pandas DataFrame or Generator of Pandas DataFrames if chunksize is provided.

    Examples
    --------
    >>> import awswrangler as wr
    >>> df = wr.cleanrooms.read_sql_query(
    >>>     sql='SELECT DISTINCT...',
    >>>     membership_id='membership-id',
    >>>     output_bucket='output-bucket',
    >>>     output_prefix='output-prefix',
    >>> )
    """
    client_cleanrooms = _utils.client(service_name="cleanrooms", session=boto3_session)

    query_id: str = client_cleanrooms.start_protected_query(
        type="SQL",
        membershipIdentifier=membership_id,
        sqlParameters={"queryString": _process_sql_params(sql, params, engine_type="partiql")},
        resultConfiguration={
            "outputConfiguration": {
                "s3": {
                    "bucket": output_bucket,
                    "keyPrefix": output_prefix,
                    "resultFormat": "PARQUET",
                }
            }
        },
    )["protectedQuery"]["id"]

    _logger.debug("query_id: %s", query_id)
    path: str = wait_query(membership_id=membership_id, query_id=query_id, boto3_session=boto3_session)[
        "protectedQuery"
    ]["result"]["output"]["s3"]["location"]

    _logger.debug("path: %s", path)
    chunked: Union[bool, int] = False if chunksize is None else chunksize
    ret = s3.read_parquet(
        path=path,
        use_threads=use_threads,
        chunked=chunked,
        boto3_session=boto3_session,
        pyarrow_additional_kwargs=pyarrow_additional_kwargs,
    )

    _logger.debug("type(ret): %s", type(ret))
    kwargs: Dict[str, Any] = {
        "path": path,
        "use_threads": use_threads,
        "boto3_session": boto3_session,
    }
    if chunked is False:
        if keep_files is False:
            s3.delete_objects(**kwargs)
        return ret
    return _delete_after_iterate(ret, keep_files, kwargs)
