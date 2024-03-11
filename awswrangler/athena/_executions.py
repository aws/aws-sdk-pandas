"""Query executions Module for Amazon Athena."""

from __future__ import annotations

import logging
import time
from typing import (
    Any,
    Dict,
    cast,
)

import boto3
import botocore
from typing_extensions import Literal

from awswrangler import _utils, exceptions, typing
from awswrangler._config import apply_configs

from ._cache import _CacheInfo, _check_for_cached_results
from ._utils import (
    _QUERY_FINAL_STATES,
    _QUERY_WAIT_POLLING_DELAY,
    _apply_formatter,
    _get_workgroup_config,
    _start_query_execution,
    _WorkGroupConfig,
)

_logger: logging.Logger = logging.getLogger(__name__)


@apply_configs
def start_query_execution(
    sql: str,
    database: str | None = None,
    s3_output: str | None = None,
    workgroup: str = "primary",
    encryption: str | None = None,
    kms_key: str | None = None,
    params: dict[str, Any] | list[str] | None = None,
    paramstyle: Literal["qmark", "named"] = "named",
    boto3_session: boto3.Session | None = None,
    client_request_token: str | None = None,
    athena_cache_settings: typing.AthenaCacheSettings | None = None,
    athena_query_wait_polling_delay: float = _QUERY_WAIT_POLLING_DELAY,
    data_source: str | None = None,
    wait: bool = False,
) -> str | dict[str, Any]:
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
    workgroup : str
        Athena workgroup. Primary by default.
    encryption : str, optional
        None, 'SSE_S3', 'SSE_KMS', 'CSE_KMS'.
    kms_key : str, optional
        For SSE-KMS and CSE-KMS , this is the KMS key ARN or ID.
    params: Dict[str, any] | List[str], optional
        Parameters that will be used for constructing the SQL query.
        Only named or question mark parameters are supported.
        The parameter style needs to be specified in the ``paramstyle`` parameter.

        For ``paramstyle="named"``, this value needs to be a dictionary.
        The dict needs to contain the information in the form ``{'name': 'value'}`` and the SQL query needs to contain
        ``:name``.
        The formatter will be applied client-side in this scenario.

        For ``paramstyle="qmark"``, this value needs to be a list of strings.
        The formatter will be applied server-side.
        The values are applied sequentially to the parameters in the query in the order in which the parameters occur.
    paramstyle: str, optional
        Determines the style of ``params``.
        Possible values are:

        - ``named``
        - ``qmark``
    boto3_session : boto3.Session(), optional
        Boto3 Session. The default boto3 session will be used if boto3_session receive None.
    client_request_token : str, optional
        A unique case-sensitive string used to ensure the request to create the query is idempotent (executes only once).
        If another StartQueryExecution request is received, the same response is returned and another query is not created.
        If a parameter has changed, for example, the QueryString , an error is returned.
        If you pass the same client_request_token value with different parameters the query fails with error
        message "Idempotent parameters do not match". Use this only with ctas_approach=False and unload_approach=False
        and disabled cache.
    athena_cache_settings: typing.AthenaCacheSettings, optional
        Parameters of the Athena cache settings such as max_cache_seconds, max_cache_query_inspections,
        max_remote_cache_entries, and max_local_cache_entries.
        AthenaCacheSettings is a `TypedDict`, meaning the passed parameter can be instantiated either as an
        instance of AthenaCacheSettings or as a regular Python dict.
        If cached results are valid, awswrangler ignores the `ctas_approach`, `s3_output`, `encryption`, `kms_key`,
        `keep_files` and `ctas_temp_table_name` params.
        If reading cached data fails for any reason, execution falls back to the usual query run path.
    athena_query_wait_polling_delay: float, default: 0.25 seconds
        Interval in seconds for how often the function will check if the Athena query has completed.
    data_source : str, optional
        Data Source / Catalog name. If None, 'AwsDataCatalog' will be used by default.
    wait : bool, default False
        Indicates whether to wait for the query to finish and return a dictionary with the query execution response.

    Returns
    -------
    Union[str, Dict[str, Any]]
        Query execution ID if `wait` is set to `False`, dictionary with the get_query_execution response otherwise.

    Examples
    --------
    Querying into the default data source (Amazon s3 - 'AwsDataCatalog')

    >>> import awswrangler as wr
    >>> query_exec_id = wr.athena.start_query_execution(sql='...', database='...')

    Querying into another data source (PostgreSQL, Redshift, etc)

    >>> import awswrangler as wr
    >>> query_exec_id = wr.athena.start_query_execution(sql='...', database='...', data_source='...')

    """
    # Substitute query parameters if applicable
    sql, execution_params = _apply_formatter(sql, params, paramstyle)
    _logger.debug("Executing query:\n%s", sql)

    if not client_request_token:
        cache_info: _CacheInfo = _check_for_cached_results(
            sql=sql,
            boto3_session=boto3_session,
            workgroup=workgroup,
            athena_cache_settings=athena_cache_settings,
        )
        _logger.debug("Cache info:\n%s", cache_info)

    if not client_request_token and cache_info.has_valid_cache and cache_info.query_execution_id is not None:
        query_execution_id = cache_info.query_execution_id
        _logger.debug("Valid cache found. Retrieving...")
    else:
        wg_config: _WorkGroupConfig = _get_workgroup_config(session=boto3_session, workgroup=workgroup)
        query_execution_id = _start_query_execution(
            sql=sql,
            wg_config=wg_config,
            database=database,
            data_source=data_source,
            s3_output=s3_output,
            workgroup=workgroup,
            encryption=encryption,
            kms_key=kms_key,
            execution_params=execution_params,
            client_request_token=client_request_token,
            boto3_session=boto3_session,
        )
    if wait:
        return wait_query(
            query_execution_id=query_execution_id,
            boto3_session=boto3_session,
            athena_query_wait_polling_delay=athena_query_wait_polling_delay,
        )

    return query_execution_id


def stop_query_execution(query_execution_id: str, boto3_session: boto3.Session | None = None) -> None:
    """Stop a query execution.

    Requires you to have access to the workgroup in which the query ran.

    Parameters
    ----------
    query_execution_id : str
        Athena query execution ID.
    boto3_session : boto3.Session(), optional
        Boto3 Session. The default boto3 session will be used if boto3_session receive None.

    Returns
    -------
    None
        None.

    Examples
    --------
    >>> import awswrangler as wr
    >>> wr.athena.stop_query_execution(query_execution_id='query-execution-id')

    """
    client_athena = _utils.client(service_name="athena", session=boto3_session)
    client_athena.stop_query_execution(QueryExecutionId=query_execution_id)


@apply_configs
def wait_query(
    query_execution_id: str,
    boto3_session: boto3.Session | None = None,
    athena_query_wait_polling_delay: float = _QUERY_WAIT_POLLING_DELAY,
) -> dict[str, Any]:
    """Wait for the query end.

    Parameters
    ----------
    query_execution_id : str
        Athena query execution ID.
    boto3_session : boto3.Session(), optional
        Boto3 Session. The default boto3 session will be used if boto3_session receive None.
    athena_query_wait_polling_delay: float, default: 0.25 seconds
        Interval in seconds for how often the function will check if the Athena query has completed.

    Returns
    -------
    Dict[str, Any]
        Dictionary with the get_query_execution response.

    Examples
    --------
    >>> import awswrangler as wr
    >>> res = wr.athena.wait_query(query_execution_id='query-execution-id')

    """
    response: dict[str, Any] = get_query_execution(query_execution_id=query_execution_id, boto3_session=boto3_session)
    state: str = response["Status"]["State"]
    while state not in _QUERY_FINAL_STATES:
        time.sleep(athena_query_wait_polling_delay)
        response = get_query_execution(query_execution_id=query_execution_id, boto3_session=boto3_session)
        state = response["Status"]["State"]
    _logger.debug("Query state: %s", state)
    _logger.debug("Query state change reason: %s", response["Status"].get("StateChangeReason"))
    if state == "FAILED":
        raise exceptions.QueryFailed(response["Status"].get("StateChangeReason"))
    if state == "CANCELLED":
        raise exceptions.QueryCancelled(response["Status"].get("StateChangeReason"))
    return response


def get_query_execution(query_execution_id: str, boto3_session: boto3.Session | None = None) -> dict[str, Any]:
    """Fetch query execution details.

    https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/athena.html#Athena.Client.get_query_execution

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
    >>> res = wr.athena.get_query_execution(query_execution_id='query-execution-id')

    """
    client_athena = _utils.client(service_name="athena", session=boto3_session)
    response = _utils.try_it(
        f=client_athena.get_query_execution,
        ex=botocore.exceptions.ClientError,
        ex_code="ThrottlingException",
        max_num_tries=5,
        QueryExecutionId=query_execution_id,
    )
    _logger.debug("Get query execution response:\n%s", response)
    return cast(Dict[str, Any], response["QueryExecution"])
