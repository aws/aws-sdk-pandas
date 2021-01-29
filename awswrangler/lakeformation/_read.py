"""Amazon Lake Formation Module gathering all read functions."""
import concurrent.futures
import itertools
import logging
from typing import Any, Dict, Iterator, List, Optional, Tuple, Union

import boto3
import pandas as pd
from pyarrow import NativeFile, RecordBatchStreamReader

from awswrangler import _utils, exceptions
from awswrangler._config import apply_configs
from awswrangler.lakeformation._utils import wait_query

_logger: logging.Logger = logging.getLogger(__name__)


def _execute_query(
    query_id: str,
    token_work_unit: Tuple[str, int],
    boto3_session: Optional[boto3.Session] = None,
) -> pd.DataFrame:
    session: boto3.Session = _utils.ensure_session(session=boto3_session)
    client_lakeformation: boto3.client = _utils.client(service_name="lakeformation", session=session)
    token, work_unit = token_work_unit
    messages: NativeFile = client_lakeformation.execute(QueryId=query_id, Token=token, WorkUnitId=work_unit)["Messages"]
    return RecordBatchStreamReader(messages.read()).read_pandas()


def _resolve_sql_query(
    query_id: str,
    chunked: Optional[bool] = None,
    use_threads: bool = True,
    boto3_session: Optional[boto3.Session] = None,
) -> Union[pd.DataFrame, Iterator[pd.DataFrame]]:
    session: boto3.Session = _utils.ensure_session(session=boto3_session)
    client_lakeformation: boto3.client = _utils.client(service_name="lakeformation", session=session)

    wait_query(query_id=query_id, boto3_session=session)

    # The LF Query Engine distributes the load across workers
    # Retrieve the tokens and their associated work units until NextToken is ''
    # One Token can span multiple work units
    # PageSize determines the size of the "Units" array in each call
    # TODO: Inquire about good page size # pylint: disable=W0511
    scan_kwargs: Dict[str, Union[str, int]] = {"QueryId": query_id, "PageSize": 2}
    next_token: str = "init_token"  # Dummy token
    token_work_units: List[Tuple[str, int]] = []
    while next_token:
        response = client_lakeformation.get_work_units(**scan_kwargs)
        token_work_units.extend(  # [(Token0, WorkUnitId0), (Token0, WorkUnitId1), (Token1, WorkUnitId2) ... ]
            [
                (unit["Token"], unit_id)
                for unit in response["Units"]
                for unit_id in range(unit["WorkUnitIdMin"], unit["WorkUnitIdMax"] + 1)  # Max is inclusive
            ]
        )
        next_token = response["NextToken"]
        scan_kwargs["NextToken"] = next_token

    dfs: List[pd.DataFrame] = list()
    if use_threads is False:
        dfs = list(
            _execute_query(query_id=query_id, token_work_unit=token_work_unit, boto3_session=boto3_session)
            for token_work_unit in token_work_units
        )
    else:
        cpus: int = _utils.ensure_cpu_count(use_threads=use_threads)
        with concurrent.futures.ThreadPoolExecutor(max_workers=cpus) as executor:
            dfs = list(
                executor.map(
                    _execute_query,
                    itertools.repeat(query_id),
                    token_work_units,
                    itertools.repeat(_utils.boto3_to_primitives(boto3_session=boto3_session)),
                )
            )
    if not chunked:
        return pd.concat(dfs)
    return dfs


@apply_configs
def read_sql_query(
    sql: str,
    database: str,
    transaction_id: Optional[str] = None,
    query_as_of_time: Optional[str] = None,
    catalog_id: Optional[str] = None,
    chunked: bool = False,
    use_threads: bool = True,
    boto3_session: Optional[boto3.Session] = None,
    params: Optional[Dict[str, Any]] = None,
) -> Union[pd.DataFrame, Iterator[pd.DataFrame]]:
    """Execute PartiQL query on AWS Glue Table (Transaction ID or time travel timestamp). Return Pandas DataFrame.

    Note
    ----
    The database must NOT be explicitely defined in the PartiQL statement.
    i.e. sql="SELECT * FROM my_table" is valid
    but sql="SELECT * FROM my_db.my_table" is NOT valid

    Note
    ----
    Pass one of `transaction_id` or `query_as_of_time`, not both.

    Note
    ----
    `chunked` argument (memory-friendly):
    If set to `True`, return an Iterable of DataFrames instead of a regular DataFrame.

    Parameters
    ----------
    sql : str
        partiQL query.
    database : str
        AWS Glue database name
    transaction_id : str, optional
        The ID of the transaction at which to read the table contents.
        Cannot be specified alongside query_as_of_time
    query_as_of_time : str, optional
        The time as of when to read the table contents. Must be a valid Unix epoch timestamp.
        Cannot be specified alongside transaction_id
    catalog_id : str, optional
        The ID of the Data Catalog from which to retrieve Databases.
        If none is provided, the AWS account ID is used by default.
    chunked : bool, optional
        If `True`, Wrangler returns an Iterable of DataFrames with no guarantee of chunksize.
    use_threads : bool
        True to enable concurrent requests, False to disable multiple threads.
        When enabled, os.cpu_count() is used as the max number of threads.
    boto3_session : boto3.Session(), optional
        Boto3 Session. The default boto3 session is used if boto3_session receives None.
    params: Dict[str, any], optional
        Dict of parameters used to format the partiQL query. Only named parameters are supported.
        The dict must contain the information in the form {"name": "value"} and the SQL query must contain
        `:name`.

    Returns
    -------
    Union[pd.DataFrame, Iterator[pd.DataFrame]]
        Pandas DataFrame or Generator of Pandas DataFrames if chunked is passed.

    Examples
    --------
    >>> import awswrangler as wr
    >>> df = wr.lakeformation.read_sql_query(
    ...     sql="SELECT * FROM my_table;",
    ...     database="my_db",
    ...     catalog_id="111111111111"
    ... )

    >>> import awswrangler as wr
    >>> df = wr.lakeformation.read_sql_query(
    ...     sql="SELECT * FROM my_table LIMIT 10;",
    ...     database="my_db",
    ...     transaction_id="1b62811fa3e02c4e5fdbaa642b752030379c4a8a70da1f8732ce6ccca47afdc9"
    ... )

    >>> import awswrangler as wr
    >>> df = wr.lakeformation.read_sql_query(
    ...     sql="SELECT * FROM my_table WHERE name=:name;",
    ...     database="my_db",
    ...     query_as_of_time="1611142914",
    ...     params={"name": "'filtered_name'"}
    ... )

    """
    if transaction_id is not None and query_as_of_time is not None:
        raise exceptions.InvalidArgumentCombination(
            "Please pass only one of `transaction_id` or `query_as_of_time`, not both"
        )
    session: boto3.Session = _utils.ensure_session(session=boto3_session)
    client_lakeformation: boto3.client = _utils.client(service_name="lakeformation", session=session)
    if params is None:
        params = {}
    for key, value in params.items():
        sql = sql.replace(f":{key}", str(value))

    args: Dict[str, Optional[str]] = {"DatabaseName": database, "Statement": sql}
    if catalog_id:
        args["CatalogId"] = catalog_id
    if query_as_of_time:
        args["QueryAsOfTime"] = query_as_of_time
    elif transaction_id:
        args["TransactionId"] = transaction_id
    else:
        _logger.debug("Neither `transaction_id` nor `query_as_of_time` were specified, beginning transaction")
        transaction_id = client_lakeformation.begin_transaction(ReadOnly=True)["TransactionId"]
        args["TransactionId"] = transaction_id
    query_id: str = client_lakeformation.plan_query(**args)["QueryId"]

    return _resolve_sql_query(query_id=query_id, chunked=chunked, use_threads=use_threads, boto3_session=boto3_session)
