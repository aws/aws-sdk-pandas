"""Amazon Lake Formation Module gathering all read functions."""
import logging
import sys
from typing import Any, Dict, Iterator, Optional, Union

import boto3
import pandas as pd
import pyarrow as pa

from awswrangler import _utils, exceptions
from awswrangler._config import apply_configs
from awswrangler.lakeformation._utils import wait_query

_logger: logging.Logger = logging.getLogger(__name__)


@apply_configs
def read_sql_query(
    sql: str,
    database: str,
    transaction_id: Optional[str] = None,
    query_as_of_time: Optional[str] = None,
    catalog_id: Optional[str] = None,
    chunksize: Optional[Union[int, bool]] = None,
    boto3_session: Optional[boto3.Session] = None,
    params: Optional[Dict[str, Any]] = None,
) -> Union[pd.DataFrame, Iterator[pd.DataFrame]]:
    """Execute PartiQL query against an AWS Glue Governed Table based on Transaction ID or time travel timestamp. Return single Pandas DataFrame or Iterator.

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
    `chunksize` argument (memory-friendly) (i.e batching):

    Return an Iterable of DataFrames instead of a regular DataFrame.

    There are two batching strategies:

    - If **chunksize=True**, a new DataFrame will be returned for each file in the query result.

    - If **chunksize=INTEGER**, Wrangler will iterate on the data so that the DataFrames number of rows is equal to INTEGER.

    `P.S.` `chunksize=True` is faster and uses less memory

    Parameters
    ----------
    sql : str
        partiQL query.
    database : str
        AWS Glue database name
    transaction_id : str, optional
        The ID of the transaction at which to read the table contents. Cannot be specified alongside query_as_of_time
    query_as_of_time : str, optional
        The time as of when to read the table contents. Must be a valid Unix epoch timestamp. Cannot be specified alongside transaction_id
    catalog_id : str, optional
        The ID of the Data Catalog from which to retrieve Databases.
        If none is provided, the AWS account ID is used by default.
    chunksize : Union[int, bool], optional
        If passed will split the data into an Iterable of DataFrames (memory-friendly).
        If `True`, Wrangler will iterate on the data by files in the most efficient way without guarantee of chunksize.
        If an `INTEGER` is passed, Wrangler will iterate on the data so that the DataFrames number of rows is equal to INTEGER.
    boto3_session : boto3.Session(), optional
        Boto3 Session. The default boto3 session will be used if boto3_session receives None.
    params: Dict[str, any], optional
        Dict of parameters used to format the partiQL query. Only named parameters are supported.
        The dict must contain the information in the form {'name': 'value'} and the SQL query must contain
        `:name;`.

    Returns
    -------
    Union[pd.DataFrame, Iterator[pd.DataFrame]]
        Pandas DataFrame or Generator of Pandas DataFrames if chunksize is passed.

    Examples
    --------
    >>> import awswrangler as wr
    >>> df = wr.lakeformation.read_sql_query(
    ...     sql="SELECT * FROM my_table LIMIT 10;",
    ...     database="my_db",
    ...     transaction_id="ba9a11b5-619a-4ac3-bd70-5a744d09414c"
    ... )

    >>> import awswrangler as wr
    >>> df = wr.lakeformation.read_sql_query(
    ...     sql="SELECT * FROM my_table WHERE name=:name;",
    ...     database="my_db",
    ...     query_as_of_time="1611142914",
    ...     params={"name": "filtered_name"}
    ... )

    """
    if transaction_id is None and query_as_of_time is None:
        raise exceptions.InvalidArgumentCombination("Please pass one of transaction_id or query_as_of_time")
    # TODO: Generate transaction_id if both transaction_id and query_as_of_time missing?
    if transaction_id is not None and query_as_of_time is not None:
        raise exceptions.InvalidArgumentCombination("Please pass only one of transaction_id or query_as_of_time, not both")
    session: boto3.Session = _utils.ensure_session(session=boto3_session)
    chunksize = sys.maxsize if chunksize is True else chunksize
    if params is None:
        params = {}
    for key, value in params.items():
        sql = sql.replace(f":{key};", str(value))
    client_lakeformation: boto3.client = _utils.client(service_name="lakeformation", session=session)
    # TODO: Check if the Glue Table is governed?

    args: Dict[str, Any] = {
        "DatabaseName": database,
        "Statement": sql
    }
    if catalog_id:
        args["CatalogId"] = catalog_id
    if transaction_id:
        args["TransactionId"] = transaction_id
    else:
        args["QueryAsOfTime"] = query_as_of_time
    query_id: str = client_lakeformation.plan_query(**args)["QueryId"]

    wait_query(query_id=query_id, boto3_session=session)

    work_units_output: Dict[str, Any] = client_lakeformation.get_work_units(QueryId=query_id)
    print(work_units_output)

    a = client_lakeformation.execute(QueryId=query_id, Token=work_units_output["Units"][0]["Token"], WorkUnitId=0)
    print(a)

    buf = a["Messages"].read()
    stream = pa.RecordBatchStreamReader(buf)
    table = stream.read_all()
    df = table.to_pandas()
    return df
