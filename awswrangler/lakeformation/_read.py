"""Amazon Lake Formation Module gathering all read functions."""
import concurrent.futures
import itertools
import logging
from typing import Any, Dict, Iterator, List, Optional, Tuple, Union

import boto3
import pandas as pd
from pyarrow import NativeFile, RecordBatchStreamReader, Table

from awswrangler import _data_types, _utils, catalog, exceptions
from awswrangler._config import apply_configs
from awswrangler.catalog._utils import _catalog_id
from awswrangler.lakeformation._utils import cancel_transaction, start_transaction, wait_query

_logger: logging.Logger = logging.getLogger(__name__)


def _get_work_unit_results(
    query_id: str,
    token_work_unit: Tuple[str, int],
    categories: Optional[List[str]],
    safe: bool,
    map_types: bool,
    use_threads: bool,
    boto3_session: boto3.Session,
) -> pd.DataFrame:
    client_lakeformation: boto3.client = _utils.client(service_name="lakeformation", session=boto3_session)
    token, work_unit = token_work_unit
    messages: NativeFile = client_lakeformation.get_work_unit_results(
        QueryId=query_id, WorkUnitToken=token, WorkUnitId=work_unit
    )["ResultStream"]
    table: Table = RecordBatchStreamReader(messages.read()).read_all()
    args: Dict[str, Any] = {}
    if table.num_rows > 0:
        args = {
            "use_threads": use_threads,
            "split_blocks": True,
            "self_destruct": True,
            "integer_object_nulls": False,
            "date_as_object": True,
            "ignore_metadata": True,
            "strings_to_categorical": False,
            "categories": categories,
            "safe": safe,
            "types_mapper": _data_types.pyarrow2pandas_extension if map_types else None,
        }
    df: pd.DataFrame = _utils.ensure_df_is_mutable(df=table.to_pandas(**args))
    return df


def _resolve_sql_query(
    query_id: str,
    categories: Optional[List[str]],
    safe: bool,
    map_types: bool,
    use_threads: bool,
    boto3_session: boto3.Session,
) -> Union[pd.DataFrame, Iterator[pd.DataFrame]]:
    client_lakeformation: boto3.client = _utils.client(service_name="lakeformation", session=boto3_session)

    wait_query(query_id=query_id, boto3_session=boto3_session)

    # The LF Query Engine distributes the load across workers
    # Retrieve the tokens and their associated work units until NextToken is ''
    # One Token can span multiple work units
    # PageSize determines the size of the "Units" array in each call
    scan_kwargs: Dict[str, Union[str, int]] = {"QueryId": query_id, "PageSize": 10}
    next_token: str = "init_token"  # Dummy token
    token_work_units: List[Tuple[str, int]] = []
    while next_token:
        response = client_lakeformation.get_work_units(**scan_kwargs)
        token_work_units.extend(  # [(Token0, WorkUnitId0), (Token0, WorkUnitId1), (Token1, WorkUnitId2) ... ]
            [
                (unit["WorkUnitToken"], unit_id)
                for unit in response["WorkUnitRanges"]
                for unit_id in range(unit["WorkUnitIdMin"], unit["WorkUnitIdMax"] + 1)  # Max is inclusive
            ]
        )
        next_token = response.get("NextToken", None)
        scan_kwargs["NextToken"] = next_token

    dfs: List[pd.DataFrame] = list()
    if use_threads is False:
        dfs = list(
            _get_work_unit_results(
                query_id=query_id,
                token_work_unit=token_work_unit,
                categories=categories,
                safe=safe,
                map_types=map_types,
                use_threads=use_threads,
                boto3_session=boto3_session,
            )
            for token_work_unit in token_work_units
        )
    else:
        cpus: int = _utils.ensure_cpu_count(use_threads=use_threads)
        with concurrent.futures.ThreadPoolExecutor(max_workers=cpus) as executor:
            dfs = list(
                executor.map(
                    _get_work_unit_results,
                    itertools.repeat(query_id),
                    token_work_units,
                    itertools.repeat(categories),
                    itertools.repeat(safe),
                    itertools.repeat(map_types),
                    itertools.repeat(use_threads),
                    itertools.repeat(_utils.boto3_to_primitives(boto3_session=boto3_session)),
                )
            )
    return pd.concat([df for df in dfs if not df.empty], sort=False, copy=False, ignore_index=False)


@apply_configs
def read_sql_query(
    sql: str,
    database: str,
    transaction_id: Optional[str] = None,
    query_as_of_time: Optional[str] = None,
    catalog_id: Optional[str] = None,
    categories: Optional[List[str]] = None,
    safe: bool = True,
    map_types: bool = True,
    use_threads: bool = True,
    boto3_session: Optional[boto3.Session] = None,
    params: Optional[Dict[str, Any]] = None,
) -> Union[pd.DataFrame, Iterator[pd.DataFrame]]:
    """Execute PartiQL query on AWS Glue Table (Transaction ID or time travel timestamp). Return Pandas DataFrame.

    Note
    ----
    ORDER BY operations are not honoured.
    i.e. sql="SELECT * FROM my_table ORDER BY my_column" is NOT valid

    Note
    ----
    The database must NOT be explicitely defined in the PartiQL statement.
    i.e. sql="SELECT * FROM my_table" is valid
    but sql="SELECT * FROM my_db.my_table" is NOT valid

    Note
    ----
    Pass one of `transaction_id` or `query_as_of_time`, not both.

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
    categories: Optional[List[str]], optional
        List of columns names that should be returned as pandas.Categorical.
        Recommended for memory restricted environments.
    safe : bool, default True
        For certain data types, a cast is needed in order to store the
        data in a pandas DataFrame or Series (e.g. timestamps are always
        stored as nanoseconds in pandas). This option controls whether it
        is a safe cast or not.
    map_types : bool, default True
        True to convert pyarrow DataTypes to pandas ExtensionDtypes. It is
        used to override the default pandas type for conversion of built-in
        pyarrow types or in absence of pandas_metadata in the Table schema.
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
    ...     sql="SELECT * FROM my_table WHERE name=:name; AND city=:city;",
    ...     database="my_db",
    ...     query_as_of_time="1611142914",
    ...     params={"name": "'filtered_name'", "city": "'filtered_city'"}
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
        sql = sql.replace(f":{key};", str(value))

    args: Dict[str, Optional[str]] = _catalog_id(catalog_id=catalog_id, **{"DatabaseName": database})
    if query_as_of_time:
        args["QueryAsOfTime"] = query_as_of_time
    elif transaction_id:
        args["TransactionId"] = transaction_id
    else:
        _logger.debug("Neither `transaction_id` nor `query_as_of_time` were specified, starting transaction")
        transaction_id = start_transaction(read_only=True, boto3_session=session)
        args["TransactionId"] = transaction_id
    query_id: str = client_lakeformation.start_query_planning(QueryString=sql, QueryPlanningContext=args)["QueryId"]
    try:
        return _resolve_sql_query(
            query_id=query_id,
            categories=categories,
            safe=safe,
            map_types=map_types,
            use_threads=use_threads,
            boto3_session=session,
        )
    except Exception as ex:
        _logger.debug("Canceling transaction with ID: %s.", transaction_id)
        if transaction_id:
            cancel_transaction(transaction_id=transaction_id, boto3_session=session)
        _logger.error(ex)
        raise


@apply_configs
def read_sql_table(
    table: str,
    database: str,
    transaction_id: Optional[str] = None,
    query_as_of_time: Optional[str] = None,
    catalog_id: Optional[str] = None,
    categories: Optional[List[str]] = None,
    safe: bool = True,
    map_types: bool = True,
    use_threads: bool = True,
    boto3_session: Optional[boto3.Session] = None,
) -> Union[pd.DataFrame, Iterator[pd.DataFrame]]:
    """Extract all rows from AWS Glue Table (Transaction ID or time travel timestamp). Return Pandas DataFrame.

    Note
    ----
    ORDER BY operations are not honoured.
    i.e. sql="SELECT * FROM my_table ORDER BY my_column" is NOT valid

    Note
    ----
    Pass one of `transaction_id` or `query_as_of_time`, not both.

    Parameters
    ----------
    table : str
        AWS Glue table name.
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
    categories: Optional[List[str]], optional
        List of columns names that should be returned as pandas.Categorical.
        Recommended for memory restricted environments.
    safe : bool, default True
        For certain data types, a cast is needed in order to store the
        data in a pandas DataFrame or Series (e.g. timestamps are always
        stored as nanoseconds in pandas). This option controls whether it
        is a safe cast or not.
    map_types : bool, default True
        True to convert pyarrow DataTypes to pandas ExtensionDtypes. It is
        used to override the default pandas type for conversion of built-in
        pyarrow types or in absence of pandas_metadata in the Table schema.
    use_threads : bool
        True to enable concurrent requests, False to disable multiple threads.
        When enabled, os.cpu_count() is used as the max number of threads.
    boto3_session : boto3.Session(), optional
        Boto3 Session. The default boto3 session is used if boto3_session receives None.

    Returns
    -------
    Union[pd.DataFrame, Iterator[pd.DataFrame]]
        Pandas DataFrame or Generator of Pandas DataFrames if chunked is passed.

    Examples
    --------
    >>> import awswrangler as wr
    >>> df = wr.lakeformation.read_sql_table(
    ...     table="my_table",
    ...     database="my_db",
    ...     catalog_id="111111111111",
    ... )

    >>> import awswrangler as wr
    >>> df = wr.lakeformation.read_sql_table(
    ...     table="my_table",
    ...     database="my_db",
    ...     transaction_id="1b62811fa3e02c4e5fdbaa642b752030379c4a8a70da1f8732ce6ccca47afdc9",
    ... )

    >>> import awswrangler as wr
    >>> df = wr.lakeformation.read_sql_table(
    ...     table="my_table",
    ...     database="my_db",
    ...     query_as_of_time="1611142914",
    ...     use_threads=True,
    ... )

    """
    table = catalog.sanitize_table_name(table=table)
    return read_sql_query(
        sql=f"SELECT * FROM {table}",
        database=database,
        transaction_id=transaction_id,
        query_as_of_time=query_as_of_time,
        safe=safe,
        map_types=map_types,
        catalog_id=catalog_id,
        categories=categories,
        use_threads=use_threads,
        boto3_session=boto3_session,
    )
