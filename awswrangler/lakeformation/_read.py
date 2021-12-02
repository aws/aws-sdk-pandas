"""Amazon Lake Formation Module gathering all read functions."""
import concurrent.futures
import itertools
import logging
from typing import Any, Dict, List, Optional, Tuple, Union

import boto3
import pandas as pd
from pyarrow import NativeFile, RecordBatchStreamReader, Table, concat_tables

from awswrangler import _data_types, _utils, catalog
from awswrangler._config import apply_configs
from awswrangler.catalog._utils import _catalog_id, _transaction_id
from awswrangler.lakeformation._utils import commit_transaction, start_transaction, wait_query

_logger: logging.Logger = logging.getLogger(__name__)


def _get_work_unit_results(
    query_id: str,
    token_work_unit: Tuple[str, int],
    client_lakeformation: boto3.client,
) -> Table:
    token, work_unit = token_work_unit
    messages: NativeFile = client_lakeformation.get_work_unit_results(
        QueryId=query_id, WorkUnitToken=token, WorkUnitId=work_unit
    )["ResultStream"]
    return RecordBatchStreamReader(messages.read()).read_all()


def _resolve_sql_query(
    query_id: str,
    categories: Optional[List[str]],
    safe: bool,
    map_types: bool,
    use_threads: bool,
    boto3_session: boto3.Session,
) -> pd.DataFrame:
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

    tables: List[Table] = []
    if use_threads is False:
        tables = list(
            _get_work_unit_results(
                query_id=query_id,
                token_work_unit=token_work_unit,
                client_lakeformation=client_lakeformation,
            )
            for token_work_unit in token_work_units
        )
    else:
        cpus: int = _utils.ensure_cpu_count(use_threads=use_threads)
        with concurrent.futures.ThreadPoolExecutor(max_workers=cpus) as executor:
            tables = list(
                executor.map(
                    _get_work_unit_results,
                    itertools.repeat(query_id),
                    token_work_units,
                    itertools.repeat(client_lakeformation),
                )
            )
    table = concat_tables(tables)
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
    return _utils.ensure_df_is_mutable(df=table.to_pandas(**args))


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
) -> pd.DataFrame:
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
    pd.DataFrame
        Pandas DataFrame.

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
    session: boto3.Session = _utils.ensure_session(session=boto3_session)
    client_lakeformation: boto3.client = _utils.client(service_name="lakeformation", session=session)
    commit_trans: bool = False
    if params is None:
        params = {}
    for key, value in params.items():
        sql = sql.replace(f":{key};", str(value))

    if not any([transaction_id, query_as_of_time]):
        _logger.debug("Neither `transaction_id` nor `query_as_of_time` were specified, starting transaction")
        transaction_id = start_transaction(read_only=True, boto3_session=session)
        commit_trans = True
    args: Dict[str, Optional[str]] = _catalog_id(
        catalog_id=catalog_id,
        **_transaction_id(transaction_id=transaction_id, query_as_of_time=query_as_of_time, DatabaseName=database),
    )
    query_id: str = client_lakeformation.start_query_planning(QueryString=sql, QueryPlanningContext=args)["QueryId"]
    df = _resolve_sql_query(
        query_id=query_id,
        categories=categories,
        safe=safe,
        map_types=map_types,
        use_threads=use_threads,
        boto3_session=session,
    )
    if commit_trans:
        commit_transaction(transaction_id=transaction_id)  # type: ignore
    return df


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
) -> pd.DataFrame:
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
    pd.DataFrame
        Pandas DataFrame.

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
