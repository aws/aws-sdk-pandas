"""Amazon Lake Formation Module gathering all read functions."""
from __future__ import annotations

import itertools
import logging
from typing import TYPE_CHECKING, Any

import boto3
import pandas as pd
from pyarrow import NativeFile, RecordBatchStreamReader, Table
from typing_extensions import Literal

from awswrangler import _data_types, _utils, catalog
from awswrangler._config import apply_configs
from awswrangler._distributed import engine
from awswrangler._executor import _BaseExecutor, _get_executor
from awswrangler._sql_formatter import _process_sql_params
from awswrangler.catalog._utils import _catalog_id, _transaction_id
from awswrangler.lakeformation._utils import commit_transaction, start_transaction, wait_query

if TYPE_CHECKING:
    from mypy_boto3_lakeformation.client import LakeFormationClient

_logger: logging.Logger = logging.getLogger(__name__)


@engine.dispatch_on_engine
def _get_work_unit_results(
    client_lakeformation: "LakeFormationClient" | None,
    query_id: str,
    token_work_unit: tuple[str, int],
) -> Table:
    client_lakeformation = client_lakeformation if client_lakeformation else _utils.client(service_name="lakeformation")
    token, work_unit = token_work_unit
    messages: NativeFile = client_lakeformation.get_work_unit_results(
        QueryId=query_id, WorkUnitToken=token, WorkUnitId=work_unit
    )["ResultStream"]
    return RecordBatchStreamReader(messages.read()).read_all()


def _resolve_sql_query(
    query_id: str,
    use_threads: bool,
    boto3_session: boto3.Session | None,
    arrow_kwargs: dict[str, Any],
) -> pd.DataFrame:
    client_lakeformation = _utils.client(service_name="lakeformation", session=boto3_session)

    wait_query(query_id=query_id, boto3_session=boto3_session)

    # The LF Query Engine distributes the load across workers
    # Retrieve the tokens and their associated work units until NextToken is ''
    # One Token can span multiple work units
    # PageSize determines the size of the "Units" array in each call
    scan_kwargs: dict[str, str | int] = {"QueryId": query_id, "PageSize": 10}
    next_token: str | None = "init_token"  # Dummy token
    token_work_units: list[tuple[str, int]] = []
    while next_token:
        response = client_lakeformation.get_work_units(**scan_kwargs)  # type: ignore[arg-type]
        token_work_units.extend(  # [(Token0, WorkUnitId0), (Token0, WorkUnitId1), (Token1, WorkUnitId2) ... ]
            [
                (unit["WorkUnitToken"], unit_id)
                for unit in response["WorkUnitRanges"]
                for unit_id in range(unit["WorkUnitIdMin"], unit["WorkUnitIdMax"] + 1)  # Max is inclusive
            ]
        )
        next_token = response.get("NextToken", None)
        scan_kwargs["NextToken"] = next_token  # type: ignore[assignment]

    executor: _BaseExecutor = _get_executor(use_threads=use_threads)

    tables = executor.map(
        _get_work_unit_results,
        client_lakeformation,
        itertools.repeat(query_id),
        token_work_units,
    )
    return _utils.table_refs_to_df(tables, kwargs=arrow_kwargs)


@apply_configs
@_utils.validate_distributed_kwargs(
    unsupported_kwargs=["boto3_session", "dtype_backend"],
)
def read_sql_query(
    sql: str,
    database: str,
    transaction_id: str | None = None,
    query_as_of_time: str | None = None,
    catalog_id: str | None = None,
    dtype_backend: Literal["numpy_nullable", "pyarrow"] = "numpy_nullable",
    use_threads: bool = True,
    boto3_session: boto3.Session | None = None,
    params: dict[str, Any] | None = None,
    pyarrow_additional_kwargs: dict[str, Any] | None = None,
) -> pd.DataFrame:
    """Execute PartiQL query on AWS Glue Table (Transaction ID or time travel timestamp). Return Pandas DataFrame.

    Note
    ----
    ORDER BY operations are not honored.
    i.e. sql="SELECT * FROM my_table ORDER BY my_column" is NOT valid

    Note
    ----
    The database must NOT be explicitly defined in the PartiQL statement.
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
        AWS Glue database name.
    transaction_id : str, optional
        The ID of the transaction at which to read the table contents.
        Cannot be specified alongside query_as_of_time.
    query_as_of_time : str, optional
        The time as of when to read the table contents. Must be a valid Unix epoch timestamp.
        Cannot be specified alongside transaction_id.
    catalog_id : str, optional
        The ID of the Data Catalog from which to retrieve Databases.
        If none is provided, the AWS account ID is used by default.
    dtype_backend: str, optional
        Which dtype_backend to use, e.g. whether a DataFrame should have NumPy arrays,
        nullable dtypes are used for all dtypes that have a nullable implementation when
        “numpy_nullable” is set, pyarrow is used for all dtypes if “pyarrow” is set.

        The dtype_backends are still experimential. The "pyarrow" backend is only supported with Pandas 2.0 or above.
    use_threads : bool
        True to enable concurrent requests, False to disable multiple threads.
        When enabled, os.cpu_count() is used as the max number of threads.
    boto3_session : boto3.Session(), optional
        Boto3 Session. The default boto3 session is used if boto3_session receives None.
    params : Dict[str, any], optional
        Dict of parameters used to format the partiQL query. Only named parameters are supported.
        The dict must contain the information in the form {"name": "value"} and the SQL query must contain
        `:name`.
    pyarrow_additional_kwargs : Dict[str, Any], optional
        Forwarded to `to_pandas` method converting from PyArrow tables to Pandas DataFrame.
        Valid values include "split_blocks", "self_destruct", "ignore_metadata".
        e.g. pyarrow_additional_kwargs={'split_blocks': True}.

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
    ...     sql="SELECT * FROM my_table WHERE name=:name AND city=:city",
    ...     database="my_db",
    ...     query_as_of_time="1611142914",
    ...     params={"name": "filtered_name", "city": "filtered_city"}
    ... )

    """
    client_lakeformation = _utils.client(service_name="lakeformation", session=boto3_session)
    commit_trans: bool = False

    sql = _process_sql_params(sql, params, engine_type="partiql")

    if not any([transaction_id, query_as_of_time]):
        _logger.debug("Neither `transaction_id` nor `query_as_of_time` were specified, starting transaction")
        transaction_id = start_transaction(read_only=True, boto3_session=boto3_session)
        commit_trans = True
    args: dict[str, str | None] = _catalog_id(
        catalog_id=catalog_id,
        **_transaction_id(transaction_id=transaction_id, query_as_of_time=query_as_of_time, DatabaseName=database),
    )
    result = client_lakeformation.start_query_planning(
        QueryString=sql,
        QueryPlanningContext=args,  # type: ignore[arg-type]
    )
    query_id: str = result["QueryId"]
    arrow_kwargs = _data_types.pyarrow2pandas_defaults(
        use_threads=use_threads, kwargs=pyarrow_additional_kwargs, dtype_backend=dtype_backend
    )
    df = _resolve_sql_query(
        query_id=query_id,
        use_threads=use_threads,
        boto3_session=boto3_session,
        arrow_kwargs=arrow_kwargs,
    )
    if commit_trans:
        commit_transaction(transaction_id=transaction_id, boto3_session=boto3_session)  # type: ignore[arg-type]
    return df


@apply_configs
@_utils.validate_distributed_kwargs(
    unsupported_kwargs=["boto3_session", "dtype_backend"],
)
def read_sql_table(
    table: str,
    database: str,
    transaction_id: str | None = None,
    query_as_of_time: str | None = None,
    catalog_id: str | None = None,
    dtype_backend: Literal["numpy_nullable", "pyarrow"] = "numpy_nullable",
    use_threads: bool = True,
    boto3_session: boto3.Session | None = None,
    pyarrow_additional_kwargs: dict[str, Any] | None = None,
) -> pd.DataFrame:
    """Extract all rows from AWS Glue Table (Transaction ID or time travel timestamp). Return Pandas DataFrame.

    Note
    ----
    ORDER BY operations are not honored.
    i.e. sql="SELECT * FROM my_table ORDER BY my_column" is NOT valid

    Note
    ----
    Pass one of `transaction_id` or `query_as_of_time`, not both.

    Parameters
    ----------
    table : str
        AWS Glue table name.
    database : str
        AWS Glue database name.
    transaction_id : str, optional
        The ID of the transaction at which to read the table contents.
        Cannot be specified alongside query_as_of_time.
    query_as_of_time : str, optional
        The time as of when to read the table contents. Must be a valid Unix epoch timestamp.
        Cannot be specified alongside transaction_id.
    catalog_id : str, optional
        The ID of the Data Catalog from which to retrieve Databases.
        If none is provided, the AWS account ID is used by default.
    dtype_backend: str, optional
        Which dtype_backend to use, e.g. whether a DataFrame should have NumPy arrays,
        nullable dtypes are used for all dtypes that have a nullable implementation when
        “numpy_nullable” is set, pyarrow is used for all dtypes if “pyarrow” is set.

        The dtype_backends are still experimential. The "pyarrow" backend is only supported with Pandas 2.0 or above.
    use_threads : bool
        True to enable concurrent requests, False to disable multiple threads.
        When enabled, os.cpu_count() is used as the max number of threads.
    boto3_session : boto3.Session(), optional
        Boto3 Session. The default boto3 session is used if boto3_session receives None.
    pyarrow_additional_kwargs : Dict[str, Any], optional
        Forwarded to `to_pandas` method converting from PyArrow tables to Pandas DataFrame.
        Valid values include "split_blocks", "self_destruct", "ignore_metadata".
        e.g. pyarrow_additional_kwargs={'split_blocks': True}.

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
        catalog_id=catalog_id,
        dtype_backend=dtype_backend,
        use_threads=use_threads,
        boto3_session=boto3_session,
        pyarrow_additional_kwargs=pyarrow_additional_kwargs,
    )
