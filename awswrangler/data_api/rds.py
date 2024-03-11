"""RDS Data API Connector."""

from __future__ import annotations

import datetime as dt
import logging
import time
import uuid
from decimal import Decimal
from typing import TYPE_CHECKING, Any, Callable, TypeVar, cast

import boto3
from typing_extensions import Literal

import awswrangler.pandas as pd
from awswrangler import _data_types, _databases, _utils, exceptions
from awswrangler._sql_utils import identifier
from awswrangler.data_api import _connector

if TYPE_CHECKING:
    from mypy_boto3_rds_data.client import BotocoreClientError
    from mypy_boto3_rds_data.type_defs import BatchExecuteStatementResponseTypeDef, ExecuteStatementResponseTypeDef


_logger = logging.getLogger(__name__)


_ExecuteStatementResponseType = TypeVar(
    "_ExecuteStatementResponseType", "ExecuteStatementResponseTypeDef", "BatchExecuteStatementResponseTypeDef"
)


class RdsDataApi(_connector.DataApiConnector):
    """Provides access to the RDS Data API.

    Parameters
    ----------
    resource_arn: str
        ARN for the RDS resource.
    database: str
        Target database name.
    secret_arn: str
        The ARN for the secret to be used for authentication.
    sleep: float
        Number of seconds to sleep between connection attempts to paused clusters - defaults to 0.5.
    backoff: float
        Factor by which to increase the sleep between connection attempts to paused clusters - defaults to 1.0.
    retries: int
        Maximum number of connection attempts to paused clusters - defaults to 10.
    boto3_session : boto3.Session(), optional
        The boto3 session. If `None`, the default boto3 session is used.
    """

    def __init__(
        self,
        resource_arn: str,
        database: str,
        secret_arn: str = "",
        sleep: float = 0.5,
        backoff: float = 1.0,
        retries: int = 30,
        boto3_session: boto3.Session | None = None,
    ) -> None:
        super().__init__()

        self.client = _utils.client(service_name="rds-data", session=boto3_session)

        self.resource_arn = resource_arn
        self.database = database
        self.secret_arn = secret_arn
        self.wait_config = _connector.WaitConfig(sleep, backoff, retries)
        self.results: dict[str, "ExecuteStatementResponseTypeDef" | "BatchExecuteStatementResponseTypeDef"] = {}

    def close(self) -> None:
        """Close underlying endpoint connections."""
        self.client.close()

    def begin_transaction(self, database: str | None = None, schema: str | None = None) -> str:
        """Start an SQL transaction."""
        if database is None:
            database = self.database

        kwargs = {}
        if database:
            kwargs["database"] = database
        if schema:
            kwargs["schema"] = schema

        response = self.client.begin_transaction(
            resourceArn=self.resource_arn,
            secretArn=self.secret_arn,
            **kwargs,
        )
        return response["transactionId"]

    def commit_transaction(self, transaction_id: str) -> str:
        """Commit an SQL transaction."""
        response = self.client.commit_transaction(
            resourceArn=self.resource_arn,
            secretArn=self.secret_arn,
            transactionId=transaction_id,
        )
        return response["transactionStatus"]

    def rollback_transaction(self, transaction_id: str) -> str:
        """Roll back an SQL transaction."""
        response = self.client.rollback_transaction(
            resourceArn=self.resource_arn,
            secretArn=self.secret_arn,
            transactionId=transaction_id,
        )
        return response["transactionStatus"]

    def _execute_with_retry(
        self,
        sql: str,
        function: Callable[[str], _ExecuteStatementResponseType],
    ) -> str:
        sleep: float = self.wait_config.sleep
        total_tries: int = 0
        total_sleep: float = 0
        response: _ExecuteStatementResponseType | None = None
        last_exception: "BotocoreClientError" | None = None
        while total_tries < self.wait_config.retries:
            try:
                response = function(sql)
                _logger.debug(
                    "Response received after %s tries and sleeping for a total of %s seconds", total_tries, total_sleep
                )
                break
            except self.client.exceptions.BadRequestException as exception:
                last_exception = exception
                total_sleep += sleep
                _logger.debug("BadRequestException occurred: %s", exception)
                _logger.debug(
                    "Cluster may be paused - sleeping for %s seconds for a total of %s before retrying",
                    sleep,
                    total_sleep,
                )
                time.sleep(sleep)
                total_tries += 1
                sleep *= self.wait_config.backoff

        if response is None:
            _logger.exception("Maximum BadRequestException retries reached for query %s", sql)
            raise last_exception  # type: ignore[misc]

        request_id: str = uuid.uuid4().hex
        self.results[request_id] = response
        return request_id

    def _execute_statement(
        self,
        sql: str,
        database: str | None = None,
        transaction_id: str | None = None,
        parameters: list[dict[str, Any]] | None = None,
    ) -> str:
        if database is None:
            database = self.database

        additional_kwargs: dict[str, Any] = {}
        if transaction_id:
            additional_kwargs["transactionId"] = transaction_id
        if parameters:
            additional_kwargs["parameters"] = parameters

        def function(sql: str) -> "ExecuteStatementResponseTypeDef":
            return self.client.execute_statement(
                resourceArn=self.resource_arn,
                database=database,
                sql=sql,
                secretArn=self.secret_arn,
                includeResultMetadata=True,
                **additional_kwargs,
            )

        return self._execute_with_retry(sql=sql, function=function)

    def _batch_execute_statement(
        self,
        sql: str | list[str],
        database: str | None = None,
        transaction_id: str | None = None,
        parameter_sets: list[list[dict[str, Any]]] | None = None,
    ) -> str:
        if isinstance(sql, list):
            raise exceptions.InvalidArgumentType("`sql` parameter cannot be list.")

        if database is None:
            database = self.database

        additional_kwargs: dict[str, Any] = {}
        if transaction_id:
            additional_kwargs["transactionId"] = transaction_id
        if parameter_sets:
            additional_kwargs["parameterSets"] = parameter_sets

        def function(sql: str) -> "BatchExecuteStatementResponseTypeDef":
            return self.client.batch_execute_statement(
                resourceArn=self.resource_arn,
                database=database,
                sql=sql,
                secretArn=self.secret_arn,
                **additional_kwargs,
            )

        return self._execute_with_retry(sql=sql, function=function)

    def _get_statement_result(self, request_id: str) -> pd.DataFrame:
        try:
            result = cast("ExecuteStatementResponseTypeDef", self.results.pop(request_id))
        except KeyError as exception:
            raise KeyError(f"Request {request_id} not found in results {self.results}") from exception

        if "records" not in result:
            return pd.DataFrame()

        rows: list[list[Any]] = []
        column_types = [col.get("typeName") for col in result["columnMetadata"]]

        for record in result["records"]:
            row: list[Any] = [
                _connector.DataApiConnector._get_column_value(column, col_type)  # type: ignore[arg-type]
                for column, col_type in zip(record, column_types)
            ]
            rows.append(row)

        column_names: list[str] = [column["name"] for column in result["columnMetadata"]]
        dataframe = pd.DataFrame(rows, columns=column_names)
        return dataframe


def connect(
    resource_arn: str, database: str, secret_arn: str = "", boto3_session: boto3.Session | None = None, **kwargs: Any
) -> RdsDataApi:
    """Create a RDS Data API connection.

    Parameters
    ----------
    resource_arn: str
        ARN for the RDS resource.
    database: str
        Target database name.
    secret_arn: str
        The ARN for the secret to be used for authentication.
    boto3_session : boto3.Session(), optional
        The boto3 session. If `None`, the default boto3 session is used.
    **kwargs
        Any additional kwargs are passed to the underlying RdsDataApi class.

    Returns
    -------
    A RdsDataApi connection instance that can be used with `wr.rds.data_api.read_sql_query`.
    """
    return RdsDataApi(resource_arn, database, secret_arn=secret_arn, boto3_session=boto3_session, **kwargs)


def read_sql_query(sql: str, con: RdsDataApi, database: str | None = None) -> pd.DataFrame:
    """Run an SQL query on an RdsDataApi connection and return the result as a DataFrame.

    Parameters
    ----------
    sql: str
        SQL query to run.
    con: RdsDataApi
        A RdsDataApi connection instance
    database: str
        Database to run query on - defaults to the database specified by `con`.

    Returns
    -------
    A Pandas DataFrame containing the query results.
    """
    return con.execute(sql, database=database)


def _drop_table(con: RdsDataApi, table: str, database: str, transaction_id: str, sql_mode: str) -> None:
    sql = f"DROP TABLE IF EXISTS {identifier(table, sql_mode=sql_mode)}"
    _logger.debug("Drop table query:\n%s", sql)
    con.execute(sql, database=database, transaction_id=transaction_id)


def _does_table_exist(con: RdsDataApi, table: str, database: str, transaction_id: str) -> bool:
    res = con.execute(
        "SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = :table",
        parameters=[
            {
                "name": "table",
                "value": {"stringValue": table},
            },
        ],
    )
    return not res.empty


def _create_table(
    df: pd.DataFrame,
    con: RdsDataApi,
    table: str,
    database: str,
    transaction_id: str,
    mode: str,
    index: bool,
    dtype: dict[str, str] | None,
    varchar_lengths: dict[str, int] | None,
    sql_mode: str,
) -> None:
    if mode == "overwrite":
        _drop_table(con=con, table=table, database=database, transaction_id=transaction_id, sql_mode=sql_mode)
    elif _does_table_exist(con=con, table=table, database=database, transaction_id=transaction_id):
        return

    mysql_types: dict[str, str] = _data_types.database_types_from_pandas(
        df=df,
        index=index,
        dtype=dtype,
        varchar_lengths_default="TEXT",
        varchar_lengths=varchar_lengths,
        converter_func=_data_types.pyarrow2mysql,
    )
    cols_str: str = "".join([f"{identifier(k, sql_mode=sql_mode)} {v},\n" for k, v in mysql_types.items()])[:-2]
    sql = f"CREATE TABLE IF NOT EXISTS {identifier(table, sql_mode=sql_mode)} (\n{cols_str})"

    _logger.debug("Create table query:\n%s", sql)
    con.execute(sql, database=database, transaction_id=transaction_id)


def _create_value_dict(  # noqa: PLR0911
    value: Any,
) -> tuple[dict[str, Any], str | None]:
    if value is None or pd.isnull(value):
        return {"isNull": True}, None

    if isinstance(value, bool):
        return {"booleanValue": value}, None

    if isinstance(value, int):
        return {"longValue": value}, None

    if isinstance(value, float):
        return {"doubleValue": value}, None

    if isinstance(value, str):
        return {"stringValue": value}, None

    if isinstance(value, bytes):
        return {"blobValue": value}, None

    if isinstance(value, dt.datetime):
        return {"stringValue": str(value)}, "TIMESTAMP"

    if isinstance(value, dt.date):
        return {"stringValue": str(value)}, "DATE"

    if isinstance(value, dt.time):
        return {"stringValue": str(value)}, "TIME"

    if isinstance(value, Decimal):
        return {"stringValue": str(value)}, "DECIMAL"

    raise exceptions.InvalidArgumentType(f"Value {value} not supported.")


def _generate_parameters(columns: list[str], values: list[Any]) -> list[dict[str, Any]]:
    parameter_list = []

    for col, value in zip(columns, values):
        value, type_hint = _create_value_dict(value)  # noqa: PLW2901

        parameter = {
            "name": col,
            "value": value,
        }
        if type_hint:
            parameter["typeHint"] = type_hint

        parameter_list.append(parameter)

    return parameter_list


def _generate_parameter_sets(df: pd.DataFrame) -> list[list[dict[str, Any]]]:
    parameter_sets = []

    columns = df.columns.tolist()
    for values in df.values.tolist():
        parameter_sets.append(_generate_parameters(columns, values))

    return parameter_sets


def to_sql(
    df: pd.DataFrame,
    con: RdsDataApi,
    table: str,
    database: str,
    mode: Literal["append", "overwrite"] = "append",
    index: bool = False,
    dtype: dict[str, str] | None = None,
    varchar_lengths: dict[str, int] | None = None,
    use_column_names: bool = False,
    chunksize: int = 200,
    sql_mode: str = "mysql",
) -> None:
    """
    Insert data using an SQL query on a Data API connection.

    Parameters
    ----------
    df: pandas.DataFrame
        `Pandas DataFrame <https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html>`_
    con: RdsDataApi
        A RdsDataApi connection instance
    database: str
        Database to run query on - defaults to the database specified by `con`.
    table: str
        Table name
    mode: str
        `append` (inserts new records into table), `overwrite` (drops table and recreates)
    index: bool
        True to store the DataFrame index as a column in the table,
        otherwise False to ignore it.
    dtype: Dict[str, str], optional
        Dictionary of columns names and MySQL types to be casted.
        Useful when you have columns with undetermined or mixed data types.
        (e.g. ```{'col name': 'TEXT', 'col2 name': 'FLOAT'}```)
    varchar_lengths : Dict[str, int], optional
        Dict of VARCHAR length by columns. (e.g. ```{"col1": 10, "col5": 200}```).
    use_column_names: bool
        If set to True, will use the column names of the DataFrame for generating the INSERT SQL Query.
        E.g. If the DataFrame has two columns `col1` and `col3` and `use_column_names` is True, data will only be
        inserted into the database columns `col1` and `col3`.
    chunksize: int
        Number of rows which are inserted with each SQL query. Defaults to inserting 200 rows per query.
    sql_mode: str
        "mysql" for default MySQL identifiers (backticks) or "ansi" for ANSI-compatible identifiers (double quotes).
    """
    if df.empty is True:
        raise exceptions.EmptyDataFrame("DataFrame cannot be empty.")

    _databases.validate_mode(mode=mode, allowed_modes=["append", "overwrite"])

    transaction_id: str | None = None
    try:
        transaction_id = con.begin_transaction(database=database)

        _create_table(
            df=df,
            con=con,
            table=table,
            database=database,
            transaction_id=transaction_id,
            mode=mode,
            index=index,
            dtype=dtype,
            varchar_lengths=varchar_lengths,
            sql_mode=sql_mode,
        )

        if index:
            df = df.reset_index(level=df.index.names)

        if use_column_names:
            insertion_columns = "(" + ", ".join([f"{identifier(col, sql_mode=sql_mode)}" for col in df.columns]) + ")"
        else:
            insertion_columns = ""

        placeholders = ", ".join([f":{col}" for col in df.columns])

        sql = f"INSERT INTO {identifier(table, sql_mode=sql_mode)} {insertion_columns} VALUES ({placeholders})"
        parameter_sets = _generate_parameter_sets(df)

        for parameter_sets_chunk in _utils.chunkify(parameter_sets, max_length=chunksize):
            con.batch_execute(
                sql=sql,
                database=database,
                transaction_id=transaction_id,
                parameter_sets=parameter_sets_chunk,
            )

    except Exception as ex:
        if transaction_id:
            con.rollback_transaction(transaction_id=transaction_id)
        _logger.error(ex)
        raise

    else:
        con.commit_transaction(transaction_id=transaction_id)
