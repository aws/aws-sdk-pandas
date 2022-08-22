"""RDS Data API Connector."""
import logging
import time
import uuid
from typing import Any, Dict, List, Optional

import boto3
import pandas as pd

from awswrangler import _utils
from awswrangler.data_api import connector


class RdsDataApi(connector.DataApiConnector):
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
        boto3_session: Optional[boto3.Session] = None,
    ) -> None:
        self.resource_arn = resource_arn
        self.database = database
        self.secret_arn = secret_arn
        self.wait_config = connector.WaitConfig(sleep, backoff, retries)
        self.client: boto3.client = _utils.client(service_name="rds-data", session=boto3_session)
        self.results: Dict[str, Dict[str, Any]] = {}
        logger: logging.Logger = logging.getLogger(__name__)
        super().__init__(self.client, logger)

    def _execute_statement(self, sql: str, database: Optional[str] = None) -> str:
        if database is None:
            database = self.database

        sleep: float = self.wait_config.sleep
        total_tries: int = 0
        total_sleep: float = 0
        response: Optional[Dict[str, Any]] = None
        last_exception: Optional[Exception] = None
        while total_tries < self.wait_config.retries:
            try:
                response = self.client.execute_statement(
                    resourceArn=self.resource_arn,
                    database=database,
                    sql=sql,
                    secretArn=self.secret_arn,
                    includeResultMetadata=True,
                )
                self.logger.debug(
                    "Response received after %s tries and sleeping for a total of %s seconds", total_tries, total_sleep
                )
                break
            except self.client.exceptions.BadRequestException as exception:
                last_exception = exception
                total_sleep += sleep
                self.logger.debug("BadRequestException occurred: %s", exception)
                self.logger.debug(
                    "Cluster may be paused - sleeping for %s seconds for a total of %s before retrying",
                    sleep,
                    total_sleep,
                )
                time.sleep(sleep)
                total_tries += 1
                sleep *= self.wait_config.backoff

        if response is None:
            self.logger.exception("Maximum BadRequestException retries reached for query %s", sql)
            raise last_exception  # type: ignore

        request_id: str = uuid.uuid4().hex
        self.results[request_id] = response
        return request_id

    def _get_statement_result(self, request_id: str) -> pd.DataFrame:
        try:
            result = self.results.pop(request_id)
        except KeyError as exception:
            raise KeyError(f"Request {request_id} not found in results {self.results}") from exception

        if "records" not in result:
            return pd.DataFrame()

        rows: List[List[Any]] = []
        for record in result["records"]:
            row: List[Any] = [
                connector.DataApiConnector._get_column_value(column)  # pylint: disable=protected-access
                for column in record
            ]
            rows.append(row)

        column_names: List[str] = [column["name"] for column in result["columnMetadata"]]
        dataframe = pd.DataFrame(rows, columns=column_names)
        return dataframe


def connect(
    resource_arn: str, database: str, secret_arn: str = "", boto3_session: Optional[boto3.Session] = None, **kwargs: Any
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


def read_sql_query(sql: str, con: RdsDataApi, database: Optional[str] = None) -> pd.DataFrame:
    """Run an SQL query on an RdsDataApi connection and return the result as a dataframe.

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
    A Pandas dataframe containing the query results.
    """
    return con.execute(sql, database=database)
