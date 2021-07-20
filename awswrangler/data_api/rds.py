"""RDS Data API Connector."""
from __future__ import annotations

import logging
import time
import uuid
from typing import Any, Dict, List, Optional

import boto3
import pandas as pd

from awswrangler.data_api import connector


def connect(resource_arn: str, database: str, secret_arn: str = "", **kwargs: Any) -> RdsDataApi:
    """Create a RDS Data API connection.

    Parameters
    ----------
    resource_arn: str
        ARN for the RDS resource.
    database: str
        Target database name.
    secret_arn: str
        The ARN for the secret to be used for authentication.
    **kwargs
        Any additional kwargs are passed to the underlying RdsDataApi class.

    Returns
    -------
    A RdsDataApi connection instance that can be used with `wr.rds.data_api.read_sql_query`.
    """
    return RdsDataApi(resource_arn, database, secret_arn=secret_arn, **kwargs)


def read_sql_query(sql: str, con: RdsDataApi, database: Optional[str] = None) -> pd.DataFrame:
    """Runs an SQL query on an RdsDataApi connection and returns the result as a dataframe.

    Parameters
    ----------
    sql: str
        SQL query to run.
    database: str
        Database to run query on - defaults to the database specified by `con`.

    Returns
    -------
    A Pandas dataframe containing the query results.
    """
    return con.execute(sql, database=database)


class RdsDataApi(connector.DataApiConnector):
    """Provides access to the RDS Data API."""

    def __init__(
        self,
        resource_arn: str,
        database: str,
        secret_arn: str = "",
        max_tries: int = 30,
        sleep: float = 0.5,
        backoff: float = 1.0,
    ) -> None:
        """
        Parameters
        ----------
        resource_arn: str
            ARN for the RDS resource.
        database: str
            Target database name.
        secret_arn: str
            The ARN for the secret to be used for authentication.
        """
        self.resource_arn = resource_arn
        self.database = database
        self.secret_arn = secret_arn
        self.client = boto3.client("rds-data")
        self.results: Dict[str, Dict[str, Any]] = {}
        self.max_tries: int = max_tries
        self.sleep: float = sleep
        self.backoff: float = backoff
        logger: logging.Logger = logging.getLogger("RdsDataApi")
        super().__init__(self.client, logger)

    def _execute_statement(self, sql: str, database: Optional[str] = None) -> str:
        if database is None:
            database = self.database

        sleep: float = self.sleep
        total_tries: int = 0
        total_sleep: float = 0
        response: Optional[Dict[str, Any]] = None
        while total_tries < self.max_tries:
            try:
                response = self.client.execute_statement(
                    resourceArn=self.resource_arn,
                    database=database,
                    sql=sql,
                    secretArn=self.secret_arn,
                    includeResultMetadata=True,
                )
                break
            except self.client.exceptions.BadRequestException as exception:
                self.logger.info("BadRequestException occurred %s", exception)
                self.logger.info(
                    "Cluster may be paused - sleeping for %s seconds for a total of %s seconds before retrying",
                    sleep,
                    total_sleep,
                )
                time.sleep(sleep)
                total_sleep += sleep
                total_tries += 1
                sleep *= self.backoff

        if response is None:
            raise self.client.exceptions.BadRequestException(
                f"BadRequestException received after {self.max_tries} tries and sleeping {total_sleep}s"
            )

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
            row: List[Any] = [connector.DataApiConnector._get_column_value(column) for column in record]
            rows.append(row)

        column_names: List[str] = [column["name"] for column in result["columnMetadata"]]
        dataframe = pd.DataFrame(rows, columns=column_names)
        return dataframe
