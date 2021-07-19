"""Redshift Data API Connector."""
from __future__ import annotations

import time
from typing import Any, Dict, List, Optional

import boto3
import pandas as pd

from awswrangler.data_api import connector


def connect(cluster_id: str, database: str, secret_arn: str = "", db_user: str = "") -> RedshiftDataApi:
    """Create a Redshift Data API connection.

    Parameters
    ----------
    cluster_id: str
        Id for the target Redshift cluster.
    database: str
        Target database name.
    secret_arn: str
        The ARN for the secret to be used for authentication - only required if `db_user` not provided.
    db_user: str
        The database user to generate temporary credentials for - only required if `secret_arn` not provided.

    Returns
    -------
    A RedshiftDataApi connection instance that can be used with `wr.redshift.data_api.read_sql_query`.
    """
    return RedshiftDataApi(cluster_id, database, secret_arn=secret_arn, db_user=db_user)


def read_sql_query(sql: str, con: RedshiftDataApi, database: Optional[str] = None) -> pd.DataFrame:
    """Runs an SQL query on a RedshiftDataApi connection and returns the result as a dataframe.

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


class RedshiftDataApi(connector.DataApiConnector):
    """Provides access to a Redshift cluster via the Data API."""

    def __init__(self, cluster_id: str, database: str, secret_arn: str = "", db_user: str = "") -> None:
        """
        Parameters
        ----------
        cluster_id: str
            Id for the target Redshift cluster.
        database: str
            Target database name.
        secret_arn: str
            The ARN for the secret to be used for authentication - only required if `db_user` not provided.
        db_user: str
            The database user to generate temporary credentials for - only required if `secret_arn` not provided.
        """
        self.cluster_id = cluster_id
        self.database = database
        self.secret_arn = secret_arn
        self.db_user = db_user
        self.client = boto3.client("redshift-data")
        self.waiter = RedshiftDataApiWaiter(self.client)
        super().__init__(self.client)

    def _validate_auth_method(self) -> None:
        if self.secret_arn == "" and self.db_user == "":
            raise ValueError("Either `secret_arn` or `db_user` must be set for authentication")

    def _execute_statement(self, sql: str, database: Optional[str] = None) -> str:
        self._validate_auth_method()
        credentials = {"SecretArn": self.secret_arn}
        if self.db_user:
            credentials = {"DbUser": self.db_user}

        if database is None:
            database = self.database

        response: Dict[str, Any] = self.client.execute_statement(
            ClusterIdentifier=self.cluster_id,
            Database=database,
            Sql=sql,
            **credentials,
        )
        return str(response["Id"])

    def _get_statement_result(self, request_id: str) -> pd.DataFrame:
        self.waiter.wait(request_id)
        response: Dict[str, Any]
        response = self.client.describe_statement(Id=request_id)
        if not response["HasResultSet"]:
            return pd.DataFrame()

        paginator = self.client.get_paginator("get_statement_result")
        response_iterator = paginator.paginate(Id=request_id)

        rows: List[List[Any]] = []
        column_metadata: List[Dict[str, str]]
        for response in response_iterator:
            column_metadata = response["ColumnMetadata"]
            for record in response["Records"]:
                row: List[Any] = [connector.DataApiConnector._get_column_value(column) for column in record]
                rows.append(row)

        print(column_metadata)
        column_names: List[str] = [column["name"] for column in column_metadata]
        dataframe = pd.DataFrame(rows, columns=column_names)
        return dataframe


class RedshiftDataApiWaiter:
    """Waits for a  DescribeStatement call to return a completed status.

    Parameters
    ----------
    client:
        A Boto client with a `describe_statement` function, such as 'redshift-data'
    sleep: float
        Number of seconds to sleep between tries.
    backoff: float
        Factor by which to increase the sleep between tries.
    retries: int
        Maximum number of tries.
    """

    def __init__(self, client: Any, sleep: float = 1.0, backoff: float = 1.5, retries: int = 4) -> None:
        self.client = client
        self.sleep = sleep
        self.backoff = backoff
        self.retries = retries

    def wait(self, request_id: str) -> bool:
        """Waits for the `describe_statement` function of self.client to return a completed status.

        Parameters
        ----------
        request_id:
            The execution id to check the status for.

        Returns
        -------
        True if the execution finished without error.
        Raises RedshiftDataApiExecutionFailedException if FAILED or ABORTED.
        Raises RedshiftDataApiExecutionTimeoutException if retries exceeded before completion.
        """

        sleep: float = self.sleep
        total_sleep: float = 0
        total_tries: int = 0
        while total_tries <= self.retries:
            response: Dict[str, Any] = self.client.describe_statement(Id=request_id)
            status: str = response["Status"]
            if status == "FINISHED":
                return True
            if status in ["ABORTED", "FAILED"]:
                raise RedshiftDataApiFailedException(f"Request {request_id} failed with status {status}")
            sleep = sleep * self.backoff
            total_tries += 1
            time.sleep(sleep)
            total_sleep += sleep
        raise RedshiftDataApiTimeoutException(
            f"Request {request_id} timed out after {total_tries} tries and {total_sleep}s total sleep"
        )


class RedshiftDataApiFailedException(Exception):
    """Indicates a statement execution was aborted or failed."""


class RedshiftDataApiTimeoutException(Exception):
    """Indicates a statement execution did not complete in the expected wait time."""
