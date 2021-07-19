"""RDS Data API Connector."""
from __future__ import annotations

import uuid
from typing import Any, Dict, List, Optional

import boto3
import pandas as pd

from awswrangler.data_api import connector


def connect(resource_arn: str, database: str, secret_arn: str = "") -> RdsDataApi:
    """Create a RDS Data API connection.

    Parameters
    ----------
    resource_arn: str
        ARN for the RDS resource.
    database: str
        Target database name.
    secret_arn: str
        The ARN for the secret to be used for authentication.

    Returns
    -------
    A RdsDataApi connection instance that can be used with `wr.rds.data_api.read_sql_query`.
    """
    return RdsDataApi(resource_arn, database, secret_arn=secret_arn)


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

    def __init__(self, resource_arn: str, database: str, secret_arn: str = "") -> None:
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
        super().__init__(self.client)

    def _execute_statement(self, sql: str, database: Optional[str] = None) -> str:
        if database is None:
            database = self.database

        response: Dict[str, Any] = self.client.execute_statement(
            dbClusterOrInstanceArn=self.resource_arn,
            database=database,
            sqlStatements=sql,
            awsSecretStoreArn=self.secret_arn,
        )

        request_id: str = uuid.uuid4().hex
        self.results[request_id] = response
        return request_id

    def _get_statement_result(self, request_id: str) -> pd.DataFrame:
        try:
            result = self.results.pop(request_id)
        except KeyError as exception:
            raise KeyError(f"Request {request_id} not found in results {self.results}") from exception

        if len(result["records"]) == 0:
            return pd.DataFrame()

        rows: List[List[Any]] = []
        for record in result["records"]:
            row: List[Any] = [connector.DataApiConnector._get_column_value(column) for column in record]
            rows.append(row)

        column_metadata = result["ColumnMetadata"]
        column_names: List[str] = [column["name"] for column in column_metadata]
        dataframe = pd.DataFrame(rows, columns=column_names)
        return dataframe
