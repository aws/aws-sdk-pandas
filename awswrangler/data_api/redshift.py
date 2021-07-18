"""Redshift Data API Connector."""
from __future__ import annotations

from typing import Any, Dict, List, Optional

import boto3
import pandas as pd

from awswrangler.data_api import connector


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
        super().__init__(self.client)

    def _validate_auth_method(self) -> None:
        if self.secret_arn == "" and self.db_user == "":
            raise ValueError("Either `secret_arn` or `db_user` must be set for authentication")

    def _execute_statement(self, sql: str, database: Optional[str] = None) -> str:
        self._validate_auth_method()
        credentials = {"SecretArn": self.secret_arn}
        if self.secret_arn:
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
                row: List[Any] = [RedshiftDataApi._get_column_value(column) for column in record]
                rows.append(row)

        column_names: List[str] = [column["name"] for column in column_metadata]
        dataframe = pd.DataFrame(rows, columns=column_names)
        return dataframe

    @staticmethod
    def _get_column_value(column_value: Dict[str, Any]) -> Any:
        """Returns the first non-null key value for a given dictionary.

        The key names for a given record depend on the column type: stringValue, longValue, etc.

        Therefore, a record in the response does not have consistent key names. The ColumnMetadata
        typeName information could be used to infer the key, but there is no direct mapping here
        that could be easily parsed with creating a static dictionary:
            varchar -> stringValue
            int2 -> longValue
            timestamp -> stringValue

        What has been observed is that each record appears to have a single key, so this function
        iterates over the keys and returns the first non-null value. If none are found, None is
        returned.

        Documentation:
            https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/redshift-data.html#RedshiftDataAPIService.Client.get_statement_result
        """
        for key in column_value:
            if column_value[key] is not None:
                return column_value[key]
        return None

    @staticmethod
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

    @staticmethod
    def read_sql_query(sql: str, con: RedshiftDataApi, database: Optional[str] = None) -> pd.DataFrame:
        """Runs an SQL query on a RedshiftDataApi connection and returns the results as a dataframe.

        Parameters
        ----------
        sql: str
            SQL query to run.
        database: str
            Databas to run query on - defaults to the database specified by `con`.

        Returns
        -------
        A Pandas dataframe containing the query results..
        """
        return con.execute(sql, database=database)
