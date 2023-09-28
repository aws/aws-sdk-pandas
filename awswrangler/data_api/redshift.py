"""Redshift Data API Connector."""
import logging
import time
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Union

import boto3

import awswrangler.pandas as pd
from awswrangler import _utils, exceptions
from awswrangler.data_api import _connector

if TYPE_CHECKING:
    from mypy_boto3_redshift_data.client import RedshiftDataAPIServiceClient
    from mypy_boto3_redshift_data.type_defs import ColumnMetadataTypeDef


_logger = logging.getLogger(__name__)


class RedshiftDataApi(_connector.DataApiConnector):
    """Provides access to a Redshift cluster via the Data API.

    Note
    ----
    When connecting to a standard Redshift cluster, `cluster_id` is used.
    When connecting to Redshift Serverless, `workgroup_name` is used. These two arguments are mutually exclusive.

    Parameters
    ----------
    cluster_id: str
        Id for the target Redshift cluster - only required if `workgroup_name` not provided.
    database: str
        Target database name.
    workgroup_name: str
        Name for the target serverless Redshift workgroup - only required if `cluster_id` not provided.
    secret_arn: str
        The ARN for the secret to be used for authentication - only required if `db_user` not provided.
    db_user: str
        The database user to generate temporary credentials for - only required if `secret_arn` not provided.
    sleep: float
        Number of seconds to sleep between result fetch attempts - defaults to 0.25.
    backoff: float
        Factor by which to increase the sleep between result fetch attempts - defaults to 1.5.
    retries: int
        Maximum number of result fetch attempts - defaults to 15.
    boto3_session : boto3.Session(), optional
        The boto3 session. If `None`, the default boto3 session is used.
    """

    def __init__(
        self,
        cluster_id: str = "",
        database: str = "",
        workgroup_name: str = "",
        secret_arn: str = "",
        db_user: str = "",
        sleep: float = 0.25,
        backoff: float = 1.5,
        retries: int = 15,
        boto3_session: Optional[boto3.Session] = None,
    ) -> None:
        super().__init__()

        self.client = _utils.client(service_name="redshift-data", session=boto3_session)

        self.cluster_id = cluster_id
        self.database = database
        self.workgroup_name = workgroup_name
        self.secret_arn = secret_arn
        self.db_user = db_user
        self.waiter = RedshiftDataApiWaiter(self.client, sleep, backoff, retries)

    def close(self) -> None:
        """Close underlying endpoint connections."""
        self.client.close()

    def begin_transaction(self, database: Optional[str] = None, schema: Optional[str] = None) -> str:
        """Start an SQL transaction."""
        raise NotImplementedError("Redshift Data API does not support transactions.")

    def commit_transaction(self, transaction_id: str) -> str:
        """Commit an SQL transaction."""
        raise NotImplementedError("Redshift Data API does not support transactions.")

    def rollback_transaction(self, transaction_id: str) -> str:
        """Roll back an SQL transaction."""
        raise NotImplementedError("Redshift Data API does not support transactions.")

    def _validate_redshift_target(self) -> None:
        if not self.database:
            raise ValueError("`database` must be set for connection")
        if not self.cluster_id and not self.workgroup_name:
            raise ValueError("Either `cluster_id` or `workgroup_name`(Redshift Serverless) must be set for connection")

    def _validate_auth_method(self) -> None:
        if not self.workgroup_name and not self.secret_arn and not self.db_user and not self.cluster_id:
            raise exceptions.InvalidArgumentCombination(
                "Either `secret_arn`, `workgroup_name`, `db_user`, or `cluster_id` must be set for authentication."
            )
        if self.db_user and self.secret_arn:
            raise exceptions.InvalidArgumentCombination("Only one of `secret_arn` or `db_user` is allowed.")

    def _execute_statement(
        self,
        sql: str,
        database: Optional[str] = None,
        transaction_id: Optional[str] = None,
        parameters: Optional[List[Dict[str, Any]]] = None,
    ) -> str:
        if transaction_id:
            exceptions.InvalidArgument("`transaction_id` not supported for Redshift Data API")
        if parameters:
            exceptions.InvalidArgument("`parameters` not supported for Redshift Data API")

        self._validate_redshift_target()
        self._validate_auth_method()
        args = {}
        if self.secret_arn:
            args["SecretArn"] = self.secret_arn
        if self.db_user:
            args["DbUser"] = self.db_user

        if database is None:
            database = self.database

        if self.cluster_id:
            args["ClusterIdentifier"] = self.cluster_id
        if self.workgroup_name:
            args["WorkgroupName"] = self.workgroup_name

        _logger.debug("Executing %s", sql)
        response = self.client.execute_statement(
            Database=database,
            Sql=sql,
            **args,  # type: ignore[arg-type]
        )
        return response["Id"]

    def _batch_execute_statement(
        self,
        sql: Union[str, List[str]],
        database: Optional[str] = None,
        transaction_id: Optional[str] = None,
        parameter_sets: Optional[List[List[Dict[str, Any]]]] = None,
    ) -> str:
        raise NotImplementedError("Batch execute statement not support for Redshift Data API.")

    def _get_statement_result(self, request_id: str) -> pd.DataFrame:
        self.waiter.wait(request_id)
        describe_response = self.client.describe_statement(Id=request_id)
        if not describe_response["HasResultSet"]:
            return pd.DataFrame()

        paginator = self.client.get_paginator("get_statement_result")
        response_iterator = paginator.paginate(Id=request_id)

        rows: List[List[Any]] = []
        column_metadata: List["ColumnMetadataTypeDef"]
        for response in response_iterator:
            column_metadata = response["ColumnMetadata"]
            for record in response["Records"]:
                row: List[Any] = [
                    _connector.DataApiConnector._get_column_value(column)  # type: ignore[arg-type]  # pylint: disable=protected-access
                    for column in record
                ]
                rows.append(row)

        column_names: List[str] = [column["name"] for column in column_metadata]
        dataframe = pd.DataFrame(rows, columns=column_names)
        return dataframe


class RedshiftDataApiWaiter:
    """Waits for a DescribeStatement call to return a completed status.

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

    def __init__(self, client: "RedshiftDataAPIServiceClient", sleep: float, backoff: float, retries: int) -> None:
        self.client = client
        self.wait_config = _connector.WaitConfig(sleep, backoff, retries)

    def wait(self, request_id: str) -> bool:
        """Wait for the `describe_statement` function of self.client to return a completed status.

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
        sleep: float = self.wait_config.sleep
        total_sleep: float = 0
        total_tries: int = 0
        while total_tries <= self.wait_config.retries:
            response = self.client.describe_statement(Id=request_id)
            status: str = response["Status"]
            if status == "FINISHED":
                return True
            if status in ["ABORTED", "FAILED"]:
                error = response["Error"]
                raise RedshiftDataApiFailedException(
                    f"Request {request_id} failed with status {status} and error {error}"
                )
            _logger.debug("Statement execution status %s - sleeping for %s seconds", status, sleep)
            time.sleep(sleep)
            sleep = sleep * self.wait_config.backoff
            total_tries += 1
            total_sleep += sleep
        raise RedshiftDataApiTimeoutException(
            f"Request {request_id} timed out after {total_tries} tries and {total_sleep}s total sleep"
        )


class RedshiftDataApiFailedException(Exception):
    """Indicates a statement execution was aborted or failed."""


class RedshiftDataApiTimeoutException(Exception):
    """Indicates a statement execution did not complete in the expected wait time."""


def connect(
    cluster_id: str = "",
    database: str = "",
    workgroup_name: str = "",
    secret_arn: str = "",
    db_user: str = "",
    boto3_session: Optional[boto3.Session] = None,
    **kwargs: Any,
) -> RedshiftDataApi:
    """Create a Redshift Data API connection.

    Note
    ----
    When connecting to a standard Redshift cluster, `cluster_id` is used.
    When connecting to Redshift Serverless, `workgroup_name` is used. These two arguments are mutually exclusive.

    Parameters
    ----------
    cluster_id: str
        Id for the target Redshift cluster - only required if `workgroup_name` not provided.
    database: str
        Target database name.
    workgroup_name: str
        Name for the target serverless Redshift workgroup - only required if `cluster_id` not provided.
    secret_arn: str
        The ARN for the secret to be used for authentication - only required if `db_user` not provided.
    db_user: str
        The database user to generate temporary credentials for - only required if `secret_arn` not provided.
    boto3_session : boto3.Session(), optional
        The boto3 session. If `None`, the default boto3 session is used.
    **kwargs
        Any additional kwargs are passed to the underlying RedshiftDataApi class.

    Returns
    -------
    A RedshiftDataApi connection instance that can be used with `wr.redshift.data_api.read_sql_query`.
    """
    return RedshiftDataApi(
        cluster_id=cluster_id,
        database=database,
        workgroup_name=workgroup_name,
        secret_arn=secret_arn,
        db_user=db_user,
        boto3_session=boto3_session,
        **kwargs,
    )


def read_sql_query(sql: str, con: RedshiftDataApi, database: Optional[str] = None) -> pd.DataFrame:
    """Run an SQL query on a RedshiftDataApi connection and return the result as a DataFrame.

    Parameters
    ----------
    sql: str
        SQL query to run.
    con: RedshiftDataApi
        A RedshiftDataApi connection instance
    database: str
        Database to run query on - defaults to the database specified by `con`.

    Returns
    -------
    A Pandas DataFrame containing the query results.
    """
    return con.execute(sql, database=database)
