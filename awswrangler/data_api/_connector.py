"""Data API Connector base class."""

from __future__ import annotations

import datetime as dt
from abc import ABC, abstractmethod
from dataclasses import dataclass
from decimal import Decimal
from types import TracebackType
from typing import Any

import awswrangler.pandas as pd


class DataApiConnector(ABC):
    """Base class for Data API (RDS, Redshift, etc.) connectors."""

    def execute(
        self,
        sql: str,
        database: str | None = None,
        transaction_id: str | None = None,
        parameters: list[dict[str, Any]] | None = None,
    ) -> pd.DataFrame:
        """Execute SQL statement against a Data API Service.

        Parameters
        ----------
        sql: str
            SQL statement to execute.

        Returns
        -------
        A Pandas DataFrame containing the execution results.
        """
        request_id: str = self._execute_statement(
            sql, database=database, transaction_id=transaction_id, parameters=parameters
        )
        return self._get_statement_result(request_id)

    def batch_execute(
        self,
        sql: str | list[str],
        database: str | None = None,
        transaction_id: str | None = None,
        parameter_sets: list[list[dict[str, Any]]] | None = None,
    ) -> None:
        """Batch execute SQL statements against a Data API Service.

        Parameters
        ----------
        sql: str
            SQL statement to execute.
        """
        self._batch_execute_statement(
            sql, database=database, transaction_id=transaction_id, parameter_sets=parameter_sets
        )

    def __enter__(self) -> "DataApiConnector":
        return self

    @abstractmethod
    def close(self) -> None:
        """Close underlying endpoint connections."""
        pass

    def __exit__(
        self,
        exception_type: type[BaseException] | None,
        exception_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> bool | None:
        self.close()
        return None

    @abstractmethod
    def begin_transaction(self, database: str | None = None, schema: str | None = None) -> str:
        pass

    @abstractmethod
    def commit_transaction(self, transaction_id: str) -> str:
        pass

    @abstractmethod
    def rollback_transaction(self, transaction_id: str) -> str:
        pass

    @abstractmethod
    def _execute_statement(
        self,
        sql: str,
        database: str | None = None,
        transaction_id: str | None = None,
        parameters: list[dict[str, Any]] | None = None,
    ) -> str:
        pass

    @abstractmethod
    def _batch_execute_statement(
        self,
        sql: str | list[str],
        database: str | None = None,
        transaction_id: str | None = None,
        parameter_sets: list[list[dict[str, Any]]] | None = None,
    ) -> str:
        pass

    @abstractmethod
    def _get_statement_result(self, request_id: str) -> pd.DataFrame:
        pass

    @staticmethod
    def _get_column_value(  # noqa: PLR0911
        column_value: dict[str, Any], col_type: str | None = None
    ) -> Any:
        """Return the first non-null key value for a given dictionary.

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
            https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/rds-data.html#RDSDataService.Client.execute_statement
        """
        for key in column_value:
            if column_value[key] is not None:
                if (key == "isNull") and column_value[key]:
                    return None
                if key == "arrayValue":
                    raise ValueError(f"arrayValue not supported yet - could not extract {column_value[key]}")

                if key == "stringValue":
                    if col_type == "DATETIME":
                        return dt.datetime.strptime(column_value[key], "%Y-%m-%d %H:%M:%S")

                    if col_type == "DATE":
                        return dt.datetime.strptime(column_value[key], "%Y-%m-%d").date()

                    if col_type == "TIME":
                        return dt.datetime.strptime(column_value[key], "%H:%M:%S").time()

                    if col_type == "DECIMAL":
                        return Decimal(column_value[key])

                return column_value[key]
        return None


@dataclass
class WaitConfig:
    """Holds standard wait configuration values."""

    sleep: float
    backoff: float
    retries: int
