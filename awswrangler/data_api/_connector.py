"""Data API Connector base class."""
import logging
from typing import Any, Dict, Optional

import pandas as pd


class DataApiConnector:
    """Base class for Data API (RDS, Redshift, etc.) connectors."""

    def __init__(self, client: Any, logger: logging.Logger):
        self.client = client
        self.logger: logging.Logger = logger

    def execute(self, sql: str, database: Optional[str] = None) -> pd.DataFrame:
        """Execute SQL statement against a Data API Service.

        Parameters
        ----------
        sql: str
            SQL statement to execute.

        Returns
        -------
        A Pandas DataFrame containing the execution results.
        """
        request_id: str = self._execute_statement(sql, database=database)
        return self._get_statement_result(request_id)

    def _execute_statement(self, sql: str, database: Optional[str] = None) -> str:
        raise NotImplementedError()

    def _get_statement_result(self, request_id: str) -> pd.DataFrame:
        raise NotImplementedError()

    @staticmethod
    def _get_column_value(column_value: Dict[str, Any]) -> Any:
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
                return column_value[key]
        return None


class WaitConfig:
    """Holds standard wait configuration values."""

    def __init__(self, sleep: float, backoff: float, retries: int) -> None:
        self.sleep = sleep
        self.backoff = backoff
        self.retries = retries
