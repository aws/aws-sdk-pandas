"""Data API Connector base class."""
from typing import Any, Optional

import pandas as pd

from awswrangler.data_api import waiter


class DataApiConnector:
    """Base class for Data API (RDS, Redshift, etc.) connectors."""

    def __init__(self, client: Any):
        self.client = client
        self.waiter = waiter.DataApiWaiter(client)

    def execute(self, sql: str, database: Optional[str] = None) -> pd.DataFrame:
        """Executes SQL statement against Data API Service.

        Parameters
        ----------
        sql: str
            SQL statement to execute.

        Returns
        -------
        A Pandas DataFrame containing the execution results.
        """
        request_id: str = self._execute_statement(sql, database=database)
        self.waiter.wait(request_id)
        return self._get_statement_result(request_id)

    def _execute_statement(self, sql: str, database: Optional[str] = None) -> str:
        raise NotImplementedError()

    def _get_statement_result(self, request_id: str) -> pd.DataFrame:
        raise NotImplementedError()
