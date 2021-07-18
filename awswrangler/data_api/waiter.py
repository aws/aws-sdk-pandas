"Data API waiter class to provide a synchronous execution interface." ""
import time
from typing import Any, Dict


class DataApiWaiter:
    """Waits for a Boto service DescribeStatement call to return a completed status.

    Parameters
    ----------
    client:
        A Boto client with a `describe_statement` function, such as 'redshift-data' or 'rds-data'.
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
        Raises DataApiExecutionFailedException if FAILED or ABORTED.
        Raises DataApiExecutionTimeoutException if retries exceeded before completion.
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
                raise DataApiExecutionFailedException(f"Request {request_id} failed with status {status}")
            sleep = sleep * self.backoff
            total_tries += 1
            time.sleep(sleep)
            total_sleep += sleep
        raise DataApiExecutionTimeoutException(
            f"Request {request_id} timed out after {total_tries} tries and {total_sleep}s total sleep"
        )


class DataApiExecutionFailedException(Exception):
    """Indicates a statement execution was aborted or failed."""


class DataApiExecutionTimeoutException(Exception):
    """Indicates a statement execution did not complete in the expected wait time."""
