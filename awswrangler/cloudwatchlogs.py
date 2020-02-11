"""CloudWatchLogs Module."""

from datetime import datetime
from logging import Logger, getLogger
from time import sleep
from typing import TYPE_CHECKING

from boto3 import client  # type: ignore

from awswrangler.exceptions import QueryCancelled, QueryFailed

if TYPE_CHECKING:
    from awswrangler.session import Session

logger: Logger = getLogger(__name__)

QUERY_WAIT_POLLING_DELAY: float = 0.2  # MILLISECONDS


class CloudWatchLogs:
    """CloudWatchLogs Class."""
    def __init__(self, session: "Session"):
        """
        Cloudwatchlogs Class Constructor.

        Don't use it directly, call through a Session().
        e.g. wr.redshift.your_method()

        :param session: awswrangler.Session()
        """
        self._session: "Session" = session
        self._client_logs: client = session.boto3_session.client(service_name="logs", config=session.botocore_config)

    def start_query(self,
                    query: str,
                    log_group_names,
                    start_time=datetime(year=1970, month=1, day=1),
                    end_time=datetime.utcnow(),
                    limit=None):
        """
        Run a query against AWS CloudWatchLogs Insights and wait the results.

        https://docs.aws.amazon.com/AmazonCloudWatch/latest/logs/CWL_QuerySyntax.html

        :param query: The query string to use.
        :param log_group_names: The list of log groups to be queried. You can include up to 20 log groups.
        :param start_time: The beginning of the time range to query (datetime.datetime object)
        :param end_time: The end of the time range to query (datetime.datetime object)
        :param limit: The maximum number of log events to return in the query.
        :return: Query ID
        """
        logger.debug(f"log_group_names: {log_group_names}")
        start_timestamp = int(1000 * start_time.timestamp())
        end_timestamp = int(1000 * end_time.timestamp())
        logger.debug(f"start_timestamp: {start_timestamp}")
        logger.debug(f"end_timestamp: {end_timestamp}")
        args = {
            "logGroupNames": log_group_names,
            "startTime": start_timestamp,
            "endTime": end_timestamp,
            "queryString": query
        }
        if limit:
            args["limit"] = limit
        response = self._client_logs.start_query(**args)
        return response["queryId"]

    def wait_query(self, query_id):
        """
        Wait query ends.

        :param query_id: Query ID
        :return: Query results
        """
        final_states = ["Complete", "Failed", "Cancelled"]
        response = self._client_logs.get_query_results(queryId=query_id)
        status = response["status"]
        while status not in final_states:
            sleep(QUERY_WAIT_POLLING_DELAY)
            response = self._client_logs.get_query_results(queryId=query_id)
            status = response["status"]
        logger.debug(f"status: {status}")
        if status == "Failed":
            raise QueryFailed(f"query ID: {query_id}")
        elif status == "Cancelled":
            raise QueryCancelled(f"query ID: {query_id}")
        return response

    def query(self,
              query,
              log_group_names,
              start_time=datetime(year=1970, month=1, day=1),
              end_time=datetime.utcnow(),
              limit=None):
        """
        Run a query against AWS CloudWatchLogs Insights and wait the results.

        https://docs.aws.amazon.com/AmazonCloudWatch/latest/logs/CWL_QuerySyntax.html

        :param query: The query string to use.
        :param log_group_names: The list of log groups to be queried. You can include up to 20 log groups.
        :param start_time: The beginning of the time range to query (datetime.datetime object)
        :param end_time: The end of the time range to query (datetime.datetime object)
        :param limit: The maximum number of log events to return in the query.
        :return: Results
        """
        query_id = self.start_query(query=query,
                                    log_group_names=log_group_names,
                                    start_time=start_time,
                                    end_time=end_time,
                                    limit=limit)
        response = self.wait_query(query_id=query_id)
        return response["results"]
