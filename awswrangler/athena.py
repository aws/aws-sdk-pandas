from time import sleep
import logging

logger = logging.getLogger(__name__)

QUERY_WAIT_POLLING_DELAY = 0.2  # MILLISECONDS


class Athena:
    def __init__(self, session):
        self._session = session
        self._client_athena = session.boto3_session.client(
            service_name="athena", config=session.botocore_config)

    def run_query(self, query, database, s3_output):
        response = self._client_athena.start_query_execution(
            QueryString=query,
            QueryExecutionContext={"Database": database},
            ResultConfiguration={"OutputLocation": s3_output},
        )
        return response["QueryExecutionId"]

    def wait_query(self, query_execution_id):
        final_states = ["FAILED", "SUCCEEDED", "CANCELLED"]
        response = self._client_athena.get_query_execution(
            QueryExecutionId=query_execution_id)
        while (response.get("QueryExecution").get("Status").get("State") not in
               final_states):
            sleep(QUERY_WAIT_POLLING_DELAY)
            response = self._client_athena.get_query_execution(
                QueryExecutionId=query_execution_id)
        return response
