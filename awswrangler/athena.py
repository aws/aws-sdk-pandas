from time import sleep

QUERY_WAIT_POLLING_DELAY = 0.2  # MILLISECONDS


class Athena:
    def __init__(self, session):
        self._session = session

    def run_query(self, query, database, s3_output):
        client = self._session.boto3_session.client("athena")
        response = client.start_query_execution(
            QueryString=query,
            QueryExecutionContext={"Database": database},
            ResultConfiguration={"OutputLocation": s3_output},
        )
        return response["QueryExecutionId"]

    def wait_query(self, query_execution_id):
        client = self._session.boto3_session.client("athena")
        final_states = ["FAILED", "SUCCEEDED", "CANCELLED"]
        response = client.get_query_execution(QueryExecutionId=query_execution_id)
        while (
            response.get("QueryExecution").get("Status").get("State")
            not in final_states
        ):
            sleep(QUERY_WAIT_POLLING_DELAY)
            response = client.get_query_execution(QueryExecutionId=query_execution_id)
        return response
