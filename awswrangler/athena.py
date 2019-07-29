from time import sleep
import logging

from awswrangler.exceptions import UnsupportedType

logger = logging.getLogger(__name__)

QUERY_WAIT_POLLING_DELAY = 0.2  # MILLISECONDS


class Athena:
    def __init__(self, session):
        self._session = session
        self._client_athena = session.boto3_session.client(
            service_name="athena", config=session.botocore_config)

    def get_query_columns_metadata(self, query_execution_id):
        response = self._client_athena.get_query_results(
            QueryExecutionId=query_execution_id, MaxResults=1)
        col_info = response["ResultSet"]["ResultSetMetadata"]["ColumnInfo"]
        return {x["Name"]: x["Type"] for x in col_info}

    @staticmethod
    def _type_athena2pandas(dtype):
        dtype = dtype.lower()
        if dtype in ["int", "integer", "bigint", "smallint", "tinyint"]:
            return "Int64"
        elif dtype in ["float", "double", "real"]:
            return "float64"
        elif dtype == "boolean":
            return "bool"
        elif dtype in ["string", "char", "varchar", "array", "row", "map"]:
            return "object"
        elif dtype in ["timestamp", "date"]:
            return "datetime64"
        else:
            raise UnsupportedType(f"Unsupported Athena type: {dtype}")

    def get_query_dtype(self, query_execution_id):
        cols_metadata = self.get_query_columns_metadata(
            query_execution_id=query_execution_id)
        dtype = {}
        parse_dates = []
        for col_name, col_type in cols_metadata.items():
            ptype = Athena._type_athena2pandas(dtype=col_type)
            if ptype == "datetime64":
                parse_dates.append(col_name)
            else:
                dtype[col_name] = ptype
        logger.debug(f"dtype: {dtype}")
        logger.debug(f"parse_dates: {parse_dates}")
        return dtype, parse_dates

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
