from time import sleep
import logging
import ast
import re
import unicodedata

from awswrangler import data_types
from awswrangler.exceptions import QueryFailed, QueryCancelled

logger = logging.getLogger(__name__)

QUERY_WAIT_POLLING_DELAY = 0.2  # MILLISECONDS


class Athena:
    def __init__(self, session):
        self._session = session
        self._client_athena = session.boto3_session.client(service_name="athena", config=session.botocore_config)

    def get_query_columns_metadata(self, query_execution_id):
        response = self._client_athena.get_query_results(QueryExecutionId=query_execution_id, MaxResults=1)
        col_info = response["ResultSet"]["ResultSetMetadata"]["ColumnInfo"]
        return {x["Name"]: x["Type"] for x in col_info}

    def get_query_dtype(self, query_execution_id):
        cols_metadata = self.get_query_columns_metadata(query_execution_id=query_execution_id)
        logger.debug(f"cols_metadata: {cols_metadata}")
        dtype = {}
        parse_timestamps = []
        parse_dates = []
        converters = {}
        for col_name, col_type in cols_metadata.items():
            pandas_type = data_types.athena2pandas(dtype=col_type)
            if pandas_type in ["datetime64", "date"]:
                parse_timestamps.append(col_name)
                if pandas_type == "date":
                    parse_dates.append(col_name)
            elif pandas_type == "literal_eval":
                converters[col_name] = ast.literal_eval
            else:
                dtype[col_name] = pandas_type
        logger.debug(f"dtype: {dtype}")
        logger.debug(f"parse_timestamps: {parse_timestamps}")
        logger.debug(f"parse_dates: {parse_dates}")
        return dtype, parse_timestamps, parse_dates, converters

    def create_athena_bucket(self):
        """
        Creates the default Athena bucket if not exists

        :return: Bucket s3 path (E.g. s3://aws-athena-query-results-ACCOUNT-REGION/)
        """
        account_id = (self._session.boto3_session.client(
            service_name="sts", config=self._session.botocore_config).get_caller_identity().get("Account"))
        session_region = self._session.boto3_session.region_name
        s3_output = f"s3://aws-athena-query-results-{account_id}-{session_region}/"
        s3_resource = self._session.boto3_session.resource("s3")
        s3_resource.Bucket(s3_output)
        return s3_output

    def run_query(self, query, database, s3_output=None, workgroup=None):
        """
        Run a SQL Query against AWS Athena

        :param query: SQL query
        :param database: AWS Glue/Athena database name
        :param s3_output: AWS S3 path
        :param workgroup: Athena workgroup (By default uses de Session() workgroup)
        :return: Query execution ID
        """
        if s3_output is None:
            s3_output = self.create_athena_bucket()
        if workgroup is None:
            workgroup = self._session.athena_workgroup
        logger.debug(f"Workgroup: {workgroup}")
        response = self._client_athena.start_query_execution(QueryString=query,
                                                             QueryExecutionContext={"Database": database},
                                                             ResultConfiguration={"OutputLocation": s3_output},
                                                             WorkGroup=workgroup)
        return response["QueryExecutionId"]

    def wait_query(self, query_execution_id):
        """
        Wait query ends

        :param query_execution_id: Query execution ID
        :return: Query response
        """
        final_states = ["FAILED", "SUCCEEDED", "CANCELLED"]
        response = self._client_athena.get_query_execution(QueryExecutionId=query_execution_id)
        state = response["QueryExecution"]["Status"]["State"]
        while state not in final_states:
            sleep(QUERY_WAIT_POLLING_DELAY)
            response = self._client_athena.get_query_execution(QueryExecutionId=query_execution_id)
            state = response["QueryExecution"]["Status"]["State"]
        logger.debug(f"state: {state}")
        logger.debug(f"StateChangeReason: {response['QueryExecution']['Status'].get('StateChangeReason')}")
        if state == "FAILED":
            raise QueryFailed(response["QueryExecution"]["Status"].get("StateChangeReason"))
        elif state == "CANCELLED":
            raise QueryCancelled(response["QueryExecution"]["Status"].get("StateChangeReason"))
        return response

    def repair_table(self, database, table, s3_output=None, workgroup=None):
        """
        Hive's metastore consistency check
        "MSCK REPAIR TABLE table;"
        Recovers partitions and data associated with partitions.
        Use this statement when you add partitions to the catalog.
        It is possible it will take some time to add all partitions.
        If this operation times out, it will be in an incomplete state
        where only a few partitions are added to the catalog.

        :param database: Glue database name
        :param table: Glue table name
        :param s3_output: AWS S3 path
        :param workgroup: Athena workgroup (By default uses de Session() workgroup)
        :return: Query execution ID
        """
        query = f"MSCK REPAIR TABLE {table};"
        query_id = self.run_query(query=query, database=database, s3_output=s3_output, workgroup=workgroup)
        self.wait_query(query_execution_id=query_id)
        return query_id

    @staticmethod
    def _normalize_name(name):
        name = "".join(c for c in unicodedata.normalize("NFD", name) if unicodedata.category(c) != "Mn")
        name = name.replace(" ", "_")
        name = name.replace("-", "_")
        name = name.replace(".", "_")
        name = re.sub("(.)([A-Z][a-z]+)", r"\1_\2", name)
        name = re.sub("([a-z0-9])([A-Z])", r"\1_\2", name)
        name = name.lower()
        return re.sub(r"(_)\1+", "\\1", name)  # remove repeated underscores

    @staticmethod
    def normalize_column_name(name):
        """
        https://docs.aws.amazon.com/athena/latest/ug/tables-databases-columns-names.html
        :param name: column name (str)
        :return: normalized column name (str)
        """
        return Athena._normalize_name(name=name)

    @staticmethod
    def normalize_table_name(name):
        """
        https://docs.aws.amazon.com/athena/latest/ug/tables-databases-columns-names.html
        :param name: table name (str)
        :return: normalized table name (str)
        """
        return Athena._normalize_name(name=name)
