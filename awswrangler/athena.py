"""Amazon Athena Module."""

import re
import unicodedata
from datetime import date, datetime
from logging import Logger, getLogger
from time import sleep
from typing import TYPE_CHECKING, Any, Dict, Iterator, List, Optional, Tuple

from boto3 import client  # type: ignore

from awswrangler.data_types import athena2python
from awswrangler.exceptions import QueryCancelled, QueryFailed

if TYPE_CHECKING:
    from awswrangler.session import Session

logger: Logger = getLogger(__name__)

QUERY_WAIT_POLLING_DELAY: float = 0.2  # MILLISECONDS


class Athena:
    """Amazon Athena Class."""
    def __init__(self, session: "Session"):
        """
        Amazon Athena Class Constructor.

        Don't use it directly, call through a Session().
        e.g. wr.athena.your_method()

        :param session: awswrangler.Session()
        """
        self._session: "Session" = session
        self._client_athena: client = session.boto3_session.client(service_name="athena",
                                                                   use_ssl=True,
                                                                   config=session.botocore_config)
        self._client_s3: client = session.boto3_session.client(service_name="s3",
                                                               use_ssl=True,
                                                               config=session.botocore_config)

    def get_query_columns_metadata(self, query_execution_id: str) -> Dict[str, str]:
        """
        Get the data type of all columns queried.

        :param query_execution_id: Athena query execution ID
        :return: Dictionary with all data types
        """
        response: Dict = self._client_athena.get_query_results(QueryExecutionId=query_execution_id, MaxResults=1)
        col_info: List[Dict[str, str]] = response["ResultSet"]["ResultSetMetadata"]["ColumnInfo"]
        return {x["Name"]: x["Type"] for x in col_info}

    def create_athena_bucket(self) -> str:
        """
        Create the default Athena bucket if not exists.

        :return: Bucket s3 path (E.g. s3://aws-athena-query-results-ACCOUNT-REGION/)
        """
        account_id = (self._session.boto3_session.client(
            service_name="sts", config=self._session.botocore_config).get_caller_identity().get("Account"))
        session_region = self._session.boto3_session.region_name
        s3_output = f"s3://aws-athena-query-results-{account_id}-{session_region}/"
        s3_resource = self._session.boto3_session.resource("s3")
        s3_resource.Bucket(s3_output)
        return s3_output

    def run_query(self,
                  query: str,
                  database: Optional[str] = None,
                  s3_output: Optional[str] = None,
                  workgroup: Optional[str] = None,
                  encryption: Optional[str] = None,
                  kms_key: Optional[str] = None) -> str:
        """
        Run a SQL Query against AWS Athena.

        P.S All default values will be inherited from the Session()

        :param query: SQL query
        :param database: AWS Glue/Athena database name
        :param s3_output: AWS S3 path
        :param workgroup: Athena workgroup (By default uses de Session() workgroup)
        :param encryption: None|'SSE_S3'|'SSE_KMS'|'CSE_KMS'
        :param kms_key: For SSE-KMS and CSE-KMS , this is the KMS key ARN or ID.
        :return: Query execution ID
        """
        args: Dict[str, Any] = {"QueryString": query}

        # s3_output
        if s3_output is None:
            if self._session.athena_s3_output is not None:
                s3_output = self._session.athena_s3_output
            else:
                s3_output = self.create_athena_bucket()
        args["ResultConfiguration"] = {"OutputLocation": s3_output}

        # encryption
        if encryption is not None:
            args["ResultConfiguration"]["EncryptionConfiguration"] = {"EncryptionOption": encryption}
            if kms_key is not None:
                args["ResultConfiguration"]["EncryptionConfiguration"]["KmsKey"] = kms_key
        elif self._session.athena_encryption is not None:
            args["ResultConfiguration"]["EncryptionConfiguration"] = {
                "EncryptionOption": self._session.athena_encryption
            }
            if self._session.athena_kms_key is not None:
                args["ResultConfiguration"]["EncryptionConfiguration"]["KmsKey"] = self._session.athena_kms_key

        # database
        if database is not None:
            args["QueryExecutionContext"] = {"Database": database}
        elif self._session.athena_database is not None:
            args["QueryExecutionContext"] = {"Database": self._session.athena_database}

        # workgroup
        if workgroup is not None:
            args["WorkGroup"] = workgroup
        elif self._session.athena_workgroup is not None:
            args["WorkGroup"] = self._session.athena_workgroup

        logger.debug(f"args: {args}")
        response = self._client_athena.start_query_execution(**args)
        return response["QueryExecutionId"]

    def wait_query(self, query_execution_id: str) -> Dict:
        """
        Wait query ends.

        :param query_execution_id: Query execution ID
        :return: Query response
        """
        final_states: List[str] = ["FAILED", "SUCCEEDED", "CANCELLED"]
        response: Dict = self._client_athena.get_query_execution(QueryExecutionId=query_execution_id)
        state: str = response["QueryExecution"]["Status"]["State"]
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

    def repair_table(self,
                     table: str,
                     database: Optional[str] = None,
                     s3_output: Optional[str] = None,
                     workgroup: Optional[str] = None,
                     encryption: Optional[str] = None,
                     kms_key: Optional[str] = None):
        """
        Run the Hive's metastore consistency check: "MSCK REPAIR TABLE table;".

        Recovers partitions and data associated with partitions.
        Use this statement when you add partitions to the catalog.
        It is possible it will take some time to add all partitions.
        If this operation times out, it will be in an incomplete state
        where only a few partitions are added to the catalog.

        P.S All default values will be inherited from the Session()

        :param database: Glue database name
        :param table: Glue table name
        :param s3_output: AWS S3 path
        :param workgroup: Athena workgroup (By default uses de Session() workgroup)
        :param encryption: None|'SSE_S3'|'SSE_KMS'|'CSE_KMS'
        :param kms_key: For SSE-KMS and CSE-KMS , this is the KMS key ARN or ID.
        :return: Query execution ID
        """
        query = f"MSCK REPAIR TABLE {table};"
        query_id = self.run_query(query=query,
                                  database=database,
                                  s3_output=s3_output,
                                  workgroup=workgroup,
                                  encryption=encryption,
                                  kms_key=kms_key)
        self.wait_query(query_execution_id=query_id)
        return query_id

    @staticmethod
    def _rows2row(rows: List[Dict[str, List[Dict[str, str]]]],
                  python_types: List[Tuple[str, Optional[type]]]) -> Iterator[Dict[str, Any]]:
        for row in rows:
            vals_varchar: List[Optional[str]] = [x["VarCharValue"] if x else None for x in row["Data"]]
            data: Dict[str, Any] = {}
            for (name, ptype), val in zip(python_types, vals_varchar):
                if val is not None:
                    if ptype is None:
                        data[name] = None
                    elif ptype == date:
                        data[name] = date(*[int(y) for y in val.split("-")])
                    elif ptype == datetime:
                        data[name] = datetime.strptime(val + "000", "%Y-%m-%d %H:%M:%S.%f")
                    else:
                        data[name] = ptype(val)
                else:
                    data[name] = None
            yield data

    def get_results(self, query_execution_id: str) -> Iterator[Dict[str, Any]]:
        """
        Get a query results and return a list of rows.

        :param query_execution_id: Query execution ID
        :return: Iterator os lists
        """
        res: Dict = self._client_athena.get_query_results(QueryExecutionId=query_execution_id)
        cols_info: List[Dict] = res["ResultSet"]["ResultSetMetadata"]["ColumnInfo"]
        athena_types: List[Tuple[str, str]] = [(x["Label"], x["Type"]) for x in cols_info]
        logger.info(f"athena_types: {athena_types}")
        python_types: List[Tuple[str, Optional[type]]] = [(n, athena2python(dtype=t)) for n, t in athena_types]
        logger.info(f"python_types: {python_types}")
        rows: List[Dict[str, List[Dict[str, str]]]] = res["ResultSet"]["Rows"][1:]
        for row in Athena._rows2row(rows=rows, python_types=python_types):
            yield row
        next_token: Optional[str] = res.get("NextToken")
        while next_token is not None:
            logger.info(f"next_token: {next_token}")
            res = self._client_athena.get_query_results(QueryExecutionId=query_execution_id, NextToken=next_token)
            rows = res["ResultSet"]["Rows"]
            for row in Athena._rows2row(rows=rows, python_types=python_types):
                yield row
            next_token = res.get("NextToken")

    def query(self,
              query: str,
              database: Optional[str] = None,
              s3_output: Optional[str] = None,
              workgroup: Optional[str] = None,
              encryption: Optional[str] = None,
              kms_key: Optional[str] = None) -> Iterator[Dict[str, Any]]:
        """
        Run a SQL Query against AWS Athena and return the result as a Iterator of lists.

        P.S All default values will be inherited from the Session()

        :param query: SQL query
        :param database: Glue database name
        :param s3_output: AWS S3 path
        :param workgroup: Athena workgroup (By default uses de Session() workgroup)
        :param encryption: None|'SSE_S3'|'SSE_KMS'|'CSE_KMS'
        :param kms_key: For SSE-KMS and CSE-KMS , this is the KMS key ARN or ID.
        :return: Query execution ID
        """
        query_id: str = self.run_query(query=query,
                                       database=database,
                                       s3_output=s3_output,
                                       workgroup=workgroup,
                                       encryption=encryption,
                                       kms_key=kms_key)
        self.wait_query(query_execution_id=query_id)
        return self.get_results(query_execution_id=query_id)

    @staticmethod
    def _normalize_name(name: str) -> str:
        name = "".join(c for c in unicodedata.normalize("NFD", name) if unicodedata.category(c) != "Mn")
        name = name.replace("{", "_")
        name = name.replace("}", "_")
        name = name.replace("]", "_")
        name = name.replace("[", "_")
        name = name.replace(")", "_")
        name = name.replace("(", "_")
        name = name.replace(" ", "_")
        name = name.replace("-", "_")
        name = name.replace(".", "_")
        name = re.sub("(.)([A-Z][a-z]+)", r"\1_\2", name)
        name = re.sub("([a-z0-9])([A-Z])", r"\1_\2", name)
        name = name.lower()
        name = re.sub(r"(_)\1+", "\\1", name)  # remove repeated underscores
        name = name[1:] if name.startswith("_") else name  # remove trailing underscores
        name = name[:-1] if name.endswith("_") else name  # remove trailing underscores
        return name

    @staticmethod
    def normalize_column_name(name: str) -> str:
        """
        Convert the column name to be compatible with Amazon Athena.

        https://docs.aws.amazon.com/athena/latest/ug/tables-databases-columns-names.html

        :param name: column name (str)
        :return: normalized column name (str)
        """
        return Athena._normalize_name(name=name)

    @staticmethod
    def normalize_table_name(name: str) -> str:
        """
        Convert the table name to be compatible with Amazon Athena.

        https://docs.aws.amazon.com/athena/latest/ug/tables-databases-columns-names.html

        :param name: table name (str)
        :return: normalized table name (str)
        """
        return Athena._normalize_name(name=name)

    @staticmethod
    def _parse_path(path: str) -> Tuple[str, str]:
        path2: str = path.replace("s3://", "")
        parts: Tuple[str, str, str] = path2.partition("/")
        return parts[0], parts[2]

    def extract_manifest_paths(self, path: str) -> List[str]:
        """
        Get the list of paths of the generated files.

        :param path: Amazon S3 path
        :return: List of S3 paths
        """
        bucket_name, key_path = self._parse_path(path)
        body: bytes = self._client_s3.get_object(Bucket=bucket_name, Key=key_path)["Body"].read()
        return [x for x in body.decode('utf-8').split("\n") if x != ""]
