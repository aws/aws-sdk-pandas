# mypy: disable-error-code=name-defined
"""Amazon NeptuneClient Module."""

import logging
from typing import Any, Dict, List, Optional, Union, cast

import boto3
from botocore.auth import SigV4Auth
from botocore.awsrequest import AWSPreparedRequest, AWSRequest

import awswrangler.neptune._gremlin_init as gremlin
from awswrangler import _utils, exceptions
from awswrangler.neptune._gremlin_parser import GremlinParser

gremlin_python = _utils.import_optional_dependency("gremlin_python")
opencypher = _utils.import_optional_dependency("requests")
sparql = _utils.import_optional_dependency("SPARQLWrapper")
if any((gremlin_python, opencypher, sparql)):
    import requests

_logger: logging.Logger = logging.getLogger(__name__)


DEFAULT_PORT = 8182
NEPTUNE_SERVICE_NAME = "neptune-db"
HTTP_PROTOCOL = "https"
WS_PROTOCOL = "wss"


class NeptuneClient:
    """Class representing a Neptune cluster connection."""

    def __init__(
        self,
        host: str,
        port: int = DEFAULT_PORT,
        iam_enabled: bool = False,
        boto3_session: Optional[boto3.Session] = None,
        region: Optional[str] = None,
    ):
        self.host = host
        self.port = port
        self.iam_enabled = iam_enabled
        self.boto3_session = self.__ensure_session(session=boto3_session)
        if region is None:
            self.region = self.__get_region_from_session()
        else:
            self.region = region
        self._http_session = requests.Session()
        self.gremlin_connection = None

    def __del__(self) -> None:
        """Close the Gremlin connection."""
        if isinstance(self.gremlin_connection, gremlin.Client):
            self.gremlin_connection.close()

    def __get_region_from_session(self) -> str:
        """Extract region from session."""
        region: Optional[str] = self.boto3_session.region_name
        if region is not None:
            return region
        raise exceptions.InvalidArgument("There is no region_name defined on boto3, please configure it.")

    @staticmethod
    def __ensure_session(session: Optional[boto3.Session] = None) -> boto3.Session:
        """Ensure that a valid boto3.Session will be returned."""
        if session is not None:
            return session
        if boto3.DEFAULT_SESSION:
            return boto3.DEFAULT_SESSION

        return boto3.Session()

    def _prepare_request(
        self,
        method: str,
        url: str,
        *,
        data: Any = None,
        params: Any = None,
        headers: Any = None,
        service: str = NEPTUNE_SERVICE_NAME,
    ) -> "requests.PreparedRequest":
        request = requests.Request(method=method, url=url, data=data, params=params, headers=headers)
        if self.boto3_session is not None:
            aws_request = self._get_aws_request(
                method=method, url=url, data=data, params=params, headers=headers, service=service
            )
            request.headers = dict(aws_request.headers)

        return request.prepare()

    def _get_aws_request(
        self,
        method: str,
        url: str,
        *,
        data: Any = None,
        params: Any = None,
        headers: Any = None,
        service: str = NEPTUNE_SERVICE_NAME,
    ) -> Union[AWSRequest, AWSPreparedRequest]:
        req = AWSRequest(method=method, url=url, data=data, params=params, headers=headers)
        if self.iam_enabled:
            credentials = self.boto3_session.get_credentials()
            try:
                frozen_creds = credentials.get_frozen_credentials()
            except AttributeError:
                _logger.warning("Could not find valid IAM credentials in any the following locations:\n")
                _logger.warning(
                    "env, assume-role, assume-role-with-web-identity, sso, shared-credential-file, custom-process, "
                    "config-file, ec2-credentials-file, boto-config, container-role, iam-role\n"
                )
                _logger.warning(
                    "Go to https://boto3.amazonaws.com/v1/documentation/api/latest/guide/credentials.html for more "
                    "details on configuring your IAM credentials."
                )
                return req
            SigV4Auth(frozen_creds, service, self.region).add_auth(req)
            prepared_iam_req = req.prepare()
            return prepared_iam_req
        return req

    def read_opencypher(self, query: str, headers: Any = None) -> Any:
        """Execute the provided openCypher query.

        Parameters
        ----------
        query : str
            The query to execute
        headers : Any, optional
            Any additional headers that should be associated with the query. Defaults to None.

        Returns
        -------
        Any
            The result of the query.
        """
        if headers is None:
            headers = {}

        if "content-type" not in headers:
            headers["content-type"] = "application/x-www-form-urlencoded"

        url = f"{HTTP_PROTOCOL}://{self.host}:{self.port}/openCypher"
        data = {"query": query}

        req = self._prepare_request("POST", url, data=data, headers=headers)
        res = self._http_session.send(req)
        _logger.debug(res)
        if res.ok:
            return res.json()["results"]
        raise exceptions.QueryFailed(f"Status Code: {res.status_code} Reason: {res.reason} Message: {res.text}")

    def read_gremlin(self, query: str, headers: Any = None) -> List[Dict[str, Any]]:
        """Execute the provided Gremlin traversal and returns the results.

        Parameters
        ----------
        query : str
            The Gremlin query

        Returns
        -------
        Dict[str, Any]
            Dictionary with the results
        """
        return self._execute_gremlin(query, headers)

    def write_gremlin(self, query: str) -> bool:
        """Execute a Gremlin write query.

        Parameters
        ----------
            query (str): The query to execute

        Returns
        -------
        bool
            The success of the Gremlin write query
        """
        res = self._execute_gremlin(query)
        _logger.debug(res)
        return True

    def _execute_gremlin(self, query: str, headers: Any = None) -> List[Dict[str, Any]]:
        try:
            c = self._get_gremlin_connection(headers)
            result = c.submit(query)
            future_results = result.all()
            results = future_results.result()
            return GremlinParser.gremlin_results_to_dict(results)
        except Exception as e:
            if isinstance(self.gremlin_connection, gremlin.Client):
                self.gremlin_connection.close()
            self.gremlin_connection = None
            _logger.error(e)
            raise exceptions.QueryFailed(e)

    def _get_gremlin_connection(self, headers: Any = None) -> "gremlin.Client":
        if self.gremlin_connection is None:
            uri = f"{HTTP_PROTOCOL}://{self.host}:{self.port}/gremlin"
            request = self._prepare_request("GET", uri, headers=headers)
            ws_url = f"{WS_PROTOCOL}://{self.host}:{self.port}/gremlin"
            self.gremlin_connection = gremlin.Client(
                ws_url, "g", headers=dict(request.headers), call_from_event_loop=True
            )
        return self.gremlin_connection

    def read_sparql(self, query: str, headers: Any = None) -> Any:
        """Execute the given query and returns the results.

        Parameters
        ----------
        query : str
            The SPARQL query to execute
        headers : Any, optional
            Any additional headers to include with the request. Defaults to None.

        Returns
        -------
        Any
            [description]
        """
        res = self._execute_sparql(query, headers)
        _logger.debug(res)
        return res

    def write_sparql(self, query: str, headers: Any = None) -> bool:
        """Execute the specified SPARQL write statements.

        Parameters
        ----------
        query : str
            The SPARQL query to execute
        headers : Any, optional
            Any additional headers to include with the request. Defaults to None.

        Returns
        -------
        bool
            The success of the query
        """
        self._execute_sparql(query, headers)
        return True

    def _execute_sparql(self, query: str, headers: Any) -> Any:
        if headers is None:
            headers = {}

        s = sparql.SPARQLWrapper("")
        s.setQuery(query)
        query_type = str(s.queryType).upper()
        if query_type in ["SELECT", "CONSTRUCT", "ASK", "DESCRIBE"]:
            data = {"query": query}
        else:
            data = {"update": query}

        if "content-type" not in headers:
            headers["content-type"] = "application/x-www-form-urlencoded"

        uri = f"{HTTP_PROTOCOL}://{self.host}:{self.port}/sparql"
        req = self._prepare_request("POST", uri, data=data, headers=headers)
        res = self._http_session.send(req)
        _logger.debug(res)
        if res.ok:
            return res.json()
        raise exceptions.QueryFailed(f"Status Code: {res.status_code} Reason: {res.reason} Message: {res.text}")

    def status(self) -> Any:
        """Return the status of the Neptune cluster.

        Returns
        -------
        str
            The result of the call to the status API for the Neptune cluster
        """
        url = f"{HTTP_PROTOCOL}://{self.host}:{self.port}/status"
        req = self._prepare_request("GET", url, data="")
        res = self._http_session.send(req)
        return res.json()

    def load(self, s3_path: str, role_arn: str, parallelism: str = "HIGH", format: str = "csv") -> str:
        """
        Start the Neptune Loader command for loading CSV data from external files on S3 into a Neptune DB cluster.

        Parameters
        ----------
        s3_path: str
            Amazon S3 URI that identifies a single file, multiple files, a folder, or multiple folders.
            Neptune loads every data file in any folder that is specified.
        role_arn: str
            The Amazon Resource Name (ARN) for an IAM role to be assumed by the Neptune DB instance for access to the S3 bucket.
            For information about creating a role that has access to Amazon S3 and then associating it with a Neptune cluster,
            see `Prerequisites: IAM Role and Amazon S3 Access <https://docs.aws.amazon.com/neptune/latest/userguide/bulk-load-tutorial-IAM.html>`_.
        parallelism: str
            Specifies the number of threads used by the bulk load process.
        format: str
            The format of the data. For more information about data formats for the Neptune Loader command,
            see `Using the Amazon Neptune Bulk Loader to Ingest Data <https://docs.aws.amazon.com/neptune/latest/userguide/load-api-reference-load.html#:~:text=The%20format%20of%20the%20data.%20For%20more%20information%20about%20data%20formats%20for%20the%20Neptune%20Loader%20command%2C%20see%20Using%20the%20Amazon%20Neptune%20Bulk%20Loader%20to%20Ingest%20Data.>`_.

        Returns
        -------
        str
            ID of the load job
        """
        data = {
            "source": s3_path,
            "format": format,
            "iamRoleArn": role_arn,
            "mode": "AUTO",
            "region": self.region,
            "failOnError": "TRUE",
            "parallelism": parallelism,
        }

        url = f"https://{self.host}:{self.port}/loader"

        req = self._prepare_request("POST", url, data=data)
        res = self._http_session.send(req)

        _logger.debug(res)
        if res.ok:
            return cast(str, res.json()["payload"]["loadId"])

        raise exceptions.NeptuneLoadError(f"Status Code: {res.status_code} Reason: {res.reason} Message: {res.text}")

    def load_status(self, load_id: str) -> Any:
        """
        Return the status of the load job to the Neptune cluster.

        Parameters
        ----------
        load_id: str
            ID of the load job

        Returns
        -------
        dict[str, Any]
            The result of the call to the status API for the load job.
            See `Neptune Loader Get-Status Responses <https://docs.aws.amazon.com/neptune/latest/userguide/load-api-reference-status-response.html>_`
        """
        url = f"https://{self.host}:{self.port}/loader/{load_id}"

        req = self._prepare_request("GET", url, data="")
        res = self._http_session.send(req)

        if res.ok:
            return res.json()

        raise exceptions.NeptuneLoadError(f"Status Code: {res.status_code} Reason: {res.reason} Message: {res.text}")
