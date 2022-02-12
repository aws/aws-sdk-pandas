"""Amazon NeptuneClient Module"""

import logging
from typing import Any, Optional

import boto3
import nest_asyncio
import requests
from botocore.auth import SigV4Auth
from botocore.awsrequest import AWSRequest
from gremlin_python.driver import client
from SPARQLWrapper import SPARQLWrapper

from awswrangler import exceptions
from awswrangler.neptune.gremlin_parser import GremlinParser

_logger: logging.Logger = logging.getLogger(__name__)

DEFAULT_PORT = 8182
NEPTUNE_SERVICE_NAME = "neptune-db"
HTTP_PROTOCOL = "https"
WS_PROTOCOL = "wss"


class NeptuneClient:
    """This object represents a Neptune cluster connection."""

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

    def __get_region_from_session(self) -> str:
        """Extract region from session."""
        region: Optional[str] = self.boto3_session.region_name
        if region is not None:
            return region
        raise exceptions.InvalidArgument("There is no region_name defined on boto3, please configure it.")

    @staticmethod
    def __ensure_session(session: boto3.Session = None) -> boto3.Session:
        """Ensure that a valid boto3.Session will be returned."""
        if session is not None:
            return session
        elif boto3.DEFAULT_SESSION:
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
    ) -> requests.PreparedRequest:
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
    ) -> AWSRequest:
        req = AWSRequest(method=method, url=url, data=data, params=params, headers=headers)
        if self.iam_enabled:
            credentials = self.boto3_session.get_credentials()
            try:
                frozen_creds = credentials.get_frozen_credentials()
            except AttributeError:
                print("Could not find valid IAM credentials in any the following locations:\n")
                print(
                    "env, assume-role, assume-role-with-web-identity, sso, shared-credential-file, custom-process, "
                    "config-file, ec2-credentials-file, boto-config, container-role, iam-role\n"
                )
                print(
                    "Go to https://boto3.amazonaws.com/v1/documentation/api/latest/guide/credentials.html for more "
                    "details on configuring your IAM credentials."
                )
                return req
            SigV4Auth(frozen_creds, service, self.region).add_auth(req)
            prepared_iam_req = req.prepare()
            return prepared_iam_req
        else:
            return req

    def read_opencypher(self, query: str, headers: Any = None) -> Any:
        """Executes the provided openCypher query

        Args:
            query (str): The query to execute
            headers (Any, optional): Any additional headers that should be associated with the query. Defaults to None.

        Returns:
            Any: [description] The result of the query
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
        return res.json()["results"]

    def read_gremlin(self, query: str) -> Any:
        """Executes the provided Gremlin traversal and returns the results

        Args:
            query (str): The Gremlin query

        Returns:
            Any: [description]
        """
        return self._execute_gremlin(query)

    def write_gremlin(self, query: str) -> bool:
        """Executes a Gremlin write query

        Args:
            query (str): The query to execute

        Returns:
            bool: The success of the Gremlin write query
        """
        res = self._execute_gremlin(query)
        _logger.debug(res)
        return True

    def _execute_gremlin(self, query: str) -> Any:
        try:
            nest_asyncio.apply()
            uri = f"{HTTP_PROTOCOL}://{self.host}:{self.port}/gremlin"
            request = self._prepare_request("GET", uri)
            ws_url = f"{WS_PROTOCOL}://{self.host}:{self.port}/gremlin"
            c = client.Client(ws_url, "g", headers=dict(request.headers))
            result = c.submit(query)
            future_results = result.all()
            results = future_results.result()
            c.close()
            return GremlinParser.gremlin_results_to_dict(results)
        except Exception as e:
            c.close()
            _logger.error(e)
            raise e

    def read_sparql(self, query: str, headers: Any = None) -> Any:
        """Executes the given query and returns the results

        Args:
            query ([type]): The SPARQL query to execute
            headers (Any, optional): Any additional headers to include with the request. Defaults to None.

        Returns:
            Any: [description]
        """
        res = self._execute_sparql(query, headers)
        _logger.debug(res)
        return res

    def write_sparql(self, query: str, headers: Any = None) -> bool:
        """Executes the specified SPARQL write statements

        Args:
            query ([type]): The SPARQL query to execute
            headers (Any, optional): Any additional headers to include with the request. Defaults to None.

        Returns:
            bool: The success of the query
        """
        self._execute_sparql(query, headers)
        return True

    def _execute_sparql(self, query: str, headers: Any) -> Any:
        if headers is None:
            headers = {}

        s = SPARQLWrapper("")
        s.setQuery(query)
        query_type = s.queryType.upper()
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
        return res.json()

    def status(self) -> Any:
        """Returns the status of the Neptune cluster

        Returns:
            str: The result of the call to the status API for the Neptune cluster
        """
        url = f"{HTTP_PROTOCOL}://{self.host}:{self.port}/status"
        req = self._prepare_request("GET", url, data="")
        res = self._http_session.send(req)
        return res.json()
