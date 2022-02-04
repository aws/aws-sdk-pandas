from awswrangler import exceptions
import boto3
from botocore.auth import SigV4Auth
from botocore.awsrequest import AWSRequest
import requests
from typing import Dict, Optional, Any
from gremlin_python.driver import client
from gremlin_python.structure.graph import Path
from gremlin_python.structure.graph import Vertex
from gremlin_python.structure.graph import Edge
from gremlin_python.structure.graph import VertexProperty
from gremlin_python.structure.graph import Property
from gremlin_python.structure.graph import Graph
import logging

_logger: logging.Logger = logging.getLogger(__name__)

DEFAULT_PORT = 8182
NEPTUNE_SERVICE_NAME = 'neptune-db'
HTTP_PROTOCOL = 'https'
WS_PROTOCOL = 'wss'


class NeptuneClient():
    def __init__(self, host: str, port: int = DEFAULT_PORT, iam_enabled: bool = False, boto3_session: Optional[boto3.Session] = None, region: Optional[str] = None):
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


    def __ensure_session(self, session: boto3.Session = None) -> boto3.Session:
        """Ensure that a valid boto3.Session will be returned."""
        if session is not None:
            return session
        elif boto3.DEFAULT_SESSION:
            return boto3.DEFAULT_SESSION
        else:
            return boto3.Session()


    def _prepare_request(self, method, url, *, data=None, params=None, headers=None, service=NEPTUNE_SERVICE_NAME) -> requests.PreparedRequest:
        request = requests.Request(method=method, url=url, data=data, params=params, headers=headers)
        if self.boto3_session is not None:
            aws_request = self._get_aws_request(method=method, url=url, data=data, params=params, headers=headers,
                                                service=service)
            request.headers = dict(aws_request.headers)

        return request.prepare()


    def _get_aws_request(self, method, url, *, data=None, params=None, headers=None, service=NEPTUNE_SERVICE_NAME) -> AWSRequest:
        req = AWSRequest(method=method, url=url, data=data, params=params, headers=headers)
        if self.iam_enabled:
            credentials = self.boto3_session.get_credentials()
            try:
                frozen_creds = credentials.get_frozen_credentials()
            except AttributeError:
                print("Could not find valid IAM credentials in any the following locations:\n")
                print("env, assume-role, assume-role-with-web-identity, sso, shared-credential-file, custom-process, "
                      "config-file, ec2-credentials-file, boto-config, container-role, iam-role\n")
                print("Go to https://boto3.amazonaws.com/v1/documentation/api/latest/guide/credentials.html for more "
                      "details on configuring your IAM credentials.")
                return req
            SigV4Auth(frozen_creds, service, self.region).add_auth(req)
            prepared_iam_req = req.prepare()
            return prepared_iam_req
        else:
            return req


    def read_opencypher(self, query: str, headers:Dict[str, Any] = None) -> Dict[str, Any]:
        if headers is None:
            headers = {}

        if 'content-type' not in headers:
            headers['content-type'] = 'application/x-www-form-urlencoded'

        url = f'{HTTP_PROTOCOL}://{self.host}:{self.port}/openCypher'
        data = {
            'query': query
        }

        req = self._prepare_request('POST', url, data=data, headers=headers)
        res = self._http_session.send(req)
        return res.json()['results']

    
    def read_gremlin(self, query) -> Dict[str, Any]:
        return self._execute_gremlin(query)

    def write_gremlin(self, query) -> bool:
        self._execute_gremlin(query)
        return True
    
    def _execute_gremlin(self, query) -> Dict[str, Any]:
        try:
            uri = f'{HTTP_PROTOCOL}://{self.host}:{self.port}/gremlin'
            request = self._prepare_request('GET', uri)
            ws_url = f'{WS_PROTOCOL}://{self.host}:{self.port}/gremlin'
            c = client.Client(ws_url, 'g', headers=dict(request.headers))
            result = c.submit(query)
            future_results = result.all()
            results = future_results.result()
            c.close()
            return self._gremlin_results_to_dict(results)
        except Exception as e:
            c.close()
            raise e

    
    def read_sparql(self, query, headers:Dict[str, Any] = None) -> Dict[str, Any]:
        if headers is None:
            headers = {}
        
        data = {'query': query}

        if 'content-type' not in headers:
            headers['content-type'] = 'application/x-www-form-urlencoded'

        uri = f'{HTTP_PROTOCOL}://{self.host}:{self.port}/sparql'
        req = self._prepare_request('POST', uri, data=data, headers=headers)
        res = self._http_session.send(req)
        return res


    def status(self):
        url = f'{HTTP_PROTOCOL}://{self.host}:{self.port}/status'
        req = self._prepare_request('GET', url, data='')
        res = self._http_session.send(req)
        if res.status_code == 200:
            return res.json()
        else:
            _logger.error("Error connecting to Amazon Neptune cluster. Please verify your connection details")
            raise ConnectionError(res.status_code)


    def get_graph_traversal_source(self):
        return Graph().traversal()


    def _gremlin_results_to_dict(self, result) -> Dict[str, Any]:
        res=[]

        # For lists or paths unwind them
        if isinstance(result, list) or isinstance(result, Path):
            for x in result:
                res.append(self._parse_dict(x))

        # For dictionaries just add them
        elif isinstance(result, dict):
            res.append(result)

        # For everything else parse them
        else:
            res.append(self._parse_dict(result))            
        return res


    def _parse_dict(self, data) -> Dict[str, Any]:
        d = dict()

        # If this is a list or Path then unwind it
        if isinstance(data, list) or isinstance(data, Path):
            res=[]        
            for x in data:
                res.append(self._parse_dict(x))
            return res
        
        # If this is an element then make it a dictionary
        elif isinstance(data, Vertex) or isinstance(data,Edge) or isinstance(data, VertexProperty) or isinstance(data, Property):
            data=data.__dict__
        
        # If this is a scalar then create a Map with it
        elif not hasattr(data, "__len__") or isinstance(data, str):
            data = {0: data}

        for (k, v) in data.items():
            # If the key is a Vertex or an Edge do special processing
            if isinstance(k, Vertex) or isinstance(k, Edge):
                k = k.id

            # If the value is a list do special processing to make it a scalar if the list is of length 1
            if isinstance(v, list) and len(v) == 1:
                d[k] = v[0]
            else:
                d[k] = v

            # If the value is a Vertex or Edge do special processing
            if isinstance(d[k], Vertex) or isinstance(d[k], Edge) or isinstance(d[k], VertexProperty) or isinstance(d[k], Property):
                d[k] = d[k].__dict__
        return d


def connect(host: str, port: str, iam_enabled: bool = False, **kwargs: Any) -> NeptuneClient:
    return NeptuneClient(host, port, iam_enabled, **kwargs)
