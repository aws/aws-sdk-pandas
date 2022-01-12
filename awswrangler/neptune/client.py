import boto3
from awswrangler import exceptions
from typing import  Optional

DEFAULT_PORT = 8182

class NeptuneClient():
    def __init__(self, host: str, port: int = DEFAULT_PORT, ssl: bool = True, 
        boto3_session: Optional[boto3.Session] = None,
        region: Optional[str] = None,
    ):
        self.host = host
        self.port = port
        self.ssl = ssl
        self.boto3_session = self.__ensure_session(session=boto3_session)
        if region is None:
            region = self.__get_region_from_session()
        else:
            self.region = region


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