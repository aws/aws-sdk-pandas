from boto3 import Session

DEFAULT_PORT = 8182
DEFAULT_REGION = 'us-east-1'

class NeptuneClient():
    def __init__(self, host: str, port: int = DEFAULT_PORT, ssl: bool = True, region: str = DEFAULT_REGION):
        self.host = host
        self.port = port
        self.ssl = ssl
