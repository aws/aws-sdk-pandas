from typing import TYPE_CHECKING
from logging import getLogger, Logger

from boto3 import client  # type: ignore

if TYPE_CHECKING:
    from awswrangler.session import Session

logger: Logger = getLogger(__name__)


class DynamoDB:
    def __init__(self, session: "Session"):
        self._session: "Session" = session
        self._client_dynamodb: client = session.boto3_session.client(service_name="dynamodb",
                                                                     use_ssl=True,
                                                                     config=session.botocore_config)
