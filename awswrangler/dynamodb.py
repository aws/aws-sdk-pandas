"""Amazon DynamoDB Module."""

from logging import Logger, getLogger
from typing import TYPE_CHECKING

from boto3 import client  # type: ignore

if TYPE_CHECKING:
    from awswrangler.session import Session

logger: Logger = getLogger(__name__)


class DynamoDB:
    """Amazon DynamoDB Class."""
    def __init__(self, session: "Session"):
        """
        Amazon DynamoDB Class Constructor.

        Don't use it directly, call through a Session().
        e.g. wr.redshift.your_method()

        :param session: awswrangler.Session()
        """
        self._session: "Session" = session
        self._client_dynamodb: client = session.boto3_session.client(service_name="dynamodb",
                                                                     use_ssl=True,
                                                                     config=session.botocore_config)
