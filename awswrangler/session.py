"""AWS Data Wrangler Session Module.

Hold boto3 and botocore states and configurations.

Source repository: https://github.com/awslabs/aws-data-wrangler
Documentation: https://aws-data-wrangler.readthedocs.io/

"""

from logging import Logger, getLogger
from typing import Optional

import boto3  # type: ignore
from botocore.config import Config  # type: ignore

from awswrangler.s3 import S3

logger: Logger = getLogger(__name__)


class Session:
    """Hold boto3 and botocore states."""
    def __init__(self, boto3_session: Optional[boto3.Session] = None, botocore_config: Optional[Config] = None):
        """Wrangler's Session.

        Note
        ----
        The Wrangler's default session inherits all boto3 and botocore defaults.

        Parameters
        ----------
        boto3_session : boto3.Session()
            boto3 Session: https://boto3.amazonaws.com/v1/documentation/api/latest/guide/session.html
        botocore_config : botocore.config.Config()
            botocore Config: https://botocore.amazonaws.com/v1/documentation/api/latest/reference/config.html

        Examples
        --------
        Using the DEFAULT session

        >>> import awswrangler as wr
        >>> wr.SERVICE.FUNCTION()  # e.g. wr.s3.does_object_exists(...)

        Using a custom session

        >>> import boto3
        >>> from botocore.config import Config
        >>> import awswrangler as wr
        >>> session = wr.Session(boto3_session=boto3.Session(profile_name="dev")
        ...     botocore_config=Config(retries={"max_attempts": 15})
        ... )
        >>> session.SERVICE.FUNCTION()  # e.g. session.s3.does_object_exists(...)

        Using multiple sessions

        >>> import boto3
        >>> import awswrangler as wr
        >>> session_virg = wr.Session(boto3_session=boto3.Session(region_name="us-east-1"))
        >>> session_ohio = wr.Session(boto3_session=boto3.Session(region_name="us-east-2"))
        >>> session_virg.SERVICE.FUNCTION()  # e.g. session_virg.s3.does_object_exists(...)
        >>> session_ohio.SERVICE.FUNCTION()  # e.g. session_ohio.s3.does_object_exists(...)

        """
        self._boto3_session: boto3.Session = boto3.Session() if boto3_session is None else boto3_session
        self._botocore_config: Config = Config() if botocore_config is None else botocore_config
        self._s3: Optional[S3] = None
        self._s3_client: Optional[boto3.client] = None

    @property
    def botocore_config(self) -> Config:
        """botocore.config.Config property."""
        return self._botocore_config

    @property
    def boto3_session(self) -> boto3.Session:
        """boto3.Session() property."""
        return self._boto3_session

    @property
    def s3(self) -> S3:
        """S3 property."""
        if self._s3 is None:
            self._s3 = S3(session=self)
        return self._s3

    @property
    def s3_client(self) -> boto3.client:
        """Boto3 S3 client property."""
        if self._s3_client is None:
            self._s3_client = self.boto3_session.client(service_name="s3", use_ssl=True, config=self.botocore_config)
        return self._s3_client
