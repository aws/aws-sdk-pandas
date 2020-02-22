"""AWS Data Wrangler Session Module.

Hold boto3 and botocore states and configurations.

Source repository: https://github.com/awslabs/aws-data-wrangler
Documentation: https://aws-data-wrangler.readthedocs.io/

"""

from logging import Logger, getLogger
from typing import Optional

import boto3  # type: ignore
from botocore.config import Config  # type: ignore

from awswrangler import exceptions
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
        >>> wr.s3.does_object_exists(...)

        Overwriting the DEFAULT session

        >>> import boto3
        >>> from botocore.config import Config
        >>> import awswrangler as wr
        >>> wr.session = wr.Session(
        ...     boto3.Session(profile_name="dev"),
        ...     botocore_config=Config(retries={"max_attempts": 15})
        ... )

        Using multiple sessions

        >>> import boto3
        >>> import awswrangler as wr
        >>> session_virg = wr.Session(boto3_session=boto3.Session(region_name="us-east-1"))
        >>> session_ohio = wr.Session(boto3_session=boto3.Session(region_name="us-east-2"))
        >>> session_virg.s3.does_object_exists(...)
        >>> session_ohio.s3.does_object_exists(...)

        """
        if boto3_session is None:
            self._boto3_session: boto3.Session = boto3.Session()
        else:
            self._boto3_session = boto3_session
        if botocore_config is None:
            self._botocore_config: Config = Config()
        else:
            self._botocore_config = botocore_config
        self._s3: Optional[S3] = None
        self._s3_client: Optional[boto3.client] = None
        self._primitives: Optional[_SessionPrimitives] = None
        self._instantiate_primitives()

    def _instantiate_primitives(self) -> None:
        """Instantiate the inner SessionPrimitives()."""
        credentials = self.boto3_session.get_credentials()
        if credentials is None:
            raise exceptions.AWSCredentialsNotFound(
                "Please run 'aws configure': https://docs.aws.amazon.com/cli/latest/userguide/cli-chap-configure.html"
            )  # pragma: no cover
        self._primitives = _SessionPrimitives(profile_name=self.boto3_session.profile_name,
                                              aws_access_key_id=credentials.access_key,
                                              aws_secret_access_key=credentials.secret_key,
                                              aws_session_token=credentials.token,
                                              region_name=self.boto3_session.region_name,
                                              botocore_config=self.botocore_config)

    @property
    def botocore_config(self) -> Config:
        """botocore.config.Config property."""
        return self._botocore_config

    @property
    def boto3_session(self) -> boto3.Session:
        """boto3.Session() property."""
        return self._boto3_session

    @property
    def primitives(self):
        """Primitives property."""
        return self._primitives

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


class _SessionPrimitives:
    """
    A minimalist and non-functional version of Session to store only primitive attributes.

    It is required to "share" the session attributes to other processes.
    It must be "pickable".

    """
    def __init__(self,
                 aws_access_key_id: Optional[str] = None,
                 aws_secret_access_key: Optional[str] = None,
                 aws_session_token: Optional[str] = None,
                 region_name: Optional[str] = None,
                 profile_name: Optional[str] = None,
                 botocore_config: Optional[Config] = None):
        self._aws_access_key_id: Optional[str] = aws_access_key_id
        self._aws_secret_access_key: Optional[str] = aws_secret_access_key
        self._aws_session_token: Optional[str] = aws_session_token
        self._region_name: Optional[str] = region_name
        self._profile_name: Optional[str] = profile_name
        self._botocore_config: Optional[Config] = botocore_config

    def build_session(self) -> Session:
        """Reconstruct the session from primitives."""
        return Session(boto3_session=boto3.Session(aws_access_key_id=self._aws_access_key_id,
                                                   aws_secret_access_key=self._aws_secret_access_key,
                                                   aws_session_token=self._aws_session_token,
                                                   region_name=self._region_name,
                                                   profile_name=self._profile_name),
                       botocore_config=self._botocore_config)
