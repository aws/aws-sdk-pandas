"""Amazon S3 Module."""

from logging import Logger, getLogger
from time import sleep
from typing import TYPE_CHECKING, List, Optional, Tuple

from botocore.exceptions import ClientError  # type: ignore

from awswrangler.exceptions import S3WaitObjectTimeout

if TYPE_CHECKING:
    from awswrangler.session import Session  # pragma: no cover

logger: Logger = getLogger(__name__)


class S3:
    """Amazon S3 Class."""
    def __init__(self, session: "Session"):
        """Amazon S3 Class Constructor.

        Note
        ----
        Don't use it directly, call through a Session().
        e.g. wr.SERVICE.FUNCTION() (Default Session)

        Parameters
        ----------
        session : awswrangler.Session()
            Wrangler's Session

        """
        self._session: "Session" = session

    def does_object_exists(self, path: str) -> bool:
        """Check if object exists on S3.

        Parameters
        ----------
        path: str
            S3 path (e.g. s3://bucket/key).

        Returns
        -------
        bool
            True if exists, False otherwise.

        Examples
        --------
        >>> import awswrangler as wr
        >>> wr.s3.does_object_exists("s3://bucket/key_real")
        True
        >>> wr.s3.does_object_exists("s3://bucket/key_unreal")
        False

        """
        bucket: str
        key: str
        bucket, key = path.replace("s3://", "").split("/", 1)
        try:
            self._session.s3_client.head_object(Bucket=bucket, Key=key)
            return True
        except ClientError as ex:
            if ex.response["ResponseMetadata"]["HTTPStatusCode"] == 404:
                return False
            raise ex  # pragma: no cover

    def wait_object_exists(self, path: str, polling_sleep: float = 0.1, timeout: Optional[float] = 10.0) -> None:
        """Wait object exists on S3.

        Parameters
        ----------
        path : str
            S3 path (e.g. s3://bucket/key).
        polling_sleep : float
            Sleep between each retry (Seconds).
        timeout : float, optional
            Timeout (Seconds).

        Returns
        -------
        None
            None

        Raises
        ------
        S3WaitObjectTimeout
            Raised in case of timeout.

        Examples
        --------
        >>> import awswrangler as wr
        >>> wr.s3.wait_object_exists("s3://bucket/key_expected")

        """
        time_acc: float = 0.0
        while self.does_object_exists(path=path) is False:
            sleep(polling_sleep)
            if timeout is not None:
                time_acc += polling_sleep
                if time_acc >= timeout:
                    raise S3WaitObjectTimeout(f"Waited for {path} for {time_acc} seconds")

    @staticmethod
    def parse_path(path: str) -> Tuple[str, str]:
        """Split a full S3 path in bucket and key strings.

        "s3://bucket/key" -> ("bucket", "key")

        Parameters
        ----------
        path : str
            S3 path (e.g. s3://bucket/key).

        Returns
        -------
        Tuple[str, str]
            Tuple of bucket and key strings

        Examples
        --------
        >>> import awswrangler as wr
        >>> bucket, key = wr.s3.parse_path("s3://bucket/key")

        """
        parts = path.replace("s3://", "").split("/", 1)
        bucket: str = parts[0]
        key: str = ""
        if len(parts) == 2:
            key = key if parts[1] is None else parts[1]
        return bucket, key

    def get_bucket_region(self, bucket: str) -> str:
        """Get bucket region.

        Parameters
        ----------
        bucket : str
            Bucket name.

        Returns
        -------
        str
            Region code (e.g. "us-east-1").

        Examples
        --------
        >>> import awswrangler as wr
        >>> region = wr.s3.get_bucket_region("bucket-name")

        """
        logger.debug(f"bucket: {bucket}")
        region: str = self._session.s3_client.get_bucket_location(Bucket=bucket)["LocationConstraint"]
        region = "us-east-1" if region is None else region
        logger.debug(f"region: {region}")
        return region

    def list_objects(self, path: str) -> List[str]:
        """List Amazon S3 objects from a prefix.

        Parameters
        ----------
        path : str
            S3 path (e.g. s3://bucket/prefix).

        Returns
        -------
        List[str]
            List of objects paths

        Examples
        --------
        >>> import awswrangler as wr
        >>> wr.s3.list_objects("s3://bucket/prefix")
        ["s3://bucket/prefix0", "s3://bucket/prefix1", "s3://bucket/prefix2"]

        """
        paginator = self._session.s3_client.get_paginator("list_objects_v2")
        bucket: str
        prefix: str
        bucket, prefix = self.parse_path(path=path)
        response_iterator = paginator.paginate(Bucket=bucket, Prefix=prefix, PaginationConfig={"PageSize": 1000})
        paths: List[str] = []
        for page in response_iterator:
            for content in page.get("Contents"):
                if (content is not None) and ("Key" in content):
                    key: str = content["Key"]
                    paths.append(f"s3://{bucket}/{key}")
        return paths
