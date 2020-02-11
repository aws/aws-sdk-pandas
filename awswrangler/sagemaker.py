"""Amazon SageMaker Module."""

import tarfile
from logging import Logger, getLogger
from typing import TYPE_CHECKING, Any, Dict

from boto3 import client  # type: ignore

from awswrangler.exceptions import InvalidParameters, InvalidSagemakerOutput

if TYPE_CHECKING:
    from awswrangler.session import Session

logger: Logger = getLogger(__name__)


class SageMaker:
    """Amazon SageMaker Class."""
    def __init__(self, session: "Session"):
        """
        Amazon SageMaker Class Constructor.

        Don't use it directly, call through a Session().
        e.g. wr.redshift.your_method()

        :param session: awswrangler.Session()
        """
        self._session: "Session" = session
        self._client_s3: client = session.boto3_session.client(service_name="s3",
                                                               use_ssl=True,
                                                               config=session.botocore_config)
        self._client_sagemaker: client = session.boto3_session.client(service_name="sagemaker",
                                                                      use_ssl=True,
                                                                      config=session.botocore_config)

    @staticmethod
    def _parse_path(path):
        path2 = path.replace("s3://", "")
        parts = path2.partition("/")
        return parts[0], parts[2]

    def get_job_outputs(self, job_name: str = None, path: str = None) -> Dict[str, Any]:
        """
        Extract and deserialize all Sagemaker's outputs (everything inside model.tar.gz).

        :param job_name: Sagemaker's job name
        :param path: S3 path (model.tar.gz path)
        :return: A Dictionary with all filenames (key) and all objects (values)
        """
        if path and job_name:
            raise InvalidParameters("Specify either path or job_name")

        if job_name:
            path = self._client_sagemaker.describe_training_job(
                TrainingJobName=job_name)["ModelArtifacts"]["S3ModelArtifacts"]

        if path is not None:
            if path.split("/")[-1] != "model.tar.gz":
                path = f"{path}/model.tar.gz"

        if self._session.s3.does_object_exists(path) is False:
            raise InvalidSagemakerOutput(f"Path does not exists ({path})")

        bucket: str
        key: str
        bucket, key = SageMaker._parse_path(path)
        body = self._client_s3.get_object(Bucket=bucket, Key=key)["Body"].read()
        body = tarfile.io.BytesIO(body)  # type: ignore
        tar = tarfile.open(fileobj=body)

        members = tar.getmembers()
        if len(members) < 1:
            raise InvalidSagemakerOutput(f"No artifacts found in {path}.")

        results: Dict[str, Any] = {}
        for member in members:
            logger.debug(f"member: {member.name}")
            results[member.name] = tar.extractfile(member)

        return results
