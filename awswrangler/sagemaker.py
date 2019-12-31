from typing import Any, Dict
import pickle
import tarfile
import logging

from awswrangler.exceptions import InvalidParameters, InvalidSagemakerOutput

logger = logging.getLogger(__name__)


class SageMaker:
    def __init__(self, session):
        self._session = session
        self._client_s3 = session.boto3_session.client(service_name="s3", use_ssl=True, config=session.botocore_config)
        self._client_sagemaker = session.boto3_session.client(service_name="sagemaker",
                                                              use_ssl=True,
                                                              config=session.botocore_config)

    @staticmethod
    def _parse_path(path):
        path2 = path.replace("s3://", "")
        parts = path2.partition("/")
        return parts[0], parts[2]

    def get_job_outputs(self, job_name: str = None, path: str = None) -> Dict[str, Any]:
        """
        Extract and deserialize all Sagemaker's outputs (everything inside model.tar.gz)

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
            raise InvalidSagemakerOutput(f"No artifacts found in {path}")

        results: Dict[str, Any] = {}
        for member in members:
            logger.debug(f"member: {member.name}")
            f = tar.extractfile(member)
            file_type: str = member.name.split(".")[-1]

            if (file_type == "pkl") and (f is not None):
                f = pickle.load(f)

            results[member.name] = f

        return results

    def get_model(self, job_name: str = None, path: str = None, model_name: str = None) -> Any:
        """
        Extract and deserialize a Sagemaker's output model (.tat.gz)

        :param job_name: Sagemaker's job name
        :param path: S3 path (model.tar.gz path)
        :param model_name: model name (e.g: )
        :return:
        """
        outputs: Dict[str, Any] = self.get_job_outputs(job_name=job_name, path=path)
        outputs_len: int = len(outputs)
        if model_name in outputs:
            return outputs[model_name]
        elif outputs_len > 1:
            raise InvalidSagemakerOutput(
                f"Number of artifacts found: {outputs_len}. Please, specify a model_name or use the Sagemaker.get_job_outputs() method."
            )
        return list(outputs.values())[0]
