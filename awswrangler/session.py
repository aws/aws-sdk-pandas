"""AWS Data Wrangler Session Module."""

import importlib
from logging import Logger, getLogger
from os import cpu_count
from sys import version_info
from typing import Dict, Optional

import boto3  # type: ignore
from botocore.config import Config  # type: ignore

from awswrangler.athena import Athena
from awswrangler.aurora import Aurora
from awswrangler.cloudwatchlogs import CloudWatchLogs
from awswrangler.dynamodb import DynamoDB
from awswrangler.emr import EMR
from awswrangler.exceptions import AWSCredentialsNotFound
from awswrangler.glue import Glue
from awswrangler.pandas import Pandas
from awswrangler.redshift import Redshift
from awswrangler.s3 import S3
from awswrangler.sagemaker import SageMaker

PYSPARK_INSTALLED: bool = False
if version_info < (3, 8) and importlib.util.find_spec("pyspark"):  # type: ignore
    PYSPARK_INSTALLED = True
    from awswrangler.spark import Spark

logger: Logger = getLogger(__name__)


class Session:
    """
    A session stores configuration state.

    (e.g. Boto3.Session, pyspark.sql.SparkSession, pyspark.SparkContext,
    AWS Glue Connections attributes, number of cpu cores that can be used, etc)
    """

    PROCS_IO_BOUND_FACTOR: int = 2

    def __init__(self,
                 boto3_session=None,
                 profile_name: Optional[str] = None,
                 aws_access_key_id: Optional[str] = None,
                 aws_secret_access_key: Optional[str] = None,
                 aws_session_token: Optional[str] = None,
                 region_name: Optional[str] = None,
                 botocore_max_retries: int = 40,
                 s3_additional_kwargs: Optional[Dict[str, str]] = None,
                 spark_context=None,
                 spark_session=None,
                 procs_cpu_bound: Optional[int] = None,
                 procs_io_bound: Optional[int] = None,
                 athena_workgroup: str = "primary",
                 athena_s3_output: Optional[str] = None,
                 athena_encryption: Optional[str] = "SSE_S3",
                 athena_kms_key: Optional[str] = None,
                 athena_database: str = "default",
                 athena_ctas_approach: bool = False,
                 redshift_temp_s3_path: Optional[str] = None,
                 aurora_temp_s3_path: Optional[str] = None):
        """
        Most parameters inherit from Boto3 or Pyspark.

        https://boto3.amazonaws.com/v1/documentation/api/latest/guide/configuration.html
        https://spark.apache.org/docs/latest/api/python/index.html

        :param boto3_session: Boto3.Session (Overwrite others Boto3 parameters)
        :param profile_name: Boto3 profile_name
        :param aws_access_key_id: Boto3 aws_access_key_id
        :param aws_secret_access_key: Boto3 aws_secret_access_key
        :param aws_session_token: Boto3 aws_session_token
        :param region_name: Boto3 region_name
        :param botocore_max_retries: Botocore max retries
        :param s3_additional_kwargs: Passed on to s3fs (https://s3fs.readthedocs.io/en/latest/#serverside-encryption)
        :param spark_context: Spark Context (pyspark.SparkContext)
        :param spark_session: Spark Session (pyspark.sql.SparkSession)
        :param procs_cpu_bound: number of processes that can be used in single node applications for CPU bound case (Default: os.cpu_count())
        :param procs_io_bound: number of processes that can be used in single node applications for I/O bound cases (Default: os.cpu_count() * PROCS_IO_BOUND_FACTOR)
        :param athena_workgroup: Default AWS Athena Workgroup (str)
        :param athena_database: AWS Glue/Athena database name
        :param athena_ctas_approach: Wraps the query with a CTAS
        :param athena_s3_output: AWS S3 path
        :param athena_encryption: None|'SSE_S3'|'SSE_KMS'|'CSE_KMS'
        :param athena_kms_key: For SSE-KMS and CSE-KMS , this is the KMS key ARN or ID.
        :param redshift_temp_s3_path: AWS S3 path to write temporary data (e.g. s3://...)
        :param aurora_temp_s3_path: AWS S3 path to write temporary data (e.g. s3://...)
        """
        self._profile_name: Optional[str] = (boto3_session.profile_name if boto3_session else profile_name)
        self._aws_access_key_id: Optional[str] = (boto3_session.get_credentials().access_key
                                                  if boto3_session else aws_access_key_id)
        self._aws_secret_access_key: Optional[str] = (boto3_session.get_credentials().secret_key
                                                      if boto3_session else aws_secret_access_key)
        self._botocore_max_retries: int = botocore_max_retries
        self._botocore_config = Config(retries={"max_attempts": self._botocore_max_retries})
        self._aws_session_token: Optional[str] = aws_session_token
        self._region_name: Optional[str] = boto3_session.region_name if boto3_session else region_name
        self._s3_additional_kwargs: Optional[Dict[str, str]] = s3_additional_kwargs
        self._spark_context = spark_context
        self._spark_session = spark_session
        cpus: Optional[int] = cpu_count()
        self._procs_cpu_bound: int = 1 if cpus is None else cpus if procs_cpu_bound is None else procs_cpu_bound
        self._procs_io_bound: int = 1 if cpus is None else cpus * Session.PROCS_IO_BOUND_FACTOR if procs_io_bound is None else procs_io_bound
        self._athena_workgroup: str = athena_workgroup
        self._athena_s3_output: Optional[str] = athena_s3_output
        self._athena_encryption: Optional[str] = athena_encryption
        self._athena_kms_key: Optional[str] = athena_kms_key
        self._athena_database: str = athena_database
        self._athena_ctas_approach: bool = athena_ctas_approach
        self._redshift_temp_s3_path: Optional[str] = redshift_temp_s3_path
        self._aurora_temp_s3_path: Optional[str] = aurora_temp_s3_path
        self._primitives: Optional[SessionPrimitives] = None
        self._load_new_primitives()
        if boto3_session:
            self._boto3_session = boto3_session
        else:
            self._load_new_boto3_session()
        self._s3: Optional[S3] = None
        self._athena: Optional[Athena] = None
        self._cloudwatchlogs: Optional[CloudWatchLogs] = None
        self._emr: Optional[EMR] = None
        self._pandas: Optional[Pandas] = None
        self._glue: Optional[Glue] = None
        self._redshift: Optional[Redshift] = None
        self._aurora: Optional[Aurora] = None
        self._spark: Optional[Spark] = None
        self._sagemaker: Optional[SageMaker] = None
        self._dynamodb: Optional[DynamoDB] = None

    def _load_new_boto3_session(self) -> None:
        """
        Load or reload a new Boto3 Session for the AWS Wrangler Session.

        :return: None
        """
        args = {}
        if self.profile_name:
            args["profile_name"] = self.profile_name
        if self.region_name:
            args["region_name"] = self.region_name
        if self.aws_access_key_id and self.aws_secret_access_key:
            args["aws_access_key_id"] = self.aws_access_key_id
            args["aws_secret_access_key"] = self.aws_secret_access_key
        self._boto3_session = boto3.Session(**args)
        self._profile_name = self._boto3_session.profile_name
        credentials = self._boto3_session.get_credentials()
        if credentials is None:
            raise AWSCredentialsNotFound(
                "Please run aws configure: https://docs.aws.amazon.com/cli/latest/userguide/cli-chap-configure.html")
        self._aws_access_key_id = credentials.access_key
        self._aws_secret_access_key = credentials.secret_key
        self._region_name = self._boto3_session.region_name

    def _load_new_primitives(self) -> None:
        """
        Load or reload a new AWS Wrangler Session primitives.

        :return: None
        """
        self._primitives = SessionPrimitives(profile_name=self._profile_name,
                                             aws_access_key_id=self._aws_access_key_id,
                                             aws_secret_access_key=self._aws_secret_access_key,
                                             aws_session_token=self._aws_session_token,
                                             region_name=self._region_name,
                                             botocore_max_retries=self._botocore_max_retries,
                                             s3_additional_kwargs=self._s3_additional_kwargs,
                                             botocore_config=self._botocore_config,
                                             procs_cpu_bound=self._procs_cpu_bound,
                                             procs_io_bound=self._procs_io_bound,
                                             athena_workgroup=self._athena_workgroup,
                                             athena_s3_output=self._athena_s3_output,
                                             athena_encryption=self._athena_encryption,
                                             athena_kms_key=self._athena_kms_key,
                                             athena_database=self._athena_database,
                                             athena_ctas_approach=self._athena_ctas_approach,
                                             redshift_temp_s3_path=self._redshift_temp_s3_path,
                                             aurora_temp_s3_path=self._aurora_temp_s3_path)

    @property
    def profile_name(self) -> Optional[str]:
        """profile_name property."""
        return self._profile_name

    @property
    def aws_access_key_id(self) -> Optional[str]:
        """aws_access_key_id property."""
        return self._aws_access_key_id

    @property
    def aws_secret_access_key(self) -> Optional[str]:
        """aws_secret_access_key property."""
        return self._aws_secret_access_key

    @property
    def aws_session_token(self) -> Optional[str]:
        """aws_session_token property."""
        return self._aws_session_token

    @property
    def region_name(self) -> Optional[str]:
        """region_name property."""
        return self._region_name

    @property
    def botocore_max_retries(self) -> int:
        """botocore_max_retries property."""
        return self._botocore_max_retries

    @property
    def botocore_config(self) -> Config:
        """botocore_config property."""
        return self._botocore_config

    @property
    def s3_additional_kwargs(self) -> Optional[Dict[str, str]]:
        """s3_additional_kwargs property."""
        return self._s3_additional_kwargs

    @property
    def spark_context(self):
        """spark_context property."""
        return self._spark_context

    @property
    def spark_session(self):
        """spark_session property."""
        return self._spark_session

    @property
    def procs_cpu_bound(self) -> int:
        """procs_cpu_bound property."""
        return self._procs_cpu_bound

    @property
    def procs_io_bound(self) -> int:
        """procs_io_bound property."""
        return self._procs_io_bound

    @property
    def athena_workgroup(self) -> str:
        """athena_workgroup property."""
        return self._athena_workgroup

    @property
    def athena_s3_output(self) -> Optional[str]:
        """athena_s3_output property."""
        return self._athena_s3_output

    @property
    def athena_encryption(self) -> Optional[str]:
        """athena_encryption property."""
        return self._athena_encryption

    @property
    def athena_kms_key(self) -> Optional[str]:
        """athena_kms_key property."""
        return self._athena_kms_key

    @property
    def athena_database(self) -> str:
        """athena_database property."""
        return self._athena_database

    @property
    def athena_ctas_approach(self) -> bool:
        """athena_ctas_approach property."""
        return self._athena_ctas_approach

    @property
    def redshift_temp_s3_path(self) -> Optional[str]:
        """redshift_temp_s3_path property."""
        return self._redshift_temp_s3_path

    @property
    def aurora_temp_s3_path(self) -> Optional[str]:
        """aurora_temp_s3_path property."""
        return self._aurora_temp_s3_path

    @property
    def boto3_session(self):
        """boto3_session property."""
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
    def athena(self) -> Athena:
        """Athena property."""
        if self._athena is None:
            self._athena = Athena(session=self)
        return self._athena

    @property
    def cloudwatchlogs(self) -> CloudWatchLogs:
        """Cloudwatchlogs property."""
        if self._cloudwatchlogs is None:
            self._cloudwatchlogs = CloudWatchLogs(session=self)
        return self._cloudwatchlogs

    @property
    def emr(self) -> EMR:
        """EMR property."""
        if self._emr is None:
            self._emr = EMR(session=self)
        return self._emr

    @property
    def pandas(self) -> Pandas:
        """Pandas property."""
        if self._pandas is None:
            self._pandas = Pandas(session=self)
        return self._pandas

    @property
    def glue(self) -> Glue:
        """Glue property."""
        if self._glue is None:
            self._glue = Glue(session=self)
        return self._glue

    @property
    def redshift(self) -> Redshift:
        """Redshift property."""
        if self._redshift is None:
            self._redshift = Redshift(session=self)
        return self._redshift

    @property
    def aurora(self) -> Aurora:
        """Aurora property."""
        if self._aurora is None:
            self._aurora = Aurora(session=self)
        return self._aurora

    @property
    def sagemaker(self) -> SageMaker:
        """Sagemaker property."""
        if self._sagemaker is None:
            self._sagemaker = SageMaker(session=self)
        return self._sagemaker

    @property
    def dynamodb(self) -> DynamoDB:
        """Dynamodb property."""
        if self._dynamodb is None:
            self._dynamodb = DynamoDB(session=self)
        return self._dynamodb

    @property
    def spark(self):
        """Spark property."""
        if PYSPARK_INSTALLED is False:
            self._spark = None
        elif not self._spark:
            self._spark = Spark(session=self)
        return self._spark


class SessionPrimitives:
    """
    A minimalist version of Session to store only primitive attributes.

    It is required to "share" the session attributes to other processes.
    That must be "pickable"!
    """
    def __init__(self,
                 profile_name=None,
                 aws_access_key_id=None,
                 aws_secret_access_key=None,
                 aws_session_token=None,
                 region_name=None,
                 botocore_max_retries=None,
                 s3_additional_kwargs=None,
                 botocore_config=None,
                 procs_cpu_bound=None,
                 procs_io_bound=None,
                 athena_workgroup: Optional[str] = None,
                 athena_s3_output: Optional[str] = None,
                 athena_encryption: Optional[str] = None,
                 athena_kms_key: Optional[str] = None,
                 athena_database: Optional[str] = None,
                 athena_ctas_approach: bool = False,
                 redshift_temp_s3_path: Optional[str] = None,
                 aurora_temp_s3_path: Optional[str] = None):
        """
        Most parameters inherit from Boto3.

        https://boto3.amazonaws.com/v1/documentation/api/latest/guide/configuration.html

        :param profile_name: Boto3 profile_name
        :param aws_access_key_id: Boto3 aws_access_key_id
        :param aws_secret_access_key: Boto3 aws_secret_access_key
        :param aws_session_token: Boto3 aws_session_token
        :param region_name: Boto3 region_name
        :param botocore_max_retries: Botocore max retries
        :param s3_additional_kwargs: Passed on to s3fs (https://s3fs.readthedocs.io/en/latest/#serverside-encryption)
        :param botocore_config: Botocore configurations
        :param procs_cpu_bound: number of processes that can be used in single node applications for CPU bound case (Default: os.cpu_count())
        :param procs_io_bound: number of processes that can be used in single node applications for I/O bound cases (Default: os.cpu_count() * PROCS_IO_BOUND_FACTOR)
        :param athena_workgroup: Default AWS Athena Workgroup (str)
        :param athena_database: AWS Glue/Athena database name
        :param athena_ctas_approach: Wraps the query with a CTAS
        :param athena_s3_output: AWS S3 path
        :param athena_encryption: None|'SSE_S3'|'SSE_KMS'|'CSE_KMS'
        :param athena_kms_key: For SSE-KMS and CSE-KMS , this is the KMS key ARN or ID.
        :param redshift_temp_s3_path: AWS S3 path to write temporary data (e.g. s3://...)
        :param aurora_temp_s3_path: AWS S3 path to write temporary data (e.g. s3://...)
        """
        self._profile_name: Optional[str] = profile_name
        self._aws_access_key_id: Optional[str] = aws_access_key_id
        self._aws_secret_access_key: Optional[str] = aws_secret_access_key
        self._aws_session_token: Optional[str] = aws_session_token
        self._region_name: Optional[str] = region_name
        self._botocore_max_retries: Optional[int] = botocore_max_retries
        self._s3_additional_kwargs = s3_additional_kwargs
        self._botocore_config = botocore_config
        self._procs_cpu_bound: Optional[int] = procs_cpu_bound
        self._procs_io_bound: Optional[int] = procs_io_bound
        self._athena_workgroup: Optional[str] = athena_workgroup
        self._athena_s3_output: Optional[str] = athena_s3_output
        self._athena_encryption: Optional[str] = athena_encryption
        self._athena_kms_key: Optional[str] = athena_kms_key
        self._athena_database: Optional[str] = athena_database
        self._athena_ctas_approach: bool = athena_ctas_approach
        self._redshift_temp_s3_path: Optional[str] = redshift_temp_s3_path
        self._aurora_temp_s3_path: Optional[str] = aurora_temp_s3_path

    @property
    def profile_name(self):
        """profile_name property."""
        return self._profile_name

    @property
    def aws_access_key_id(self):
        """aws_access_key_id property."""
        return self._aws_access_key_id

    @property
    def aws_secret_access_key(self):
        """aws_secret_access_key property."""
        return self._aws_secret_access_key

    @property
    def aws_session_token(self):
        """aws_session_token property."""
        return self._aws_session_token

    @property
    def region_name(self):
        """region_name property."""
        return self._region_name

    @property
    def botocore_max_retries(self):
        """botocore_max_retries property."""
        return self._botocore_max_retries

    @property
    def botocore_config(self):
        """botocore_config property."""
        return self._botocore_config

    @property
    def s3_additional_kwargs(self):
        """s3_additional_kwargs property."""
        return self._s3_additional_kwargs

    @property
    def procs_cpu_bound(self):
        """procs_cpu_bound property."""
        return self._procs_cpu_bound

    @property
    def procs_io_bound(self):
        """procs_io_bound property."""
        return self._procs_io_bound

    @property
    def athena_workgroup(self) -> Optional[str]:
        """athena_workgroup property."""
        return self._athena_workgroup

    @property
    def athena_s3_output(self) -> Optional[str]:
        """athena_s3_output property."""
        return self._athena_s3_output

    @property
    def athena_encryption(self) -> Optional[str]:
        """athena_encryption property."""
        return self._athena_encryption

    @property
    def athena_kms_key(self) -> Optional[str]:
        """athena_kms_key property."""
        return self._athena_kms_key

    @property
    def athena_database(self) -> Optional[str]:
        """athena_database property."""
        return self._athena_database

    @property
    def athena_ctas_approach(self) -> bool:
        """athena_ctas_approach property."""
        return self._athena_ctas_approach

    @property
    def redshift_temp_s3_path(self) -> Optional[str]:
        """redshift_temp_s3_path property."""
        return self._redshift_temp_s3_path

    @property
    def aurora_temp_s3_path(self) -> Optional[str]:
        """aurora_temp_s3_path property."""
        return self._aurora_temp_s3_path

    @property
    def session(self):
        """
        Reconstruct the session from primitives.

        :return: awswrangler.session.Session
        """
        return Session(profile_name=self._profile_name,
                       aws_access_key_id=self._aws_access_key_id,
                       aws_secret_access_key=self._aws_secret_access_key,
                       aws_session_token=self._aws_session_token,
                       region_name=self._region_name,
                       botocore_max_retries=self._botocore_max_retries,
                       s3_additional_kwargs=self._s3_additional_kwargs,
                       procs_cpu_bound=self._procs_cpu_bound,
                       procs_io_bound=self._procs_io_bound,
                       athena_workgroup=self._athena_workgroup,
                       athena_s3_output=self._athena_s3_output,
                       athena_encryption=self._athena_encryption,
                       athena_kms_key=self._athena_kms_key,
                       athena_database=self._athena_database,
                       athena_ctas_approach=self._athena_ctas_approach,
                       redshift_temp_s3_path=self._redshift_temp_s3_path,
                       aurora_temp_s3_path=self._aurora_temp_s3_path)
