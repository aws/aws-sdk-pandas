import os
import logging
import importlib

import boto3
from botocore.config import Config

from awswrangler.s3 import S3
from awswrangler.athena import Athena
from awswrangler.pandas import Pandas
from awswrangler.glue import Glue
from awswrangler.redshift import Redshift

PYSPARK_INSTALLED = False
if importlib.util.find_spec("pyspark"):
    PYSPARK_INSTALLED = True
    from awswrangler.spark import Spark

logger = logging.getLogger(__name__)


class Session:
    """
    A session stores configuration state (e.g. Boto3.Session,
    pyspark.sql.SparkSession, pyspark.SparkContext,
    AWS Glue Connections attributes, number of cpu cores that can be used, etc)
    """

    PROCS_IO_BOUND_FACTOR = 2

    def __init__(
            self,
            boto3_session=None,
            profile_name=None,
            aws_access_key_id=None,
            aws_secret_access_key=None,
            aws_session_token=None,
            region_name=None,
            botocore_max_retries=40,
            spark_context=None,
            spark_session=None,
            procs_cpu_bound=os.cpu_count(),
            procs_io_bound=os.cpu_count() * PROCS_IO_BOUND_FACTOR,
    ):
        """
        Most parameters inherit from Boto3 ou Pyspark.
        https://boto3.amazonaws.com/v1/documentation/api/latest/guide/configuration.html
        https://spark.apache.org/docs/latest/api/python/index.html

        :param boto3_session: Boto3.Session (Overwrite the others Boto3 parameters)
        :param profile_name: Boto3 profile_name
        :param aws_access_key_id: Boto3 aws_access_key_id
        :param aws_secret_access_key: Boto3 aws_secret_access_key
        :param aws_session_token: Boto3 aws_session_token
        :param region_name: Boto3 region_name
        :param botocore_max_retries: Botocore max retries
        :param spark_context: Spark Context (pyspark.SparkContext)
        :param spark_session: Spark Session (pyspark.sql.SparkSession)
        :param procs_cpu_bound: number of processes that can be used in single
        node applications for CPU bound case (Default: os.cpu_count())
        :param procs_io_bound: number of processes that can be used in single
        node applications for I/O bound cases (Default: os.cpu_count() * PROCS_IO_BOUND_FACTOR)
        """
        self._profile_name = (boto3_session.profile_name
                              if boto3_session else profile_name)
        self._aws_access_key_id = (boto3_session.get_credentials().access_key
                                   if boto3_session else aws_access_key_id)
        self._aws_secret_access_key = (
            boto3_session.get_credentials().secret_key
            if boto3_session else aws_secret_access_key)
        self._botocore_max_retries = botocore_max_retries
        self._botocore_config = Config(
            retries={"max_attempts": self._botocore_max_retries})
        self._aws_session_token = aws_session_token
        self._region_name = boto3_session.region_name if boto3_session else region_name
        self._spark_context = spark_context
        self._spark_session = spark_session
        self._procs_cpu_bound = procs_cpu_bound
        self._procs_io_bound = procs_io_bound
        self._primitives = None
        self._load_new_primitives()
        if boto3_session:
            self._boto3_session = boto3_session
        else:
            self._load_new_boto3_session()
        self._s3 = None
        self._athena = None
        self._pandas = None
        self._glue = None
        self._redshift = None
        self._spark = None

    def _load_new_boto3_session(self):
        """
        Load or reload a new Boto3 Session for the AWS Wrangler Session
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
        self._aws_access_key_id = self._boto3_session.get_credentials(
        ).access_key
        self._aws_secret_access_key = self._boto3_session.get_credentials(
        ).secret_key
        self._region_name = self._boto3_session.region_name

    def _load_new_primitives(self):
        """
        Load or reload a new AWS Wrangler Session primitives
        :return: None
        """
        self._primitives = SessionPrimitives(
            profile_name=self._profile_name,
            aws_access_key_id=self._aws_access_key_id,
            aws_secret_access_key=self._aws_secret_access_key,
            aws_session_token=self._aws_session_token,
            region_name=self._region_name,
            botocore_max_retries=self._botocore_max_retries,
            botocore_config=self._botocore_config,
            procs_cpu_bound=self._procs_cpu_bound,
            procs_io_bound=self._procs_io_bound,
        )

    @property
    def profile_name(self):
        return self._profile_name

    @property
    def aws_access_key_id(self):
        return self._aws_access_key_id

    @property
    def aws_secret_access_key(self):
        return self._aws_secret_access_key

    @property
    def aws_session_token(self):
        return self._aws_session_token

    @property
    def region_name(self):
        return self._region_name

    @property
    def botocore_max_retries(self):
        return self._botocore_max_retries

    @property
    def botocore_config(self):
        return self._botocore_config

    @property
    def spark_context(self):
        return self._spark_context

    @property
    def spark_session(self):
        return self._spark_session

    @property
    def procs_cpu_bound(self):
        return self._procs_cpu_bound

    @property
    def procs_io_bound(self):
        return self._procs_io_bound

    @property
    def boto3_session(self):
        return self._boto3_session

    @property
    def primitives(self):
        return self._primitives

    @property
    def s3(self):
        if not self._s3:
            self._s3 = S3(session=self)
        return self._s3

    @property
    def athena(self):
        if not self._athena:
            self._athena = Athena(session=self)
        return self._athena

    @property
    def pandas(self):
        if not self._pandas:
            self._pandas = Pandas(session=self)
        return self._pandas

    @property
    def glue(self):
        if not self._glue:
            self._glue = Glue(session=self)
        return self._glue

    @property
    def redshift(self):
        if not self._redshift:
            self._redshift = Redshift(session=self)
        return self._redshift

    @property
    def spark(self):
        if not PYSPARK_INSTALLED:
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
    def __init__(
            self,
            profile_name=None,
            aws_access_key_id=None,
            aws_secret_access_key=None,
            aws_session_token=None,
            region_name=None,
            botocore_max_retries=None,
            botocore_config=None,
            procs_cpu_bound=None,
            procs_io_bound=None,
    ):
        """
        Most parameters inherit from Boto3.
        https://boto3.amazonaws.com/v1/documentation/api/latest/guide/configuration.html

        :param profile_name: Boto3 profile_name
        :param aws_access_key_id: Boto3 aws_access_key_id
        :param aws_secret_access_key: Boto3 aws_secret_access_key
        :param aws_session_token: Boto3 aws_session_token
        :param region_name: Boto3 region_name
        :param botocore_max_retries: Botocore max retries
        :param botocore_config: Botocore configurations
        :param procs_cpu_bound: number of processes that can be used in single
        node applications for CPU bound case (Default: os.cpu_count())
        :param procs_io_bound: number of processes that can be used in single
        node applications for I/O bound cases (Default: os.cpu_count() * PROCS_IO_BOUND_FACTOR)
        """
        self._profile_name = profile_name
        self._aws_access_key_id = aws_access_key_id
        self._aws_secret_access_key = aws_secret_access_key
        self._aws_session_token = aws_session_token
        self._region_name = region_name
        self._botocore_max_retries = botocore_max_retries
        self._botocore_config = botocore_config
        self._procs_cpu_bound = procs_cpu_bound
        self._procs_io_bound = procs_io_bound

    @property
    def profile_name(self):
        return self._profile_name

    @property
    def aws_access_key_id(self):
        return self._aws_access_key_id

    @property
    def aws_secret_access_key(self):
        return self._aws_secret_access_key

    @property
    def aws_session_token(self):
        return self._aws_session_token

    @property
    def region_name(self):
        return self._region_name

    @property
    def botocore_max_retries(self):
        return self._botocore_max_retries

    @property
    def botocore_config(self):
        return self._botocore_config

    @property
    def procs_cpu_bound(self):
        return self._procs_cpu_bound

    @property
    def procs_io_bound(self):
        return self._procs_io_bound

    @property
    def session(self):
        """
        Reconstruct the session from primitives
        :return: awswrangler.session.Session
        """
        return Session(
            profile_name=self._profile_name,
            aws_access_key_id=self._aws_access_key_id,
            aws_secret_access_key=self._aws_secret_access_key,
            aws_session_token=self._aws_session_token,
            region_name=self._region_name,
            botocore_max_retries=self._botocore_max_retries,
            procs_cpu_bound=self._procs_cpu_bound,
            procs_io_bound=self._procs_io_bound,
        )
