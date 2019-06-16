import os

import boto3

from awswrangler.s3 import S3
from awswrangler.athena import Athena
from awswrangler.pandas import Pandas
from awswrangler.glue import Glue
from awswrangler.redshift import Redshift


class Session:
    """
    A session stores configuration state (e.g. Boto3.Session,
    pyspark.sql.SparkSession, pyspark.SparkContext,
    AWS Glue Connections attributes, number of cpu cores that can be used, etc)
    """

    def __init__(
        self,
        boto3_session=None,
        profile_name=None,
        aws_access_key_id=None,
        aws_secret_access_key=None,
        aws_session_token=None,
        region_name="us-east-1",
        spark_context=None,
        spark_session=None,
        cpu_count=os.cpu_count(),
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
        :param region_name: Boto3 region_name (Default: us-east-1)
        :param spark_context: Spark Context (pyspark.SparkContext)
        :param spark_session: Spark Session (pyspark.sql.SparkSession)
        :param cpu_count: number of cpu cores that can be used in single node applications (Default: os.cpu_count())
        """
        self._profile_name = (
            boto3_session.profile_name if boto3_session else profile_name
        )
        self._aws_access_key_id = (
            boto3_session.get_credentials().access_key
            if boto3_session
            else aws_access_key_id
        )
        self._aws_secret_access_key = (
            boto3_session.get_credentials().secret_key
            if boto3_session
            else aws_secret_access_key
        )
        self._aws_session_token = aws_session_token
        self._region_name = boto3_session.region_name if boto3_session else region_name
        self._spark_context = spark_context
        self._spark_session = spark_session
        self._cpu_count = cpu_count
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

    def _load_new_boto3_session(self):
        """
        Load or reload a new Boto3 Session for the AWS Wrangler Session
        :return: None
        """
        if self.profile_name:
            self._boto3_session = boto3.Session(
                region_name=self.region_name, profile_name=self.profile_name
            )
        elif self.aws_access_key_id and self.aws_secret_access_key:
            self._boto3_session = boto3.Session(
                region_name=self.region_name,
                aws_access_key_id=self.aws_access_key_id,
                aws_secret_access_key=self.aws_secret_access_key,
            )
        else:
            self._boto3_session = boto3.Session(region_name=self.region_name)
        self._profile_name = self._boto3_session.profile_name
        self._aws_access_key_id = self._boto3_session.get_credentials().access_key
        self._aws_secret_access_key = self._boto3_session.get_credentials().secret_key
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
            cpu_count=self._cpu_count,
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
    def spark_context(self):
        return self._spark_context

    @property
    def spark_session(self):
        return self._spark_session

    @property
    def cpu_count(self):
        return self._cpu_count

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
        cpu_count=None,
    ):
        """
        Most parameters inherit from Boto3.
        https://boto3.amazonaws.com/v1/documentation/api/latest/guide/configuration.html

        :param profile_name: Boto3 profile_name
        :param aws_access_key_id: Boto3 aws_access_key_id
        :param aws_secret_access_key: Boto3 aws_secret_access_key
        :param aws_session_token: Boto3 aws_session_token
        :param region_name: Boto3 region_name
        :param cpu_count: number of cpu cores that can be used in single node applications
        """
        self._profile_name = profile_name
        self._aws_access_key_id = aws_access_key_id
        self._aws_secret_access_key = aws_secret_access_key
        self._aws_session_token = aws_session_token
        self._region_name = region_name
        self._cpu_count = cpu_count

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
    def cpu_count(self):
        return self._cpu_count

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
            cpu_count=self._cpu_count,
        )
