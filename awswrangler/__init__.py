import logging
import importlib

from awswrangler.__version__ import __title__, __description__, __version__  # noqa
from awswrangler.session import Session  # noqa
from awswrangler.pandas import Pandas  # noqa
from awswrangler.s3 import S3  # noqa
from awswrangler.athena import Athena  # noqa
from awswrangler.cloudwatchlogs import CloudWatchLogs  # noqa
from awswrangler.glue import Glue  # noqa
from awswrangler.redshift import Redshift  # noqa
from awswrangler.emr import EMR  # noqa
from awswrangler.sagemaker import SageMaker  # noqa
import awswrangler.utils  # noqa
import awswrangler.data_types  # noqa


class DynamicInstantiate:

    __default_session = Session()

    def __init__(self, service):
        self._service = service

    def __getattr__(self, name):
        return getattr(getattr(DynamicInstantiate.__default_session, self._service), name)


if importlib.util.find_spec("pyspark"):  # type: ignore
    from awswrangler.spark import Spark  # noqa

s3 = DynamicInstantiate("s3")
emr = DynamicInstantiate("emr")
glue = DynamicInstantiate("glue")
spark = DynamicInstantiate("spark")
pandas = DynamicInstantiate("pandas")
athena = DynamicInstantiate("athena")
redshift = DynamicInstantiate("redshift")
sagemaker = DynamicInstantiate("sagemaker")
cloudwatchlogs = DynamicInstantiate("cloudwatchlogs")

logging.getLogger("awswrangler").addHandler(logging.NullHandler())
