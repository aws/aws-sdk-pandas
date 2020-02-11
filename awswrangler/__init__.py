"""Initial Module."""

import importlib
from logging import NullHandler, getLogger
from sys import version_info
from typing import Optional

import awswrangler.data_types  # noqa
import awswrangler.utils  # noqa
from awswrangler.__version__ import __description__, __title__, __version__  # noqa
from awswrangler.athena import Athena  # noqa
from awswrangler.aurora import Aurora  # noqa
from awswrangler.cloudwatchlogs import CloudWatchLogs  # noqa
from awswrangler.dynamodb import DynamoDB  # noqa
from awswrangler.emr import EMR  # noqa
from awswrangler.glue import Glue  # noqa
from awswrangler.pandas import Pandas  # noqa
from awswrangler.redshift import Redshift  # noqa
from awswrangler.s3 import S3  # noqa
from awswrangler.sagemaker import SageMaker  # noqa
from awswrangler.session import Session  # noqa


class DynamicInstantiate:
    """
    Class to instantiate the default Session.

    https://github.com/awslabs/aws-data-wrangler
    """

    __default_session: Optional[Session] = None

    def __dir__(self):
        """Override __dir__."""
        return self._class_ref.__dict__.keys()

    def __repr__(self):
        """Override __repr__."""
        return repr(self._class_ref)

    def __init__(self, module_name, class_ref):
        """
        Instantiate the default Session.

        :param module_name: Target module name
        :param class_ref: Target module reference
        """
        self._module_name: str = module_name
        self._class_ref = class_ref

    def __getattr__(self, name):
        """Override __getattr__."""
        if DynamicInstantiate.__default_session is None:
            DynamicInstantiate.__default_session = Session()
        return getattr(getattr(DynamicInstantiate.__default_session, self._module_name), name)


if version_info < (3, 8) and importlib.util.find_spec("pyspark"):  # type: ignore
    from awswrangler.spark import Spark  # noqa
    spark: Spark = DynamicInstantiate("spark", Spark)  # type: ignore

s3: S3 = DynamicInstantiate("s3", S3)  # type: ignore
emr: EMR = DynamicInstantiate("emr", EMR)  # type: ignore
glue: Glue = DynamicInstantiate("glue", Glue)  # type: ignore
pandas: Pandas = DynamicInstantiate("pandas", Pandas)  # type: ignore
athena: Athena = DynamicInstantiate("athena", Athena)  # type: ignore
aurora: Aurora = DynamicInstantiate("aurora", Aurora)  # type: ignore
redshift: Redshift = DynamicInstantiate("redshift", Redshift)  # type: ignore
dynamodb: DynamoDB = DynamicInstantiate("dynamodb", DynamoDB)  # type: ignore
sagemaker: SageMaker = DynamicInstantiate("sagemaker", SageMaker)  # type: ignore
cloudwatchlogs: CloudWatchLogs = DynamicInstantiate("cloudwatchlogs", CloudWatchLogs)  # type: ignore

getLogger("awswrangler").addHandler(NullHandler())
