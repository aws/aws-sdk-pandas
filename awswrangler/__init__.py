"""Initial Module.

Source repository: https://github.com/aws/aws-sdk-pandas
Documentation: https://aws-sdk-pandas.readthedocs.io/

"""

import logging as _logging

from awswrangler import (  # noqa
    athena,
    catalog,
    chime,
    cloudwatch,
    data_api,
    dynamodb,
    emr,
    exceptions,
    lakeformation,
    mysql,
    neptune,
    opensearch,
    oracle,
    postgresql,
    quicksight,
    redshift,
    s3,
    secretsmanager,
    sqlserver,
    sts,
    timestream,
    typing,
)
from awswrangler.__metadata__ import __description__, __license__, __title__, __version__  # noqa
from awswrangler._config import config  # noqa
from awswrangler._distributed import engine, memory_format  # noqa

engine.initialize()

__all__ = [
    "athena",
    "catalog",
    "chime",
    "cloudwatch",
    "emr",
    "data_api",
    "dynamodb",
    "exceptions",
    "opensearch",
    "oracle",
    "quicksight",
    "s3",
    "sts",
    "redshift",
    "lakeformation",
    "mysql",
    "neptune",
    "postgresql",
    "secretsmanager",
    "sqlserver",
    "config",
    "engine",
    "memory_format",
    "timestream",
    "typing",
    "__description__",
    "__license__",
    "__title__",
    "__version__",
]


_logging.getLogger("awswrangler").addHandler(_logging.NullHandler())
