"""Initial Module.

Source repository: https://github.com/awslabs/aws-data-wrangler
Documentation: https://aws-data-wrangler.readthedocs.io/

"""

import logging as _logging

from awswrangler import (  # noqa
    athena,
    catalog,
    chime,
    cloudwatch,
    dynamodb,
    emr,
    exceptions,
    mysql,
    postgresql,
    quicksight,
    redshift,
    s3,
    secretsmanager,
    sqlserver,
    sts,
    timestream,
)
from awswrangler.__metadata__ import __description__, __license__, __title__, __version__  # noqa
from awswrangler._config import config  # noqa

__all__ = [
    "athena",
    "catalog",
    "chime",
    "cloudwatch",
    "emr",
    "dynamodb",
    "exceptions",
    "quicksight",
    "s3",
    "sts",
    "redshift",
    "mysql",
    "postgresql",
    "secretsmanager",
    "sqlserver",
    "config",
    "timestream",
    "__description__",
    "__license__",
    "__title__",
    "__version__",
]


_logging.getLogger("awswrangler").addHandler(_logging.NullHandler())
