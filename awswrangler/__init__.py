"""Initial Module.

Source repository: https://github.com/awslabs/aws-data-wrangler
Documentation: https://aws-data-wrangler.readthedocs.io/

"""

import logging as _logging

from awswrangler import athena, catalog, cloudwatch, db, emr, exceptions, quicksight, s3, sts  # noqa
from awswrangler.__metadata__ import __description__, __license__, __title__, __version__  # noqa
from awswrangler._config import config

_logging.getLogger("awswrangler").addHandler(_logging.NullHandler())
