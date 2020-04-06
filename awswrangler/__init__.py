"""Initial Module.

Source repository: https://github.com/awslabs/aws-data-wrangler
Documentation: https://aws-data-wrangler.readthedocs.io/

"""

import logging

from awswrangler import athena, catalog, cloudwatch, db, emr, exceptions, s3  # noqa
from awswrangler.__metadata__ import __description__, __license__, __title__, __version__  # noqa

logging.getLogger("awswrangler").addHandler(logging.NullHandler())
