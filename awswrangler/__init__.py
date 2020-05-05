"""Initial Module.

Source repository: https://github.com/awslabs/aws-data-wrangler
Documentation: https://aws-data-wrangler.readthedocs.io/

"""

import logging
from importlib.util import find_spec

from awswrangler import athena, catalog, cloudwatch, db, emr, exceptions, s3  # noqa
from awswrangler.__metadata__ import __description__, __license__, __title__, __version__  # noqa
from awswrangler._utils import get_account_id  # noqa

if find_spec("torch") and find_spec("torchvision") and find_spec("torchaudio") and find_spec("PIL"):
    from awswrangler import torch  # noqa

logging.getLogger("awswrangler").addHandler(logging.NullHandler())
