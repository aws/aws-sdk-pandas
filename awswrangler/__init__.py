"""Initial Module.

Source repository: https://github.com/awslabs/aws-data-wrangler
Documentation: https://aws-data-wrangler.readthedocs.io/

"""

import importlib
import logging

from awswrangler import athena, catalog, cloudwatch, db, emr, exceptions, s3  # noqa
from awswrangler.__metadata__ import __description__, __license__, __title__, __version__  # noqa

if (
    importlib.util.find_spec("torch")
    and importlib.util.find_spec("torchvision")
    and importlib.util.find_spec("torchaudio")
    and importlib.util.find_spec("PIL")
):  # type: ignore
    from awswrangler import torch  # noqa

logging.getLogger("awswrangler").addHandler(logging.NullHandler())
