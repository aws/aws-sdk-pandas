"""Initial Module.

Source repository: https://github.com/awslabs/aws-data-wrangler
Documentation: https://aws-data-wrangler.readthedocs.io/

"""

from logging import NullHandler, getLogger

from awswrangler import exceptions  # noqa
from awswrangler import utils  # noqa
from awswrangler.__metadata__ import __version__  # noqa
from awswrangler.s3 import S3 as _S3
from awswrangler.session import Session

default_session: Session = Session()

s3: _S3 = default_session.s3

getLogger("awswrangler").addHandler(NullHandler())
