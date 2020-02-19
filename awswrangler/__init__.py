"""Initial Module.

Source repository: https://github.com/awslabs/aws-data-wrangler
Documentation: https://aws-data-wrangler.readthedocs.io/

"""

from logging import NullHandler, getLogger
from typing import Optional, Type

from awswrangler import exceptions  # noqa
from awswrangler import utils  # noqa
from awswrangler.__metadata__ import __description__, __license__, __title__, __version__  # noqa
from awswrangler.s3 import S3 as _S3
from awswrangler.session import Session


class _LazyDefaultSession:
    """Class to instantiate classes with the default Session dynamically and lazily."""

    __default_session: Optional[Session] = None

    @staticmethod
    def get_class(class_ref: Type[_S3]) -> _S3:
        """Return an instance of the received class.

        Also instantiate the default Session if necessary.

        Parameters
        ----------
        class_ref
            Reference to the class that will be instantiate.

        """
        if _LazyDefaultSession.__default_session is None:
            _LazyDefaultSession.__default_session = Session()
        return class_ref(_LazyDefaultSession.__default_session)


s3: _S3 = _LazyDefaultSession().get_class(_S3)

getLogger("awswrangler").addHandler(NullHandler())
