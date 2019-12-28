import logging

logger = logging.getLogger(__name__)


class Aurora:
    def __init__(self, session):
        self._session = session
