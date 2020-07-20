"""Amazon CSV S3 Write Module (PRIVATE)."""

import logging
from typing import Dict, Optional

_logger: logging.Logger = logging.getLogger(__name__)

_COMPRESSION_2_EXT: Dict[Optional[str], str] = {None: "", "gzip": ".gz", "snappy": ".snappy"}
