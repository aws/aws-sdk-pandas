import itertools
import json
import logging
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional, Union


import boto3

import awswrangler.pandas as pd
from awswrangler import _utils
from awswrangler._config import apply_configs
from awswrangler._distributed import engine
from awswrangler._executor import _get_executor
from awswrangler.distributed.ray import ray_get

_logger: logging.Logger = logging.getLogger(__name__)




