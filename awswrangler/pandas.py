"""Pandas "proxy" module."""

import datetime

from awswrangler._distributed import MemoryFormatEnum, memory_format

if memory_format.get() == MemoryFormatEnum.PANDAS:
    from pandas import *  # noqa: F403
elif memory_format.get() == MemoryFormatEnum.MODIN:
    from modin.pandas import *  # noqa: F403
else:
    raise ImportError(f"Unknown memory format {memory_format}.")
