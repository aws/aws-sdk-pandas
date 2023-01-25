"""Amazon Clean Rooms Module."""

from awswrangler.cleanrooms._read import read_sql_query
from awswrangler.cleanrooms._utils import wait_query

__all__ = [
    "read_sql_query",
    "wait_query",
]
