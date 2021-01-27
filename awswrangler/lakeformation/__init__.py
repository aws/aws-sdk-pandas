"""Amazon Lake Formation Module."""

from awswrangler.lakeformation._read import read_sql_query  # noqa
from awswrangler.lakeformation._utils import wait_query  # noqa

__all__ = [
    "read_sql_query",
    "wait_query",
]
