"""Module with parameter types."""

import sys
from typing import Dict, List, Literal, TypedDict

if sys.version_info >= (3, 11):
    from typing import NotRequired, Required
else:
    from typing_extensions import NotRequired, Required


class GlueTableSettings(TypedDict):
    """Class defining the settings for the Glue table."""

    table_type: NotRequired[Literal["EXTERNAL_TABLE", "GOVERNED"]]
    transaction_id: NotRequired[str]
    description: NotRequired[str]
    parameters: NotRequired[Dict[str, str]]
    columns_comments: NotRequired[Dict[str, str]]
    regular_partitions: NotRequired[bool]


class _S3WriteDataReturnValue(TypedDict):
    """Class defining the dictionary returned by S3 write functions."""

    paths: Required[List[str]]
    partitions_values: Required[Dict[str, List[str]]]
