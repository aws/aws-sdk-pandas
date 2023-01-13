"""Module with parameter types."""

from typing import Dict, List, Literal, TypedDict

from typing_extensions import NotRequired, Required


class GlueTableSettings(TypedDict):
    """Class defining the parameters for writing to a Glue Catalogue."""

    table_type: NotRequired[Literal["EXTERNAL_TABLE", "GOVERNED"]]
    transaction_id: NotRequired[str]
    description: NotRequired[str]
    parameters: NotRequired[Dict[str, str]]
    columns_comments: NotRequired[Dict[str, str]]
    regular_partitions: NotRequired[bool]


class S3WriteDataReturnValue(TypedDict):
    """Class defining the dictionary returned by S3 write functions."""

    paths: Required[List[str]]
    partitions_values: Required[Dict[str, List[str]]]
