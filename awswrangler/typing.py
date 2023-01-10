"""Module with parameter types."""

from dataclasses import dataclass
from typing import Dict, TypedDict

from typing_extensions import NotRequired


@dataclass
class GlueCatalogParameters(TypedDict):
    """Class defining the parameters for writing to a Glue Catalogue."""

    table_type: NotRequired[str]
    transaction_id: NotRequired[str]
    description: NotRequired[str]
    parameters: NotRequired[Dict[str, str]]
    columns_comments: NotRequired[Dict[str, str]]
    catalog_id: NotRequired[str]
    regular_partitions: NotRequired[bool]
