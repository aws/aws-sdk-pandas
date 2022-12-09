"""Module with parameter types."""

from dataclasses import dataclass
from typing import Dict, Optional


@dataclass
class GlueCatalogParameters:  # pylint: disable=too-many-instance-attributes
    """Class defining the parameters for writing to a Glue Catalogue."""

    database: str
    table: str
    table_type: Optional[str] = None
    transaction_id: Optional[str] = None
    description: Optional[str] = None
    parameters: Optional[Dict[str, str]] = None
    columns_comments: Optional[Dict[str, str]] = None
    catalog_id: Optional[str] = None
    regular_partitions: bool = True

    def __post_init__(self) -> None:
        """Post-init function."""
        if self.transaction_id:
            self.table_type = "GOVERNED"
