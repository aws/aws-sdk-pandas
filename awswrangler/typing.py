"""Module with parameter types."""

import sys
from typing import Dict, List, Literal, TypedDict

if sys.version_info >= (3, 11):
    from typing import NotRequired, Required
else:
    from typing_extensions import NotRequired, Required


class GlueTableSettings(TypedDict):
    """Typed dictionary defining the settings for the Glue table."""

    table_type: NotRequired[Literal["EXTERNAL_TABLE", "GOVERNED"]]
    """The type of the Glue Table. Set to EXTERNAL_TABLE if None."""
    transaction_id: NotRequired[str]
    """The ID of the transaction when writing to a Governed Table."""
    description: NotRequired[str]
    """Glue/Athena catalog: Table description"""
    parameters: NotRequired[Dict[str, str]]
    """Glue/Athena catalog: Key/value pairs to tag the table."""
    columns_comments: NotRequired[Dict[str, str]]
    """
    Columns names and the related comments
    (e.g. {'col0': 'Column 0.', 'col1': 'Column 1.', 'col2': 'Partition.'}).
    """
    regular_partitions: NotRequired[bool]
    """
    Create regular partitions (Non projected partitions) on Glue Catalog.
    Disable when you will work only with Partition Projection.
    Keep enabled even when working with projections is useful to keep
    Redshift Spectrum working with the regular partitions.
    """


class _S3WriteDataReturnValue(TypedDict):
    """Typed dictionary defining the dictionary returned by S3 write functions."""

    paths: Required[List[str]]
    """List of all stored files paths on S3."""
    partitions_values: Required[Dict[str, List[str]]]
    """
    Dictionary of partitions added with keys as S3 path locations
    and values as a list of partitions values as str.
    """
