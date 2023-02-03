"""Module with parameter types."""

import sys
from typing import Dict, List, Literal, Tuple, TypedDict

if sys.version_info >= (3, 11):
    from typing import NotRequired, Required
else:
    from typing_extensions import NotRequired, Required


BucketingInfoTuple = Tuple[List[str], int]


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


class AthenaCTASSettings(TypedDict):
    """Typed dictionary defining the settings for using CTAS (Create Table As Statement)."""

    ctas_database: NotRequired[str]
    """
    The name of the alternative database where the CTAS temporary table is stored.
    If None, the default `database` is used.
    """
    ctas_temp_table_name: NotRequired[str]
    """
    The name of the temporary table and also the directory name on S3 where the CTAS result is stored.
    If None, it will use the follow random pattern: `f"temp_table_{uuid.uuid4().hex()}"`.
    On S3 this directory will be under under the pattern: `f"{s3_output}/{ctas_temp_table_name}/"`.
    """
    ctas_bucketing_info: NotRequired[BucketingInfoTuple]
    """
    Tuple consisting of the column names used for bucketing as the first element and the number of buckets as the
    second element.
    Only `str`, `int` and `bool` are supported as column data types for bucketing.
    """
    ctas_write_compression: NotRequired[str]
    """
    Write compression for the temporary table where the CTAS result is stored.
    Corresponds to the `write_compression` parameters for CREATE TABLE AS statement in Athena.
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
