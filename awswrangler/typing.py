"""Module with parameter types."""

from __future__ import annotations

from typing import List, Literal, NamedTuple, Tuple, TypedDict

import pyarrow
from typing_extensions import NotRequired, Required

BucketingInfoTuple = Tuple[List[str], int]


class GlueTableSettings(TypedDict):
    """Typed dictionary defining the settings for the Glue table."""

    table_type: NotRequired[Literal["EXTERNAL_TABLE"]]
    """The type of the Glue Table. Set to EXTERNAL_TABLE if None."""
    description: NotRequired[str]
    """Glue/Athena catalog: Table description"""
    parameters: NotRequired[dict[str, str]]
    """Glue/Athena catalog: Key/value pairs to tag the table."""
    columns_comments: NotRequired[dict[str, str]]
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

    database: NotRequired[str]
    """
    The name of the alternative database where the CTAS temporary table is stored.
    If None, the default `database` is used.
    """
    temp_table_name: NotRequired[str]
    """
    The name of the temporary table and also the directory name on S3 where the CTAS result is stored.
    If None, it will use the follow random pattern: `f"temp_table_{uuid.uuid4().hex()}"`.
    On S3 this directory will be under under the pattern: `f"{s3_output}/{ctas_temp_table_name}/"`.
    """
    bucketing_info: NotRequired[BucketingInfoTuple]
    """
    Tuple consisting of the column names used for bucketing as the first element and the number of buckets as the
    second element.
    Only `str`, `int` and `bool` are supported as column data types for bucketing.
    """
    compression: NotRequired[str]
    """
    Write compression for the temporary table where the CTAS result is stored.
    Corresponds to the `write_compression` parameters for CREATE TABLE AS statement in Athena.
    """


class AthenaUNLOADSettings(TypedDict):
    """Typed dictionary defining the settings for using UNLOAD."""

    file_format: NotRequired[str]
    """
    Specifies the file format of the output. Only `PARQUET` is currently supported.
    """
    compression: NotRequired[str]
    """
    This option is specific to the ORC and Parquet formats.
    For ORC, possible values are lz4, snappy, zlib, or zstd.
    For Parquet, possible values are gzip or snappy. For ORC, the default is zlib, and for Parquet, the default is gzip.
    """
    field_delimiter: NotRequired[str]
    """
    Specifies a single-character field delimiter for files in CSV, TSV, and other text formats.
    """
    partitioned_by: NotRequired[list[str]]
    """
    A list of columns by which the output is partitioned.
    """


class AthenaCacheSettings(TypedDict):
    """Typed dictionary defining the settings for using cached Athena results."""

    max_cache_seconds: NotRequired[int]
    """
    awswrangler can look up in Athena's history if this table has been read before.
    If so, and its completion time is less than `max_cache_seconds` before now, awswrangler
    skips query execution and just returns the same results as last time.
    """
    max_cache_query_inspections: NotRequired[int]
    """
    Max number of queries that will be inspected from the history to try to find some result to reuse.
    The bigger the number of inspection, the bigger will be the latency for not cached queries.
    Only takes effect if max_cache_seconds > 0.
    """
    max_remote_cache_entries: NotRequired[int]
    """
    Max number of queries that will be retrieved from AWS for cache inspection.
    The bigger the number of inspection, the bigger will be the latency for not cached queries.
    Only takes effect if max_cache_seconds > 0 and default value is 50.
    """
    max_local_cache_entries: NotRequired[int]
    """
    Max number of queries for which metadata will be cached locally. This will reduce the latency and also
    enables keeping more than `max_remote_cache_entries` available for the cache. This value should not be
    smaller than max_remote_cache_entries.
    Only takes effect if max_cache_seconds > 0 and default value is 100.
    """


class AthenaPartitionProjectionSettings(TypedDict):
    """
    Typed dictionary defining the settings for Athena Partition Projection.

    https://docs.aws.amazon.com/athena/latest/ug/partition-projection.html
    """

    projection_types: NotRequired[dict[str, Literal["enum", "integer", "date", "injected"]]]
    """
    Dictionary of partitions names and Athena projections types.
    Valid types: "enum", "integer", "date", "injected"
    https://docs.aws.amazon.com/athena/latest/ug/partition-projection-supported-types.html
    (e.g. {'col_name': 'enum', 'col2_name': 'integer'})
    """
    projection_ranges: NotRequired[dict[str, str]]
    """
    Dictionary of partitions names and Athena projections ranges.
    https://docs.aws.amazon.com/athena/latest/ug/partition-projection-supported-types.html
    (e.g. {'col_name': '0,10', 'col2_name': '-1,8675309'})
    """
    projection_values: NotRequired[dict[str, str]]
    """
    Dictionary of partitions names and Athena projections values.
    https://docs.aws.amazon.com/athena/latest/ug/partition-projection-supported-types.html
    (e.g. {'col_name': 'A,B,Unknown', 'col2_name': 'foo,boo,bar'})
    """
    projection_intervals: NotRequired[dict[str, str]]
    """
    Dictionary of partitions names and Athena projections intervals.
    https://docs.aws.amazon.com/athena/latest/ug/partition-projection-supported-types.html
    (e.g. {'col_name': '1', 'col2_name': '5'})
    """
    projection_digits: NotRequired[dict[str, str]]
    """
    Dictionary of partitions names and Athena projections digits.
    https://docs.aws.amazon.com/athena/latest/ug/partition-projection-supported-types.html
    (e.g. {'col_name': '1', 'col2_name': '2'})
    """
    projection_formats: NotRequired[dict[str, str]]
    """
    Dictionary of partitions names and Athena projections formats.
    https://docs.aws.amazon.com/athena/latest/ug/partition-projection-supported-types.html
    (e.g. {'col_date': 'yyyy-MM-dd', 'col2_timestamp': 'yyyy-MM-dd HH:mm:ss'})
    """
    projection_storage_location_template: NotRequired[str]
    """
    Value which is allows Athena to properly map partition values if the S3 file locations do not follow
    a typical `.../column=value/...` pattern.
    https://docs.aws.amazon.com/athena/latest/ug/partition-projection-setting-up.html
    (e.g. s3://bucket/table_root/a=${a}/${b}/some_static_subdirectory/${c}/)
    """


class TimestreamBatchLoadReportS3Configuration(TypedDict):
    """
    Report configuration for a batch load task. This contains details about where error reports are stored.

    https://docs.aws.amazon.com/timestream/latest/developerguide/API_ReportS3Configuration.html
    """

    BucketName: Required[str]
    """
    The name of the bucket where the error reports are stored.
    """
    ObjectKeyPrefix: NotRequired[str]
    """
    Optional S3 prefix for the error reports.
    """
    Encryption: NotRequired[Literal["SSE_S3", "SSE_KMS"]]
    """
    Optional encryption type for the error reports. SSE_S3 by default.
    """
    KmsKeyId: NotRequired[str]
    """
    Optional KMS key ID for the error reports.
    """


class ArrowDecryptionConfiguration(TypedDict):
    """Configuration for Arrow file decrypting."""

    crypto_factory: pyarrow.parquet.encryption.CryptoFactory
    """Crypto factory for encrypting and decrypting columns.
    see: https://arrow.apache.org/docs/python/generated/pyarrow.parquet.encryption.CryptoFactory.html"""
    kms_connection_config: pyarrow.parquet.encryption.KmsConnectionConfig
    """Configuration of the connection to the Key Management Service (KMS).
    see: https://arrow.apache.org/docs/python/generated/pyarrow.parquet.encryption.KmsClient.html"""


class ArrowEncryptionConfiguration(TypedDict):
    """Configuration for Arrow file encrypting."""

    crypto_factory: pyarrow.parquet.encryption.CryptoFactory
    """Crypto factory for encrypting and decrypting columns.
    see: https://arrow.apache.org/docs/python/generated/pyarrow.parquet.encryption.CryptoFactory.html"""
    kms_connection_config: pyarrow.parquet.encryption.KmsConnectionConfig
    """Configuration of the connection to the Key Management Service (KMS).
    see: https://arrow.apache.org/docs/python/generated/pyarrow.parquet.encryption.KmsClient.html"""
    encryption_config: pyarrow.parquet.encryption.EncryptionConfiguration
    """Configuration of the encryption, such as which columns to encrypt
    see: https://arrow.apache.org/docs/python/generated/pyarrow.parquet.encryption.EncryptionConfiguration.html
    """


class RaySettings(TypedDict):
    """Typed dictionary defining the settings for distributing calls using Ray."""

    parallelism: NotRequired[int]
    """
    The requested parallelism of the read.
    Parallelism may be limited by the number of files of the dataset.
    Auto-detect by default.
    """


class RayReadParquetSettings(RaySettings):
    """Typed dictionary defining the settings for distributing reading calls using Ray."""

    bulk_read: NotRequired[bool]
    """
    True to enable a faster reading of a large number of Parquet files.
    Offers improved performance due to not gathering the file metadata in a single node.
    The drawback is that it does not offer schema resolution, so it should only be used when the
    Parquet files are all uniform.
    """


class _S3WriteDataReturnValue(TypedDict):
    """Typed dictionary defining the dictionary returned by S3 write functions."""

    paths: Required[list[str]]
    """List of all stored files paths on S3."""
    partitions_values: Required[dict[str, list[str]]]
    """
    Dictionary of partitions added with keys as S3 path locations
    and values as a list of partitions values as str.
    """


class _ReadTableMetadataReturnValue(NamedTuple):
    """Named tuple defining the return value of the ``read_*_metadata`` functions."""

    columns_types: dict[str, str]
    """Dictionary containing column names and types."""
    partitions_types: dict[str, str] | None
    """Dictionary containing partition names and types, if partitioned."""
