"""Amazon S3 Read Module."""

from awswrangler._config import MemoryFormat, config
from awswrangler.s3._copy import copy_objects, merge_datasets  # noqa
from awswrangler.s3._delete import delete_objects  # noqa
from awswrangler.s3._describe import describe_objects, get_bucket_region, size_objects  # noqa
from awswrangler.s3._download import download  # noqa
from awswrangler.s3._list import does_object_exist, list_buckets, list_directories, list_objects  # noqa
from awswrangler.s3._merge_upsert_table import merge_upsert_table  # noqa
from awswrangler.s3._read_excel import read_excel  # noqa
from awswrangler.s3._read_parquet import read_parquet, read_parquet_metadata, read_parquet_table  # noqa
from awswrangler.s3._read_text import read_csv, read_fwf, read_json  # noqa
from awswrangler.s3._select import select_query
from awswrangler.s3._upload import upload  # noqa
from awswrangler.s3._wait import wait_objects_exist, wait_objects_not_exist  # noqa
from awswrangler.s3._write_excel import to_excel  # noqa
from awswrangler.s3._write_parquet import store_parquet_metadata, to_parquet  # noqa
from awswrangler.s3._write_text import to_csv, to_json  # noqa

__all__ = [
    "copy_objects",
    "merge_datasets",
    "delete_objects",
    "describe_objects",
    "get_bucket_region",
    "size_objects",
    "does_object_exist",
    "list_buckets",
    "list_directories",
    "list_objects",
    "read_parquet",
    "read_parquet_metadata",
    "read_parquet_table",
    "read_csv",
    "read_fwf",
    "read_json",
    "wait_objects_exist",
    "wait_objects_not_exist",
    "select_query",
    "store_parquet_metadata",
    "to_parquet",
    "to_csv",
    "to_json",
    "to_excel",
    "read_excel",
    "download",
    "upload",
]

if config.memory_format == MemoryFormat.MODIN.value:
    from modin.pandas import DataFrame as ModinDataFrame

    from awswrangler.s3._write_dataset import (  # pylint: disable=ungrouped-imports
        _to_buckets,
        _to_buckets_distributed,
        _to_partitions,
        _to_partitions_distributed,
    )
    from awswrangler.s3._write_parquet import _to_parquet, _to_parquet_distributed  # pylint: disable=ungrouped-imports

    _to_parquet.register(ModinDataFrame, _to_parquet_distributed)
    _to_buckets.register(ModinDataFrame, _to_buckets_distributed)
    _to_partitions.register(ModinDataFrame, _to_partitions_distributed)
