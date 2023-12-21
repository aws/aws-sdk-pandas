"""Amazon S3 Read Module."""

from awswrangler.s3._copy import copy_objects, merge_datasets
from awswrangler.s3._delete import delete_objects
from awswrangler.s3._describe import describe_objects, get_bucket_region, size_objects
from awswrangler.s3._download import download
from awswrangler.s3._list import does_object_exist, list_buckets, list_directories, list_objects
from awswrangler.s3._read_deltalake import read_deltalake
from awswrangler.s3._read_excel import read_excel
from awswrangler.s3._read_orc import read_orc, read_orc_metadata, read_orc_table
from awswrangler.s3._read_parquet import read_parquet, read_parquet_metadata, read_parquet_table
from awswrangler.s3._read_text import read_csv, read_fwf, read_json
from awswrangler.s3._select import select_query
from awswrangler.s3._upload import upload
from awswrangler.s3._wait import wait_objects_exist, wait_objects_not_exist
from awswrangler.s3._write_deltalake import to_deltalake
from awswrangler.s3._write_excel import to_excel
from awswrangler.s3._write_orc import to_orc
from awswrangler.s3._write_parquet import store_parquet_metadata, to_parquet
from awswrangler.s3._write_text import to_csv, to_json

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
    "read_deltalake",
    "read_parquet",
    "read_parquet_metadata",
    "read_parquet_table",
    "read_orc",
    "read_orc_metadata",
    "read_orc_table",
    "read_csv",
    "read_fwf",
    "read_json",
    "wait_objects_exist",
    "wait_objects_not_exist",
    "select_query",
    "store_parquet_metadata",
    "to_parquet",
    "to_orc",
    "to_csv",
    "to_json",
    "to_deltalake",
    "to_excel",
    "read_excel",
    "download",
    "upload",
]
