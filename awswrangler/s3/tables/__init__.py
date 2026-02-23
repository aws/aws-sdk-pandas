"""Amazon S3 Tables Module."""

from awswrangler.s3.tables._create import create_namespace, create_table, create_table_bucket
from awswrangler.s3.tables._delete import delete_namespace, delete_table, delete_table_bucket
from awswrangler.s3.tables._read import from_iceberg
from awswrangler.s3.tables._write import to_iceberg

__all__ = [
    "create_table_bucket",
    "create_namespace",
    "create_table",
    "delete_table_bucket",
    "delete_namespace",
    "delete_table",
    "from_iceberg",
    "to_iceberg",
]
