"""Amazon S3 Vectors (PRIVATE subpackage).

Public functions are re-exported through ``awswrangler.s3``. This module is private —
its layout may change without notice.
"""

from awswrangler.s3._vectors._mgmt import (
    create_vector_bucket,
    create_vector_index,
    delete_vector_bucket,
    delete_vector_index,
    get_vector_bucket,
    get_vector_index,
    list_vector_buckets,
    list_vector_indexes,
)
from awswrangler.s3._vectors._read import (
    get_vectors,
    list_vectors,
    query_vectors,
)
from awswrangler.s3._vectors._write import (
    delete_vectors,
    put_vectors,
    put_vectors_from_df,
)

__all__ = [
    "create_vector_bucket",
    "delete_vector_bucket",
    "list_vector_buckets",
    "get_vector_bucket",
    "create_vector_index",
    "delete_vector_index",
    "list_vector_indexes",
    "get_vector_index",
    "put_vectors",
    "put_vectors_from_df",
    "get_vectors",
    "delete_vectors",
    "list_vectors",
    "query_vectors",
]
