"""Ray and Modin registered methods (PRIVATE)."""
# pylint: disable=import-outside-toplevel
from awswrangler._distributed import MemoryFormatEnum, memory_format
from awswrangler._utils import table_refs_to_df
from awswrangler.distributed.ray._utils import _batch_paths_distributed
from awswrangler.s3._read_parquet import _read_parquet
from awswrangler.s3._read_text import _read_text
from awswrangler.s3._wait import _batch_paths
from awswrangler.s3._write_dataset import _to_buckets, _to_partitions
from awswrangler.s3._write_parquet import _to_parquet
from awswrangler.s3._write_text import _to_text


def register_ray() -> None:
    """Register dispatched Ray and Modin (on Ray) methods."""
    _batch_paths.register(list, _batch_paths_distributed)

    if memory_format.get() == MemoryFormatEnum.MODIN.value:
        from modin.pandas import DataFrame as ModinDataFrame

        from awswrangler.distributed.ray.modin._utils import _arrow_refs_to_df
        from awswrangler.distributed.ray.modin.s3._read_parquet import _read_parquet_distributed
        from awswrangler.distributed.ray.modin.s3._read_text import _read_text_distributed
        from awswrangler.distributed.ray.modin.s3._write_dataset import (
            _to_buckets_distributed,
            _to_partitions_distributed,
        )
        from awswrangler.distributed.ray.modin.s3._write_parquet import _to_parquet_distributed
        from awswrangler.distributed.ray.modin.s3._write_text import _to_text_distributed

        # S3 Read
        _read_parquet.register(list, _read_parquet_distributed)
        _read_text.register(str, _read_text_distributed)

        # S3 Write
        _to_buckets.register(ModinDataFrame, _to_buckets_distributed)
        _to_parquet.register(ModinDataFrame, _to_parquet_distributed)
        _to_partitions.register(ModinDataFrame, _to_partitions_distributed)
        _to_text.register(ModinDataFrame, _to_text_distributed)

        # Utils
        table_refs_to_df.register(list, _arrow_refs_to_df)
