"""Ray and Modin registered methods (PRIVATE)."""
# pylint: disable=import-outside-toplevel
from awswrangler._distributed import MemoryFormatEnum, engine, memory_format
from awswrangler._utils import table_refs_to_df
from awswrangler.distributed.ray._core import ray_remote
from awswrangler.lakeformation._read import _get_work_unit_results
from awswrangler.s3._delete import _delete_objects
from awswrangler.s3._read_parquet import _read_parquet, _read_parquet_metadata_file
from awswrangler.s3._read_text import _read_text
from awswrangler.s3._select import _select_object_content, _select_query
from awswrangler.s3._wait import _wait_object_batch
from awswrangler.s3._write_dataset import _to_buckets, _to_partitions
from awswrangler.s3._write_parquet import _to_parquet, to_parquet
from awswrangler.s3._write_text import _to_text, to_csv, to_json


def register_ray() -> None:
    """Register dispatched Ray and Modin (on Ray) methods."""
    # LakeFormation
    engine.register_func(_get_work_unit_results, ray_remote(_get_work_unit_results))

    # S3
    engine.register_func(_delete_objects, ray_remote(_delete_objects))
    engine.register_func(_read_parquet_metadata_file, ray_remote(_read_parquet_metadata_file))
    engine.register_func(_select_query, ray_remote(_select_query))
    engine.register_func(_select_object_content, ray_remote(_select_object_content))
    engine.register_func(_wait_object_batch, ray_remote(_wait_object_batch))

    if memory_format.get() == MemoryFormatEnum.MODIN:
        from awswrangler.distributed.ray.modin._core import modin_repartition
        from awswrangler.distributed.ray.modin._utils import _arrow_refs_to_df
        from awswrangler.distributed.ray.modin.s3._read_parquet import _read_parquet_distributed
        from awswrangler.distributed.ray.modin.s3._read_text import _read_text_distributed
        from awswrangler.distributed.ray.modin.s3._write_dataset import (
            _to_buckets_distributed,
            _to_partitions_distributed,
        )
        from awswrangler.distributed.ray.modin.s3._write_parquet import _to_parquet_distributed
        from awswrangler.distributed.ray.modin.s3._write_text import _to_text_distributed

        # S3
        engine.register_func(_read_parquet, _read_parquet_distributed)
        engine.register_func(_read_text, _read_text_distributed)
        engine.register_func(_to_buckets, _to_buckets_distributed)
        engine.register_func(_to_parquet, _to_parquet_distributed)
        engine.register_func(_to_partitions, _to_partitions_distributed)
        engine.register_func(_to_text, _to_text_distributed)
        engine.register_func(to_csv, modin_repartition(to_csv))
        engine.register_func(to_json, modin_repartition(to_json))
        engine.register_func(to_parquet, modin_repartition(to_parquet))

        # Utils
        engine.register_func(table_refs_to_df, _arrow_refs_to_df)
