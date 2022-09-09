"""Distributed ParquetDatasource Module."""

import logging
from typing import Any, Callable, Iterator, List, Optional, Union

import numpy as np
import pyarrow as pa

# fs required to implicitly trigger S3 subsystem initialization
import pyarrow.fs  # noqa: F401 pylint: disable=unused-import
import pyarrow.parquet as pq
from ray import cloudpickle
from ray.data.context import DatasetContext
from ray.data.datasource.datasource import ReadTask
from ray.data.datasource.file_based_datasource import _resolve_paths_and_filesystem
from ray.data.datasource.file_meta_provider import DefaultParquetMetadataProvider, ParquetMetadataProvider
from ray.data.datasource.parquet_datasource import (
    _deregister_parquet_file_fragment_serialization,
    _register_parquet_file_fragment_serialization,
)
from ray.data.impl.output_buffer import BlockOutputBuffer

from awswrangler._arrow import _add_table_partitions

_logger: logging.Logger = logging.getLogger(__name__)

# The number of rows to read per batch. This is sized to generate 10MiB batches
# for rows about 1KiB in size.
PARQUET_READER_ROW_BATCH_SIZE = 100000


class ParquetDatasource:
    """Parquet datasource, for reading and writing Parquet files."""

    # Original: https://github.com/ray-project/ray/blob/releases/1.13.0/python/ray/data/datasource/parquet_datasource.py
    def prepare_read(
        self,
        parallelism: int,
        use_threads: Union[bool, int],
        paths: Union[str, List[str]],
        schema: "pyarrow.lib.Schema",
        columns: Optional[List[str]] = None,
        coerce_int96_timestamp_unit: Optional[str] = None,
        path_root: Optional[str] = None,
        filesystem: Optional["pyarrow.fs.FileSystem"] = None,
        meta_provider: ParquetMetadataProvider = DefaultParquetMetadataProvider(),
        _block_udf: Optional[Callable[..., Any]] = None,
    ) -> List[ReadTask]:
        """Create and return read tasks for a Parquet file-based datasource."""
        paths, filesystem = _resolve_paths_and_filesystem(paths, filesystem)

        parquet_dataset = pq.ParquetDataset(
            path_or_paths=paths,
            filesystem=filesystem,
            partitioning=None,
            coerce_int96_timestamp_unit=coerce_int96_timestamp_unit,
            use_legacy_dataset=False,
        )

        def read_pieces(serialized_pieces: str) -> Iterator[pa.Table]:
            # Deserialize after loading the filesystem class.
            try:
                _register_parquet_file_fragment_serialization()  # type: ignore
                pieces = cloudpickle.loads(serialized_pieces)
            finally:
                _deregister_parquet_file_fragment_serialization()  # type: ignore

            # Ensure that we're reading at least one dataset fragment.
            assert len(pieces) > 0

            ctx = DatasetContext.get_current()
            output_buffer = BlockOutputBuffer(block_udf=_block_udf, target_max_block_size=ctx.target_max_block_size)

            _logger.debug("Reading %s parquet pieces", len(pieces))
            for piece in pieces:
                batches = piece.to_batches(
                    use_threads=use_threads,
                    columns=columns,
                    schema=schema,
                    batch_size=PARQUET_READER_ROW_BATCH_SIZE,
                )
                for batch in batches:
                    # Table creation is wrapped inside _add_table_partitions
                    # to add columns with partition values when dataset=True
                    table = _add_table_partitions(
                        table=pa.Table.from_batches([batch], schema=schema),
                        path=f"s3://{piece.path}",
                        path_root=path_root,
                    )
                    # If the table is empty, drop it.
                    if table.num_rows > 0:
                        output_buffer.add_block(table)
                        if output_buffer.has_next():
                            yield output_buffer.next()

            output_buffer.finalize()
            if output_buffer.has_next():
                yield output_buffer.next()

        if _block_udf is not None:
            # Try to infer dataset schema by passing dummy table through UDF.
            dummy_table = schema.empty_table()
            try:
                inferred_schema = _block_udf(dummy_table).schema
                inferred_schema = inferred_schema.with_metadata(schema.metadata)
            except Exception:  # pylint: disable=broad-except
                _logger.debug(
                    "Failed to infer schema of dataset by passing dummy table "
                    "through UDF due to the following exception:",
                    exc_info=True,
                )
                inferred_schema = schema
        else:
            inferred_schema = schema
        read_tasks = []
        metadata = meta_provider.prefetch_file_metadata(parquet_dataset.pieces) or []
        try:
            _register_parquet_file_fragment_serialization()  # type: ignore
            for pieces, metadata in zip(  # type: ignore
                np.array_split(parquet_dataset.pieces, parallelism),
                np.array_split(metadata, parallelism),
            ):
                if len(pieces) <= 0:
                    continue
                serialized_pieces = cloudpickle.dumps(pieces)  # type: ignore
                input_files = [p.path for p in pieces]
                meta = meta_provider(
                    input_files,
                    inferred_schema,
                    pieces=pieces,
                    prefetched_metadata=metadata,
                )
                read_tasks.append(ReadTask(lambda p=serialized_pieces: read_pieces(p), meta))  # type: ignore
        finally:
            _deregister_parquet_file_fragment_serialization()  # type: ignore

        return read_tasks
