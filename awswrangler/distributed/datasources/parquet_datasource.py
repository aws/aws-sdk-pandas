"""Distributed ParquetDatasource Module."""

import logging
from typing import Any, Callable, Dict, Iterator, List, Optional, Union

import numpy as np
import pyarrow as pa

# fs required to implicitly trigger S3 subsystem initialization
import pyarrow.fs  # noqa: F401 pylint: disable=unused-import
import pyarrow.parquet as pq
from ray import cloudpickle  # pylint: disable=wrong-import-order,ungrouped-imports
from ray.data.block import Block, BlockAccessor, BlockMetadata
from ray.data.context import DatasetContext
from ray.data.datasource import BlockWritePathProvider, DefaultBlockWritePathProvider
from ray.data.datasource.datasource import ReadTask, WriteResult
from ray.data.datasource.file_based_datasource import (
    _resolve_paths_and_filesystem,
    _S3FileSystemWrapper,
    _wrap_s3_serialization_workaround,
)
from ray.data.datasource.file_meta_provider import DefaultParquetMetadataProvider, ParquetMetadataProvider
from ray.data.datasource.parquet_datasource import (
    _deregister_parquet_file_fragment_serialization,
    _register_parquet_file_fragment_serialization,
)
from ray.data.impl.output_buffer import BlockOutputBuffer
from ray.data.impl.remote_fn import cached_remote_fn
from ray.types import ObjectRef

from awswrangler._arrow import _add_table_partitions

_logger: logging.Logger = logging.getLogger(__name__)

# The number of rows to read per batch. This is sized to generate 10MiB batches
# for rows about 1KiB in size.
PARQUET_READER_ROW_BATCH_SIZE = 100000


class UserProvidedKeyBlockWritePathProvider(BlockWritePathProvider):
    """Block write path provider.

    Used when writing single-block datasets into a user-provided S3 key.
    """

    def _get_write_path_for_block(
        self,
        base_path: str,
        *,
        filesystem: Optional["pyarrow.fs.FileSystem"] = None,
        dataset_uuid: Optional[str] = None,
        block: Optional[ObjectRef[Block[Any]]] = None,
        block_index: Optional[int] = None,
        file_format: Optional[str] = None,
    ) -> str:
        return base_path


class ParquetDatasource:
    """Parquet datasource, for reading and writing Parquet files."""

    def __init__(self) -> None:
        self._write_paths: List[str] = []

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

    # Original implementation:
    # https://github.com/ray-project/ray/blob/releases/1.13.0/python/ray/data/datasource/file_based_datasource.py
    def do_write(
        self,
        blocks: List[ObjectRef[Block[Any]]],
        _: List[BlockMetadata],
        path: str,
        dataset_uuid: str,
        filesystem: Optional["pyarrow.fs.FileSystem"] = None,
        try_create_dir: bool = True,
        open_stream_args: Optional[Dict[str, Any]] = None,
        block_path_provider: BlockWritePathProvider = DefaultBlockWritePathProvider(),
        write_args_fn: Callable[[], Dict[str, Any]] = lambda: {},
        _block_udf: Optional[Callable[[Block[Any]], Block[Any]]] = None,
        ray_remote_args: Optional[Dict[str, Any]] = None,
        **write_args: Any,
    ) -> List[ObjectRef[WriteResult]]:
        """Create write tasks for a parquet file datasource."""
        paths, filesystem = _resolve_paths_and_filesystem(path, filesystem)
        path = paths[0]
        if try_create_dir:
            filesystem.create_dir(path, recursive=True)
        filesystem = _wrap_s3_serialization_workaround(filesystem)

        _write_block_to_file = self._write_block

        if open_stream_args is None:
            open_stream_args = {}

        if ray_remote_args is None:
            ray_remote_args = {}

        def write_block(write_path: str, block: Block[Any]) -> str:
            _logger.debug("Writing %s file.", write_path)
            fs: Optional["pyarrow.fs.FileSystem"] = filesystem
            if isinstance(fs, _S3FileSystemWrapper):
                fs = fs.unwrap()  # type: ignore
            if _block_udf is not None:
                block = _block_udf(block)

            with fs.open_output_stream(write_path, **open_stream_args) as f:
                _write_block_to_file(
                    f,
                    BlockAccessor.for_block(block),
                    writer_args_fn=write_args_fn,
                    **write_args,
                )
            # This is a change from original FileBasedDatasource.do_write that does not return paths
            return write_path

        write_block = cached_remote_fn(write_block).options(**ray_remote_args)

        file_format = self._file_format()
        write_tasks = []
        for block_idx, block in enumerate(blocks):
            write_path = block_path_provider(
                path,
                filesystem=filesystem,
                dataset_uuid=dataset_uuid,
                block=block,
                block_index=block_idx,
                file_format=file_format,
            )
            write_task = write_block.remote(write_path, block)  # type: ignore
            write_tasks.append(write_task)

        return write_tasks

    def on_write_complete(self, write_results: List[Any], **_: Any) -> None:
        """Execute callback on write complete."""
        _logger.debug("Write complete %s.", write_results)
        # Collect and return all write task paths
        self._write_paths.extend(write_results)

    def on_write_failed(self, write_results: List[ObjectRef[Any]], error: Exception, **_: Any) -> None:
        """Execute callback on write failed."""
        _logger.debug("Write failed %s.", write_results)
        raise error

    def get_write_paths(self) -> List[str]:
        """Return S3 paths of where the results have been written."""
        return self._write_paths

    def _write_block(
        self,
        f: "pyarrow.NativeFile",
        block: BlockAccessor[Any],
        writer_args_fn: Callable[[], Dict[str, Any]] = lambda: {},
        **writer_args: Any,
    ) -> None:
        """Write a block to S3."""
        import pyarrow.parquet as pq  # pylint: disable=import-outside-toplevel,redefined-outer-name,reimported

        writer_args = _resolve_kwargs(writer_args_fn, **writer_args)
        pq.write_table(block.to_arrow(), f, **writer_args)

    def _file_format(self) -> str:
        """Return file format."""
        return "parquet"


def _resolve_kwargs(kwargs_fn: Callable[[], Dict[str, Any]], **kwargs: Any) -> Dict[str, Any]:
    if kwargs_fn:
        kwarg_overrides = kwargs_fn()
        kwargs.update(kwarg_overrides)
    return kwargs
