"""Ray ArrowParquetDatasource Module."""
# pylint: disable=redefined-outer-name,import-outside-toplevel,reimported

import logging
import time
from typing import Any, Callable, Dict, Iterator, List, Optional, Union

import numpy as np

# fs required to implicitly trigger S3 subsystem initialization
import pyarrow.fs  # noqa: F401 pylint: disable=unused-import
import ray
from pyarrow.dataset import ParquetFileFragment
from pyarrow.lib import Schema
from ray import cloudpickle
from ray.data.block import Block, BlockAccessor
from ray.data.context import DatasetContext
from ray.data.dataset import BlockOutputBuffer  # type: ignore
from ray.data.datasource import Reader, ReadTask
from ray.data.datasource.file_based_datasource import _resolve_paths_and_filesystem
from ray.data.datasource.file_meta_provider import _handle_read_os_error
from ray.data.datasource.parquet_datasource import (  # type: ignore
    DefaultParquetMetadataProvider,
    ParquetMetadataProvider,
)

from awswrangler._arrow import _add_table_partitions, _df_to_table
from awswrangler.distributed.ray import ray_remote
from awswrangler.distributed.ray.datasources.pandas_file_based_datasource import PandasFileBasedDatasource
from awswrangler.s3._write import _COMPRESSION_2_EXT

_logger: logging.Logger = logging.getLogger(__name__)

# The number of rows to read per batch. This is sized to generate 10MiB batches
# for rows about 1KiB in size.
PARQUET_READER_ROW_BATCH_SIZE = 100000
FILE_READING_RETRY = 8

# The default size multiplier for reading Parquet data source in Arrow.
# Parquet data format is encoded with various encoding techniques (such as
# dictionary, RLE, delta), so Arrow in-memory representation uses much more memory
# compared to Parquet encoded representation. Parquet file statistics only record
# encoded (i.e. uncompressed) data size information.
#
# To estimate real-time in-memory data size, Datasets will try to estimate the correct
# inflation ratio from Parquet to Arrow, using this constant as the default value for
# safety. See https://github.com/ray-project/ray/pull/26516 for more context.
PARQUET_ENCODING_RATIO_ESTIMATE_DEFAULT = 5

# The lower bound size to estimate Parquet encoding ratio.
PARQUET_ENCODING_RATIO_ESTIMATE_LOWER_BOUND = 2

# The percentage of files (1% by default) to be sampled from the dataset to estimate
# Parquet encoding ratio.
PARQUET_ENCODING_RATIO_ESTIMATE_SAMPLING_RATIO = 0.01

# The minimal and maximal number of file samples to take from the dataset to estimate
# Parquet encoding ratio.
# This is to restrict `PARQUET_ENCODING_RATIO_ESTIMATE_SAMPLING_RATIO` within the
# proper boundary.
PARQUET_ENCODING_RATIO_ESTIMATE_MIN_NUM_SAMPLES = 2
PARQUET_ENCODING_RATIO_ESTIMATE_MAX_NUM_SAMPLES = 10

# The number of rows to read from each file for sampling. Try to keep it low to avoid
# reading too much data into memory.
PARQUET_ENCODING_RATIO_ESTIMATE_NUM_ROWS = 5


# TODO(ekl) this is a workaround for a pyarrow serialization bug, where serializing a
# raw pyarrow file fragment causes S3 network calls.
class _SerializedPiece:
    def __init__(self, frag: ParquetFileFragment):
        self._data = cloudpickle.dumps(  # type: ignore
            (frag.format, frag.path, frag.filesystem, frag.partition_expression)
        )

    def deserialize(self) -> ParquetFileFragment:
        """Implicitly trigger S3 subsystem initialization by importing pyarrow.fs."""
        import pyarrow.fs  # noqa: F401 pylint: disable=unused-import

        (file_format, path, filesystem, partition_expression) = cloudpickle.loads(self._data)
        return file_format.make_fragment(path, filesystem, partition_expression)


# Visible for test mocking.
def _deserialize_pieces(
    serialized_pieces: List[_SerializedPiece],
) -> List[ParquetFileFragment]:
    return [p.deserialize() for p in serialized_pieces]


# This retry helps when the upstream datasource is not able to handle
# overloaded read request or failed with some retriable failures.
# For example when reading data from HA hdfs service, hdfs might
# lose connection for some unknown reason expecially when
# simutaneously running many hyper parameter tuning jobs
# with ray.data parallelism setting at high value like the default 200
# Such connection failure can be restored with some waiting and retry.
def _deserialize_pieces_with_retry(
    serialized_pieces: List[_SerializedPiece],
) -> List[ParquetFileFragment]:
    min_interval: float = 0
    final_exception: Optional[Exception] = None
    for i in range(FILE_READING_RETRY):
        try:
            return _deserialize_pieces(serialized_pieces)
        except Exception as e:  # pylint: disable=broad-except
            import random
            import time

            retry_timing = "" if i == FILE_READING_RETRY - 1 else (f"Retry after {min_interval} sec. ")
            log_only_show_in_1st_retry = (
                ""
                if i
                else (
                    f"If earlier read attempt threw certain Exception"
                    f", it may or may not be an issue depends on these retries "
                    f"succeed or not. serialized_pieces:{serialized_pieces}"
                )
            )
            _logger.exception(
                "%sth attempt to deserialize ParquetFileFragment failed. %s %s",
                i + 1,
                retry_timing,
                log_only_show_in_1st_retry,
            )
            if not min_interval:
                # to make retries of different process hit hdfs server
                # at slightly different time
                min_interval = 1 + random.random()
            # exponential backoff at
            # 1, 2, 4, 8, 16, 32, 64
            time.sleep(min_interval)
            min_interval = min_interval * 2
            final_exception = e
    raise final_exception  # type: ignore


class ArrowParquetDatasource(PandasFileBasedDatasource):  # pylint: disable=abstract-method
    """Parquet datasource, for reading and writing Parquet files."""

    _FILE_EXTENSION = "parquet"

    def create_reader(self, **kwargs: Dict[str, Any]) -> Reader[Any]:
        """Return a Reader for the given read arguments."""
        return _ArrowParquetDatasourceReader(**kwargs)  # type: ignore

    def _write_block(  # type: ignore  # pylint: disable=arguments-differ, arguments-renamed, unused-argument
        self,
        f: "pyarrow.NativeFile",
        block: BlockAccessor[Any],
        pandas_kwargs: Optional[Dict[str, Any]],
        **writer_args: Any,
    ) -> None:
        """Write a block to S3."""
        import pyarrow as pa  # pylint: disable=import-outside-toplevel,reimported

        schema: pa.Schema = writer_args.get("schema", None)
        dtype: Optional[Dict[str, str]] = writer_args.get("dtype", None)
        index: bool = writer_args.get("index", False)
        compression: Optional[str] = writer_args.get("compression", None)
        pyarrow_additional_kwargs: Optional[Dict[str, Any]] = writer_args.get("pyarrow_additional_kwargs", {})

        pa.parquet.write_table(
            _df_to_table(block.to_pandas(), schema=schema, index=index, dtype=dtype),
            f,
            compression=compression,
            **pyarrow_additional_kwargs,
        )

    def _get_file_suffix(self, file_format: str, compression: Optional[str]) -> str:
        if compression is not None:
            return f"{_COMPRESSION_2_EXT.get(compression)[1:]}.{file_format}"  # type: ignore
        return file_format


# Original implementation
# https://github.com/ray-project/ray/blob/ray-2.0.0/python/ray/data/datasource/parquet_datasource.py#L170
class _ArrowParquetDatasourceReader(Reader[Any]):  # pylint: disable=too-many-instance-attributes
    def __init__(
        self,
        paths: Union[str, List[str]],
        filesystem: Optional["pyarrow.fs.FileSystem"] = None,
        columns: Optional[List[str]] = None,
        schema: Optional[Schema] = None,
        meta_provider: ParquetMetadataProvider = DefaultParquetMetadataProvider(),
        _block_udf: Optional[Callable[[Block[Any]], Block[Any]]] = None,
        **reader_args: Any,
    ):
        import pyarrow as pa
        import pyarrow.parquet as pq

        paths, filesystem = _resolve_paths_and_filesystem(paths, filesystem)
        if len(paths) == 1:
            paths = paths[0]

        dataset_kwargs = reader_args.pop("dataset_kwargs", {})
        try:
            pq_ds = pq.ParquetDataset(paths, **dataset_kwargs, filesystem=filesystem, use_legacy_dataset=False)
        except OSError as e:
            _handle_read_os_error(e, paths)
        if schema is None:
            schema = pq_ds.schema
        if columns:
            schema = pa.schema([schema.field(column) for column in columns], schema.metadata)

        if _block_udf is not None:
            # Try to infer dataset schema by passing dummy table through UDF.
            dummy_table = schema.empty_table()
            try:
                inferred_schema = _block_udf(dummy_table).schema  # type: ignore
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

        try:
            self._metadata = meta_provider.prefetch_file_metadata(pq_ds.pieces) or []
        except OSError as e:
            _handle_read_os_error(e, paths)
        self._pq_ds = pq_ds
        self._meta_provider = meta_provider
        self._inferred_schema = inferred_schema
        self._block_udf = _block_udf
        self._reader_args = reader_args
        self._columns = columns
        self._schema = schema
        self._encoding_ratio = self._estimate_files_encoding_ratio()

    def estimate_inmemory_data_size(self) -> Optional[int]:
        """Estimate data size."""
        total_size: int = 0
        for file_metadata in self._metadata:
            for row_group_idx in range(file_metadata.num_row_groups):
                row_group_metadata = file_metadata.row_group(row_group_idx)
                total_size += row_group_metadata.total_byte_size
        return total_size * self._encoding_ratio  # type: ignore

    def get_read_tasks(self, parallelism: int) -> List[ReadTask]:
        """Override the base class FileBasedDatasource.get_read_tasks().

        Required in order to leverage pyarrow's ParquetDataset abstraction,
        which simplifies partitioning logic. We still use
        FileBasedDatasource's write side (do_write), however.
        """
        read_tasks = []
        block_udf, reader_args, columns, schema = (
            self._block_udf,
            self._reader_args,
            self._columns,
            self._schema,
        )
        for pieces, metadata in zip(
            np.array_split(self._pq_ds.pieces, parallelism),
            np.array_split(self._metadata, parallelism),
        ):
            if len(pieces) <= 0:
                continue
            serialized_pieces = [_SerializedPiece(p) for p in pieces]
            input_files = [p.path for p in pieces]
            meta = self._meta_provider(
                input_files,
                self._inferred_schema,
                pieces=pieces,
                prefetched_metadata=metadata,
            )
            if meta.size_bytes is not None:
                meta.size_bytes = int(meta.size_bytes * self._encoding_ratio)
            read_tasks.append(
                ReadTask(
                    lambda p=serialized_pieces: _read_pieces(  # type: ignore
                        block_udf,
                        reader_args,
                        columns,
                        schema,
                        p,
                    ),
                    meta,
                )
            )

        return read_tasks

    def _estimate_files_encoding_ratio(self) -> float:
        """Return an estimate of the Parquet files encoding ratio.

        To avoid OOMs, it is safer to return an over-estimate than an underestimate.
        """
        if not DatasetContext.get_current().decoding_size_estimation:
            return PARQUET_ENCODING_RATIO_ESTIMATE_DEFAULT

        # Sample a few rows from Parquet files to estimate the encoding ratio.
        # Launch tasks to sample multiple files remotely in parallel.
        # Evenly distributed to sample N rows in i-th row group in i-th file.
        # TODO(ekl/cheng) take into account column pruning.
        start_time = time.perf_counter()
        num_files = len(self._pq_ds.pieces)
        num_samples = int(num_files * PARQUET_ENCODING_RATIO_ESTIMATE_SAMPLING_RATIO)
        min_num_samples = min(PARQUET_ENCODING_RATIO_ESTIMATE_MIN_NUM_SAMPLES, num_files)
        max_num_samples = min(PARQUET_ENCODING_RATIO_ESTIMATE_MAX_NUM_SAMPLES, num_files)
        num_samples = max(min(num_samples, max_num_samples), min_num_samples)

        # Evenly distributed to choose which file to sample, to avoid biased prediction
        # if data is skewed.
        file_samples = [
            self._pq_ds.pieces[idx] for idx in np.linspace(0, num_files - 1, num_samples).astype(int).tolist()
        ]

        futures = []
        for idx, sample in enumerate(file_samples):
            # Sample i-th row group in i-th file.
            # Use SPREAD scheduling strategy to avoid packing many sampling tasks on
            # same machine to cause OOM issue, as sampling can be memory-intensive.
            futures.append(_sample_piece(_SerializedPiece(sample), idx))
        sample_ratios = ray.get(futures)
        ratio = np.mean(sample_ratios)

        sampling_duration = time.perf_counter() - start_time
        if sampling_duration > 5:
            _logger.info("Parquet input size estimation took %s seconds.", round(sampling_duration, 2))
        _logger.debug("Estimated Parquet encoding ratio from sampling is %s.", ratio)
        return max(ratio, PARQUET_ENCODING_RATIO_ESTIMATE_LOWER_BOUND)  # type: ignore


# Original implementation:
# https://github.com/ray-project/ray/blob/releases/2.0.0/python/ray/data/datasource/parquet_datasource.py
def _read_pieces(
    block_udf: Optional[Callable[[Block[Any]], Block[Any]]],
    reader_args: Any,
    columns: Optional[List[str]],
    schema: Optional[Union[type, "pyarrow.lib.Schema"]],
    serialized_pieces: List[_SerializedPiece],
) -> Iterator["pyarrow.Table"]:
    # This import is necessary to load the tensor extension type.
    from ray.data.extensions.tensor_extension import (  # type: ignore # noqa: F401, E501 # pylint: disable=import-outside-toplevel, unused-import
        ArrowTensorType,
    )

    # Deserialize after loading the filesystem class.
    pieces: List[ParquetFileFragment] = _deserialize_pieces_with_retry(serialized_pieces)

    # Ensure that we're reading at least one dataset fragment.
    assert len(pieces) > 0

    import pyarrow as pa  # pylint: disable=import-outside-toplevel

    ctx = DatasetContext.get_current()
    output_buffer = BlockOutputBuffer(
        block_udf=block_udf,
        target_max_block_size=ctx.target_max_block_size,
    )

    _logger.debug("Reading %s parquet pieces", len(pieces))
    use_threads = reader_args.pop("use_threads", False)
    path_root = reader_args.pop("path_root", None)
    for piece in pieces:
        batches = piece.to_batches(
            use_threads=use_threads,
            columns=columns,
            schema=schema,
            batch_size=PARQUET_READER_ROW_BATCH_SIZE,
            **reader_args,
        )
        for batch in batches:
            # Table creation is wrapped inside _add_table_partitions
            # to add columns with partition values when dataset=True
            # and cast them to categorical
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


@ray_remote(scheduling_strategy="SPREAD")
def _sample_piece(
    file_piece: _SerializedPiece,
    row_group_id: int,
) -> float:
    # Sample the `row_group_id`-th row group from file piece `serialized_piece`.
    # Return the encoding ratio calculated from the sampled rows.
    piece = _deserialize_pieces_with_retry([file_piece])[0]

    # If required row group index is out of boundary, sample the last row group.
    row_group_id = min(piece.num_row_groups - 1, row_group_id)
    assert (
        0 <= row_group_id <= piece.num_row_groups - 1
    ), f"Required row group id {row_group_id} is not in expected bound"

    row_group = piece.subset(row_group_ids=[row_group_id])
    metadata = row_group.metadata.row_group(0)
    num_rows = min(PARQUET_ENCODING_RATIO_ESTIMATE_NUM_ROWS, metadata.num_rows)
    assert num_rows > 0 and metadata.num_rows > 0, (
        f"Sampled number of rows: {num_rows} and total number of rows: " f"{metadata.num_rows} should be positive"
    )

    parquet_size: float = metadata.total_byte_size / metadata.num_rows
    # Set batch_size to num_rows will instruct Arrow Parquet reader to read exactly
    # num_rows into memory, o.w. it will read more rows by default in batch manner.
    in_memory_size: float = row_group.head(num_rows, batch_size=num_rows).nbytes / num_rows
    ratio: float = in_memory_size / parquet_size
    _logger.debug("Estimated Parquet encoding ratio is %s for piece %s.", ratio, piece)
    return in_memory_size / parquet_size
