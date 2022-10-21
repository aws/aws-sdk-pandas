"""Ray ParquetDatasource Module."""

import logging
from typing import Any, Callable, Dict, Iterator, List, Optional, Union

# fs required to implicitly trigger S3 subsystem initialization
import pyarrow.fs  # noqa: F401 pylint: disable=unused-import
import ray
from ray.data._internal.output_buffer import BlockOutputBuffer
from ray.data.block import Block, BlockAccessor
from ray.data.context import DatasetContext
from ray.data.datasource import Reader
from ray.data.datasource.parquet_datasource import (
    PARQUET_READER_ROW_BATCH_SIZE,
    _deserialize_pieces_with_retry,
    _ParquetDatasourceReader,
    _SerializedPiece,
)

from awswrangler._arrow import _add_table_partitions, _df_to_table
from awswrangler.distributed.ray.datasources.pandas_file_based_datasource import PandasFileBasedDatasource
from awswrangler.s3._write import _COMPRESSION_2_EXT

_logger: logging.Logger = logging.getLogger(__name__)


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
    pieces: List["pyarrow._dataset.ParquetFileFragment"] = _deserialize_pieces_with_retry(serialized_pieces)

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


# Patch _read_pieces function
ray.data.datasource.parquet_datasource._read_pieces = _read_pieces  # pylint: disable=protected-access


class ParquetDatasource(PandasFileBasedDatasource):  # pylint: disable=abstract-method
    """Parquet datasource, for reading and writing Parquet files."""

    _FILE_EXTENSION = "parquet"

    def create_reader(self, **kwargs: Dict[str, Any]) -> Reader[Any]:
        """Return a Reader for the given read arguments."""
        return _ParquetDatasourceReader(**kwargs)  # type: ignore

    def _write_block(  # type: ignore  # pylint: disable=arguments-differ, arguments-renamed, unused-argument
        self,
        f: "pyarrow.NativeFile",
        block: BlockAccessor[Any],
        pandas_kwargs: Optional[Dict[str, Any]],
        **writer_args: Any,
    ) -> None:
        """Write a block to S3."""
        import pyarrow as pa  # pylint: disable=import-outside-toplevel,redefined-outer-name,reimported

        schema: pa.Schema = writer_args.get("schema", None)
        dtype: Optional[Dict[str, str]] = writer_args.get("dtype", None)
        index: bool = writer_args.get("index", False)
        compression: Optional[str] = writer_args.get("compression", None)

        pa.parquet.write_table(
            _df_to_table(block.to_pandas(), schema=schema, index=index, dtype=dtype),
            f,
            compression=compression,
        )

    def _get_file_suffix(self, file_format: str, compression: Optional[str]) -> str:
        if compression is not None:
            return f"{_COMPRESSION_2_EXT.get(compression)[1:]}.{file_format}"  # type: ignore
        return file_format


def _resolve_kwargs(kwargs_fn: Callable[[], Dict[str, Any]], **kwargs: Any) -> Dict[str, Any]:
    if kwargs_fn:
        kwarg_overrides = kwargs_fn()
        kwargs.update(kwarg_overrides)
    return kwargs
