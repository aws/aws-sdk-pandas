"""Ray ParquetBaseDatasource Module.

This module is pulled from Ray's [ParquetBaseDatasource]
(https://github.com/ray-project/ray/blob/master/python/ray/data/datasource/parquet_base_datasource.py) with a few changes
and customized to ensure compatibility with AWS SDK for pandas behavior. Changes from the original implementation,
are documented in the comments and marked with (AWS SDK for pandas) prefix.
"""

from typing import Any, Dict, List, Optional

# fs required to implicitly trigger S3 subsystem initialization
import pyarrow as pa
import pyarrow.fs
import pyarrow.parquet as pq
from ray.data.block import BlockAccessor

from awswrangler._arrow import _add_table_partitions, _df_to_table
from awswrangler.distributed.ray.datasources.pandas_file_based_datasource import PandasFileBasedDatasource


class ArrowParquetBaseDatasource(PandasFileBasedDatasource):  # pylint: disable=abstract-method
    """(AWS SDK for pandas) Parquet datasource, for reading and writing Parquet files.

    The following are the changes to the original Ray implementation:
    1. Added handling of additional parameters `dtype`, `index`, `compression` and added the ability
       to pass through additional `pyarrow_additional_kwargs` and `s3_additional_kwargs` for writes.
    3. Added `dataset` and `path_root` parameters to allow user to control loading partitions
       relative to the root S3 prefix.
    """

    _FILE_EXTENSION = "parquet"

    def _read_file(  # type: ignore[override]
        self,
        f: pa.NativeFile,
        path: str,
        path_root: str,
        **reader_args: Any,
    ) -> pa.Table:
        use_threads: bool = reader_args.get("use_threads", False)
        columns: Optional[List[str]] = reader_args.get("columns", None)

        dataset_kwargs = reader_args.get("dataset_kwargs", {})
        coerce_int96_timestamp_unit: Optional[str] = dataset_kwargs.get("coerce_int96_timestamp_unit", None)

        table = pq.read_table(
            f,
            use_threads=use_threads,
            columns=columns,
            coerce_int96_timestamp_unit=coerce_int96_timestamp_unit,
        )

        table = _add_table_partitions(
            table=table,
            path=f"s3://{path}",
            path_root=path_root,
        )

        return table

    def _open_input_source(
        self,
        filesystem: pyarrow.fs.FileSystem,
        path: str,
        **open_args: Any,
    ) -> pa.NativeFile:
        # Parquet requires `open_input_file` due to random access reads
        return filesystem.open_input_file(path, **open_args)

    def _write_block(  # type: ignore[override]
        self,
        f: pa.NativeFile,
        block: BlockAccessor,
        **writer_args: Any,
    ) -> None:
        schema: Optional[pa.schema] = writer_args.get("schema", None)
        dtype: Optional[Dict[str, str]] = writer_args.get("dtype", None)
        index: bool = writer_args.get("index", False)
        compression: Optional[str] = writer_args.get("compression", None)
        pyarrow_additional_kwargs: Dict[str, Any] = writer_args.get("pyarrow_additional_kwargs", {})

        pq.write_table(
            _df_to_table(block.to_pandas(), schema=schema, index=index, dtype=dtype),
            f,
            compression=compression,
            **pyarrow_additional_kwargs,
        )
