"""Ray ArrowCSVDatasource Module."""
from typing import Any, Dict, List, Optional

import pyarrow as pa
from ray.data.block import BlockAccessor

from awswrangler._arrow import _add_table_partitions, _df_to_table
from awswrangler.distributed.ray.datasources.pandas_file_based_datasource import PandasFileBasedDatasource


class ArrowORCDatasource(PandasFileBasedDatasource):
    """ORC datasource, for reading and writing ORC files using PyArrow."""

    _FILE_EXTENSION = "orc"

    def _read_file(  # type: ignore[override]
        self,
        f: pa.NativeFile,
        path: str,
        path_root: str,
        **reader_args: Any,
    ) -> pa.Table:
        from pyarrow import orc

        columns: Optional[List[str]] = reader_args.get("columns", None)

        table: pa.Table = orc.read_table(f, columns=columns)

        table = _add_table_partitions(
            table=table,
            path=f"s3://{path}",
            path_root=path_root,
        )

        return table

    def _open_input_source(
        self,
        filesystem: pa.fs.FileSystem,
        path: str,
        **open_args: Any,
    ) -> pa.NativeFile:
        return filesystem.open_input_file(path, **open_args)

    def _write_block(  # type: ignore[override]
        self,
        f: pa.NativeFile,
        block: BlockAccessor,
        pandas_kwargs: Optional[Dict[str, Any]],
        **writer_args: Any,
    ) -> None:
        from pyarrow import orc

        schema: Optional[pa.schema] = writer_args.get("schema", None)
        dtype: Optional[Dict[str, str]] = writer_args.get("dtype", None)
        index: bool = writer_args.get("index", False)
        compression: str = writer_args.get("compression", None) or "UNCOMPRESSED"
        pyarrow_additional_kwargs: Dict[str, Any] = writer_args.get("pyarrow_additional_kwargs", {})

        orc.write_table(
            _df_to_table(block.to_pandas(), schema=schema, index=index, dtype=dtype),
            f,
            compression=compression,
            **pyarrow_additional_kwargs,
        )
