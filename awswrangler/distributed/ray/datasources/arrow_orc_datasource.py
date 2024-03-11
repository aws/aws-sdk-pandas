"""Ray ArrowCSVDatasource Module."""

from __future__ import annotations

from typing import Any, Iterator

import pyarrow as pa
from ray.data.datasource.file_based_datasource import FileBasedDatasource

from awswrangler._arrow import _add_table_partitions


class ArrowORCDatasource(FileBasedDatasource):
    """ORC datasource, for reading and writing ORC files using PyArrow."""

    _FILE_EXTENSIONS = ["orc"]

    def __init__(
        self,
        paths: str | list[str],
        dataset: bool,
        path_root: str | None,
        use_threads: bool | int,
        schema: pa.Schema,
        arrow_orc_args: dict[str, Any] | None = None,
        **file_based_datasource_kwargs: Any,
    ):
        super().__init__(paths, **file_based_datasource_kwargs)

        self.dataset = dataset
        self.path_root = path_root

        if arrow_orc_args is None:
            arrow_orc_args = {}

        self.columns: list[str] | None = arrow_orc_args.get("columns", None)
        self.arrow_orc_args = arrow_orc_args

    def _read_stream(self, f: pa.NativeFile, path: str) -> Iterator[pa.Table]:
        from pyarrow import orc

        table: pa.Table = orc.read_table(f, columns=self.columns)

        if self.dataset:
            table = _add_table_partitions(
                table=table,
                path=f"s3://{path}",
                path_root=self.path_root,
            )

        return [table]  # type: ignore[return-value]

    def _open_input_source(
        self,
        filesystem: pa.fs.FileSystem,
        path: str,
        **open_args: Any,
    ) -> pa.NativeFile:
        return filesystem.open_input_file(path, **open_args)
