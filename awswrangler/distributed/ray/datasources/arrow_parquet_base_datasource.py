"""Ray ParquetBaseDatasource Module."""

from __future__ import annotations

from typing import Any, Iterator

# fs required to implicitly trigger S3 subsystem initialization
import pyarrow as pa
import pyarrow.fs
import pyarrow.parquet as pq
from ray.data.datasource.file_based_datasource import FileBasedDatasource

from awswrangler._arrow import _add_table_partitions


class ArrowParquetBaseDatasource(FileBasedDatasource):
    """Parquet datasource, for reading Parquet files."""

    _FILE_EXTENSIONS = ["parquet"]

    def __init__(
        self,
        paths: str | list[str],
        path_root: str,
        arrow_parquet_args: dict[str, Any] | None = None,
        **file_based_datasource_kwargs: Any,
    ):
        super().__init__(paths, **file_based_datasource_kwargs)

        if arrow_parquet_args is None:
            arrow_parquet_args = {}

        self.path_root = path_root
        self.arrow_parquet_args = arrow_parquet_args

    def _read_stream(self, f: pa.NativeFile, path: str) -> Iterator[pa.Table]:
        arrow_parquet_args = self.arrow_parquet_args

        use_threads: bool = arrow_parquet_args.get("use_threads", False)
        columns: list[str] | None = arrow_parquet_args.get("columns", None)

        dataset_kwargs = arrow_parquet_args.get("dataset_kwargs", {})
        coerce_int96_timestamp_unit: str | None = dataset_kwargs.get("coerce_int96_timestamp_unit", None)
        decryption_properties = dataset_kwargs.get("decryption_properties", None)

        table = pq.read_table(
            f,
            use_threads=use_threads,
            columns=columns,
            coerce_int96_timestamp_unit=coerce_int96_timestamp_unit,
            decryption_properties=decryption_properties,
        )

        table = _add_table_partitions(
            table=table,
            path=f"s3://{path}",
            path_root=self.path_root,
        )

        return [table]  # type: ignore[return-value]

    def _open_input_source(
        self,
        filesystem: pyarrow.fs.FileSystem,
        path: str,
        **open_args: Any,
    ) -> pa.NativeFile:
        # Parquet requires `open_input_file` due to random access reads
        return filesystem.open_input_file(path, **open_args)

    def get_name(self) -> str:
        """Return a human-readable name for this datasource."""
        return "ParquetBulk"
