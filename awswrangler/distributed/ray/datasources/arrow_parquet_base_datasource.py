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
        schema: pa.schema | None,
        columns: list[str] | None = None,
        use_threads: bool | int = False,
        dataset_kwargs: dict[str, Any] | None = None,
        **file_based_datasource_kwargs: Any,
    ):
        super().__init__(paths, **file_based_datasource_kwargs)

        if dataset_kwargs is None:
            dataset_kwargs = {}

        self.path_root = path_root
        self.schema = schema
        self.columns = columns
        self.use_threads = use_threads
        self.dataset_kwargs = dataset_kwargs

    def _read_stream(self, f: pa.NativeFile, path: str) -> Iterator[pa.Table]:
        coerce_int96_timestamp_unit: str | None = self.dataset_kwargs.get("coerce_int96_timestamp_unit", None)
        decryption_properties = self.dataset_kwargs.get("decryption_properties", None)

        table = pq.read_table(
            f,
            use_threads=self.use_threads,
            columns=self.columns,
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
