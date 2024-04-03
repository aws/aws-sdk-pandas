"""Ray ArrowCSVDatasource Module."""

from __future__ import annotations

from typing import Any, Iterator

import pyarrow as pa
from pyarrow import csv
from ray.data.datasource.file_based_datasource import FileBasedDatasource

from awswrangler._arrow import _add_table_partitions


class ArrowCSVDatasource(FileBasedDatasource):
    """CSV datasource, for reading CSV files using PyArrow."""

    _FILE_EXTENSIONS = ["csv"]

    def __init__(
        self,
        paths: str | list[str],
        dataset: bool,
        path_root: str,
        version_ids: dict[str, str] | None = None,
        s3_additional_kwargs: dict[str, str] | None = None,
        pandas_kwargs: dict[str, Any] | None = None,
        arrow_csv_args: dict[str, Any] | None = None,
        **file_based_datasource_kwargs: Any,
    ):
        from pyarrow import csv

        super().__init__(paths, **file_based_datasource_kwargs)

        self.dataset = dataset
        self.path_root = path_root

        if arrow_csv_args is None:
            arrow_csv_args = {}

        self.read_options = arrow_csv_args.pop("read_options", csv.ReadOptions(use_threads=False))
        self.parse_options = arrow_csv_args.pop("parse_options", csv.ParseOptions())
        self.convert_options = arrow_csv_args.get("convert_options", csv.ConvertOptions())
        self.arrow_csv_args = arrow_csv_args

    def _read_stream(self, f: pa.NativeFile, path: str) -> Iterator[pa.Table]:
        reader = csv.open_csv(
            f,
            read_options=self.read_options,
            parse_options=self.parse_options,
            convert_options=self.convert_options,
        )

        schema = None
        while True:
            try:
                batch = reader.read_next_batch()
                table = pa.Table.from_batches([batch], schema=schema)
                if schema is None:
                    schema = table.schema

                if self.dataset:
                    table = _add_table_partitions(
                        table=table,
                        path=f"s3://{path}",
                        path_root=self.path_root,
                    )

                yield table

            except StopIteration:
                return
