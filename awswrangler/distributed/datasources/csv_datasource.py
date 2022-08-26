from typing import Any, Dict, Iterator

import pyarrow as pa
from pyarrow import csv
from ray.data.datasource.file_based_datasource import FileBasedDatasource

from awswrangler._arrow import _add_table_partitions


class CSVDatasource(FileBasedDatasource):
    """CSV datasource, for reading and writing CSV files."""

    # Original: https://github.com/ray-project/ray/blob/releases/1.13.0/python/ray/data/datasource/csv_datasource.py
    def _read_stream(
        self,
        f: pa.NativeFile,
        path: str,
        path_root: str,
        pandas_kwargs: Dict[str, Any],
    ) -> Iterator[pa.Table]:  # type: ignore

        read_options = csv.ReadOptions(use_threads=False)

        reader = csv.open_csv(f, read_options=read_options)
        schema = None
        while True:
            try:
                batch = reader.read_next_batch()

                # Adding logic to add partitioning
                table = _add_table_partitions(
                    table=pa.Table.from_batches([batch], schema=schema),
                    path=f"s3://{path}",
                    path_root=path_root,
                )

                if schema is None:
                    schema = table.schema

                yield table

            except StopIteration:
                return
