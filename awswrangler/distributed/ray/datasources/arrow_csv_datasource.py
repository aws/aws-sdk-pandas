import datetime as dt
import pyarrow as pa

from awswrangler._arrow import _add_table_partitions
from pyarrow import csv
from ray.data.datasource.file_based_datasource import FileBasedDatasource
from typing import Iterator


class ArrowCSVDatasource(FileBasedDatasource):
    """CSV datasource, for reading and writing CSV files.

    Examples:
        >>> import ray
        >>> from ray.data.datasource import CSVDatasource
        >>> source = CSVDatasource() # doctest: +SKIP
        >>> ray.data.read_datasource( # doctest: +SKIP
        ...     source, paths="/path/to/dir").take()
        [{"a": 1, "b": "foo"}, ...]
    """

    def _read_stream(
        self,
        f: pa.NativeFile,
        path: str,
        path_root: str,
        dataset: bool,
        **reader_args,
    ) -> Iterator[pa.Table]:
        read_options = reader_args.get(
            "read_options", csv.ReadOptions(use_threads=False)
        )
        reader = csv.open_csv(f, read_options=read_options)
        print(f"{dt.datetime.now()} Opening {path}")

        schema = None
        path_root = reader_args.get("path_root", None)

        while True:
            try:
                batch = reader.read_next_batch()
                table = pa.Table.from_batches([batch], schema=schema)
                if schema is None:
                    schema = table.schema

                if dataset:
                    table = _add_table_partitions(
                        table=table,
                        path=f"s3://{path}",
                        path_root=path_root,
                    )

                yield table

            except StopIteration:
                print(f"{dt.datetime.now()} Finished reading from {path}")
                return

    def _file_format(self) -> str:
        return "csv"
