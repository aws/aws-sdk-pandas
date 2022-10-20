from typing import Any, Iterator

import pandas as pd
import pyarrow as pa
from pyarrow import csv
from ray.data.datasource.file_based_datasource import FileBasedDatasource

from awswrangler._arrow import _add_table_partitions


class ArrowCSVDatasource(FileBasedDatasource):
    _FILE_EXTENSION = "csv"

    def _read_stream(  # type: ignore  # pylint: disable=arguments-differ
        self,
        f: pa.NativeFile,
        path: str,
        path_root: str,
        dataset: bool,
        **reader_args: Any,
    ) -> Iterator[pd.DataFrame]:
        read_options = reader_args.get("read_options", csv.ReadOptions(use_threads=False))
        parse_options = reader_args.get(
            "parse_options",
            csv.ParseOptions(),
        )
        convert_options = reader_args.get("convert_options", csv.ConvertOptions())

        reader = csv.open_csv(
            f,
            read_options=read_options,
            parse_options=parse_options,
            convert_options=convert_options,
        )

        schema = None
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

                yield table.to_pandas()

            except StopIteration:
                return
