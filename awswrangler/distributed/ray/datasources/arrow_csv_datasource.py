"""Ray ArrowCSVDatasource Module."""
from typing import Any, Iterator

import pandas as pd
import pyarrow as pa
from pyarrow import csv
from ray.data.block import BlockAccessor

from awswrangler._arrow import _add_table_partitions
from awswrangler.distributed.ray.datasources.pandas_file_based_datasource import PandasFileBasedDatasource


class ArrowCSVDatasource(PandasFileBasedDatasource):  # pylint: disable=abstract-method
    """CSV datasource, for reading and writing CSV files using PyArrow."""

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

    def _write_block(  # type: ignore  # pylint: disable=arguments-differ
        self,
        f: pa.NativeFile,
        block: BlockAccessor[Any],
        **writer_args,
    ) -> None:
        write_options_dict = writer_args.pop("write_options", {})
        write_options = csv.WriteOptions(**write_options_dict)

        csv.write_csv(block.to_arrow(), f, write_options)
