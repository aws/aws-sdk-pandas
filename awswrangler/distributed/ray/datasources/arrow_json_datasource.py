"""Ray ArrowCSVDatasource Module."""
from typing import Any

import pyarrow as pa
from pyarrow import json

from awswrangler._arrow import _add_table_partitions
from awswrangler.distributed.ray.datasources.pandas_file_based_datasource import PandasFileBasedDatasource


class ArrowJSONDatasource(PandasFileBasedDatasource):  # pylint: disable=abstract-method
    """JSON datasource, for reading and writing JSON files using PyArrow."""

    _FILE_EXTENSION = "json"

    def _read_file(  # type: ignore[override]  # pylint: disable=arguments-differ
        self,
        f: pa.NativeFile,
        path: str,
        path_root: str,
        dataset: bool,
        **reader_args: Any,
    ) -> pa.Table:
        read_options = reader_args.get("read_options", json.ReadOptions(use_threads=False))
        parse_options = reader_args.get(
            "parse_options",
            json.ParseOptions(),
        )

        table = json.read_json(f, read_options=read_options, parse_options=parse_options)

        if dataset:
            table = _add_table_partitions(
                table=table,
                path=f"s3://{path}",
                path_root=path_root,
            )

        return table
