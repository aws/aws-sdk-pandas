import logging
import pandas as pd
import pyarrow as pa

from awswrangler._arrow import _add_table_partitions
from awswrangler.distributed.ray.datasources.pandas_text_datasource import PandasCSVDataSource
from pyarrow import csv
from ray.data.datasource.file_based_datasource import FileBasedDatasource
from typing import Any, Callable, Dict, Iterator, List, Optional, Tuple


SUPPORTED_PARAMS_WITH_DEFAULTS = {
    "delimiter": ",",
    "quotechar": "\"",
    "doublequote": True,
}


class CSVDatasource(PandasCSVDataSource):
    def __init__(self) -> None:
        super().__init__()

    def _parse_configuration(
        self,
        version_id_dict: Dict[str, Optional[str]],
        s3_additional_kwargs: Optional[Dict[str, str]],
        pandas_kwargs: Dict[str, Any],
    ) -> Tuple[csv.ReadOptions, csv.ParseOptions, csv.ConvertOptions]:
        if {key: value for key, value in version_id_dict.items() if value is not None}:
            raise NotImplementedError()

        if s3_additional_kwargs:
            raise NotImplementedError()

        for pandas_arg_key in pandas_kwargs:
            if pandas_arg_key not in SUPPORTED_PARAMS_WITH_DEFAULTS:
                raise NotImplementedError()

        read_options = csv.ReadOptions(
            use_threads=False,
        )
        parse_options = csv.ParseOptions(
            delimiter=pandas_kwargs.get("delimiter", SUPPORTED_PARAMS_WITH_DEFAULTS["delimiter"]),
            quote_char=pandas_kwargs.get("quotechar", SUPPORTED_PARAMS_WITH_DEFAULTS["quotechar"]),
            double_quote=pandas_kwargs.get("doublequote", SUPPORTED_PARAMS_WITH_DEFAULTS["doublequote"]),
        )
        convert_options = csv.ConvertOptions()

        return read_options, parse_options, convert_options

    def _read_stream_pyarrow(
        self,
        f: pa.NativeFile,
        path: str,
        path_root: str,
        dataset: bool,
        read_options: csv.ReadOptions,
        parse_options: csv.ParseOptions,
        convert_options: csv.ConvertOptions,
    ) -> Iterator[pd.DataFrame]:
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

    def _read_stream(
        self,
        f: pa.NativeFile,
        path: str,
        path_root: str,
        dataset: bool,
        version_id_dict: Dict[str, Optional[str]],
        s3_additional_kwargs: Optional[Dict[str, str]],
        pandas_kwargs: Dict[str, Any],
        **reader_args: Any,
    ) -> Iterator[pd.DataFrame]:
        try:
            read_options, parse_options, convert_options = self._parse_configuration(
                version_id_dict,
                s3_additional_kwargs,
                pandas_kwargs,
            )
        except NotImplementedError:
            yield from super()._read_stream(
                f,
                path,
                path_root,
                dataset,
                version_id_dict,
                s3_additional_kwargs,
                pandas_kwargs,
                **reader_args,
            )

        yield from self._read_stream_pyarrow(
            f,
            path,
            path_root,
            dataset,
            read_options,
            parse_options,
            convert_options,
        )
