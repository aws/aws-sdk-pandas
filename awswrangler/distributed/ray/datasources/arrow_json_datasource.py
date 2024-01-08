"""Ray ArrowCSVDatasource Module."""
from typing import Any, Dict, Iterator, List, Optional, Union

import pyarrow as pa
from pyarrow import json
from ray.data.datasource.file_based_datasource import FileBasedDatasource

from awswrangler._arrow import _add_table_partitions


class ArrowJSONDatasource(FileBasedDatasource):
    """JSON datasource, for reading JSON files using PyArrow."""

    _FILE_EXTENSIONS = ["json"]

    def __init__(
        self,
        paths: Union[str, List[str]],
        dataset: bool,
        path_root: str,
        version_ids: Optional[Dict[str, str]] = None,
        s3_additional_kwargs: Optional[Dict[str, str]] = None,
        pandas_kwargs: Optional[Dict[str, Any]] = None,
        arrow_json_args: Optional[Dict[str, Any]] = None,
        **file_based_datasource_kwargs: Any,
    ):
        super().__init__(paths, **file_based_datasource_kwargs)

        self.dataset = dataset
        self.path_root = path_root

        if arrow_json_args is None:
            arrow_json_args = {}

        self.read_options = json.ReadOptions(arrow_json_args.pop("read_options", dict(use_threads=False)))
        self.parse_options = json.ParseOptions(arrow_json_args.pop("parse_options", {}))
        self.arrow_json_args = arrow_json_args

    def _read_stream(self, f: pa.NativeFile, path: str) -> Iterator[pa.Table]:
        table = json.read_json(f, read_options=self.read_options, parse_options=self.parse_options)

        if self.dataset:
            table = _add_table_partitions(
                table=table,
                path=f"s3://{path}",
                path_root=self.path_root,
            )

        return [table]  # type: ignore[return-value]
