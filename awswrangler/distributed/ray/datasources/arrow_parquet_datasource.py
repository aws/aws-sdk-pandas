"""Ray ArrowParquetDatasource Module.

This module is pulled from Ray's [ParquetDatasource]
(https://github.com/ray-project/ray/blob/ray-2.0.0/python/ray/data/datasource/parquet_datasource.py) with a few changes
and customized to ensure compatibility with AWS SDK for pandas behavior. Changes from the original implementation,
are documented in the comments and marked with (AWS SDK for pandas) prefix.
"""
# pylint: disable=redefined-outer-name,import-outside-toplevel,reimported

import logging
from typing import Any, Dict, Optional

# fs required to implicitly trigger S3 subsystem initialization
import pyarrow.fs  # noqa: F401 pylint: disable=unused-import
from ray.data.block import BlockAccessor
from ray.data.datasource import Reader
from ray.data.datasource.parquet_datasource import _ParquetDatasourceReader

from awswrangler._arrow import _df_to_table
from awswrangler.distributed.ray.datasources.arrow_parquet_base_datasource import ArrowParquetBaseDatasource
from awswrangler.s3._write import _COMPRESSION_2_EXT

_logger: logging.Logger = logging.getLogger(__name__)


class ArrowParquetDatasource(ArrowParquetBaseDatasource):  # pylint: disable=abstract-method
    """(AWS SDK for pandas) Parquet datasource, for reading and writing Parquet files.

    The following are the changes to the original Ray implementation:
    1. Added handling of additional parameters `dtype`, `index`, `compression` and added the ability
       to pass through additional `pyarrow_additional_kwargs` and `s3_additional_kwargs` for writes.
    """

    def create_reader(self, **kwargs: Dict[str, Any]) -> Reader:
        """Return a Reader for the given read arguments."""
        return _ParquetDatasourceReader(**kwargs)  # type: ignore[arg-type]

    def _write_block(  # type: ignore[override]  # pylint: disable=arguments-differ, arguments-renamed, unused-argument
        self,
        f: "pyarrow.NativeFile",
        block: BlockAccessor,
        pandas_kwargs: Optional[Dict[str, Any]],
        **writer_args: Any,
    ) -> None:
        """Write a block to S3."""
        import pyarrow as pa  # pylint: disable=import-outside-toplevel,reimported

        schema: pa.Schema = writer_args.get("schema", None)
        dtype: Optional[Dict[str, str]] = writer_args.get("dtype", None)
        index: bool = writer_args.get("index", False)
        compression: Optional[str] = writer_args.get("compression", None)
        pyarrow_additional_kwargs: Optional[Dict[str, Any]] = writer_args.get("pyarrow_additional_kwargs", {})

        pa.parquet.write_table(
            _df_to_table(block.to_pandas(), schema=schema, index=index, dtype=dtype),
            f,
            compression=compression,
            **pyarrow_additional_kwargs,
        )

    def _get_file_suffix(self, file_format: str, compression: Optional[str]) -> str:
        if compression is not None:
            return f"{_COMPRESSION_2_EXT.get(compression)[1:]}.{file_format}"  # type: ignore[index]
        return file_format
