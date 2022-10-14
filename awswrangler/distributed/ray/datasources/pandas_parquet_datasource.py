"""Ray ParquetDatasource Module."""

import io
import logging
from typing import Any, Dict, Iterator, List, Optional, Union

import pandas as pd
import pyarrow as pa

# fs required to implicitly trigger S3 subsystem initialization
import pyarrow.fs  # noqa: F401 pylint: disable=unused-import
from ray.data.block import BlockAccessor

from awswrangler.distributed.ray.datasources.pandas_file_based_datasource import PandasFileBasedDatasource
from awswrangler.s3._read_parquet import _read_parquet_chunked

_logger: logging.Logger = logging.getLogger(__name__)

# The number of rows to read per batch. This is sized to generate 10MiB batches
# for rows about 1KiB in size.
READER_ROW_BATCH_SIZE = 100000


class PandasParquetDatasource(PandasFileBasedDatasource):
    """Parquet datasource, for reading and writing Parquet files."""

    _FILE_EXTENSION = "parquet"

    def _read_stream(  # type: ignore  # pylint: disable=arguments-differ
        self,
        f: pyarrow.NativeFile,  # TODO: refactor reader to use wr.open_s3_object
        path: str,
        path_root: str,
        columns: Optional[List[str]],
        coerce_int96_timestamp_unit: Optional[str],
        use_threads: Union[bool, int],
        s3_additional_kwargs: Optional[Dict[str, str]],
        arrow_kwargs: Dict[str, Any],
        version_ids: Optional[Dict[str, str]],
        **reader_args: Any,
    ) -> Iterator[pd.DataFrame]:
        s3_path = f"s3://{path}"
        version_ids = {} if not version_ids else version_ids
        s3_additional_kwargs = {} if not s3_additional_kwargs else s3_additional_kwargs
        arrow_kwargs = {} if not arrow_kwargs else arrow_kwargs
        yield from _read_parquet_chunked(
            boto3_session=None,
            paths=[s3_path],
            path_root=path_root,
            columns=columns,
            coerce_int96_timestamp_unit=coerce_int96_timestamp_unit,
            chunked=READER_ROW_BATCH_SIZE,
            use_threads=False,
            s3_additional_kwargs=s3_additional_kwargs,
            arrow_kwargs=arrow_kwargs,
            version_ids=version_ids,
        )

    def _write_block(  # type: ignore  # pylint: disable=arguments-differ
        self,
        f: Union["io.RawIOBase"],
        block: BlockAccessor[Any],
        schema: pa.Schema,
        index: bool,
        compression: Optional[str],
        pandas_kwargs: Optional[Dict[str, Any]] = None,
        pyarrow_additional_kwargs: Optional[Dict[str, Any]] = None,
        **writer_args: Any,
    ) -> None:
        if not pyarrow_additional_kwargs:
            pyarrow_additional_kwargs = {}
        if not pyarrow_additional_kwargs.get("coerce_timestamps"):
            pyarrow_additional_kwargs["coerce_timestamps"] = "ms"
        if "flavor" not in pyarrow_additional_kwargs:
            pyarrow_additional_kwargs["flavor"] = "spark"

        table: pa.Table = pyarrow.Table.from_pandas(
            df=block.to_pandas(),
            schema=schema,
            preserve_index=index,
            safe=True,
        )
        writer = pa.parquet.ParquetWriter(
            where=f,
            write_statistics=True,
            use_dictionary=True,
            compression="NONE" if compression is None else compression,
            schema=schema,
            **pyarrow_additional_kwargs,
        )
        writer.write_table(table)
