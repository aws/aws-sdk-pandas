"""Ray PandasTextDatasource Module."""
import io
import logging
from typing import Any, Callable, Dict, Iterator, List, Optional

import pandas as pd
import pyarrow
from ray.data._internal.pandas_block import PandasBlockAccessor
from ray.data._internal.remote_fn import cached_remote_fn
from ray.data.block import BlockMetadata
from ray.data.datasource.datasource import WriteResult
from ray.data.datasource.file_based_datasource import (
    BlockWritePathProvider,
    DefaultBlockWritePathProvider,
    FileBasedDatasource,
)
from ray.types import ObjectRef

from awswrangler._arrow import _add_table_partitions
from awswrangler.s3._fs import open_s3_object
from awswrangler.s3._read_text_core import _read_text_chunked, _read_text_file
from awswrangler.s3._write import _COMPRESSION_2_EXT

_logger: logging.Logger = logging.getLogger(__name__)

# The number of rows to read per batch. This is sized to generate 10MiB batches
# for rows about 1KiB in size.
READER_ROW_BATCH_SIZE = 10_0000


class PandasTextDatasource(FileBasedDatasource):
    """Pandas text datasource, for reading and writing text files using Pandas."""

    def __init__(
        self,
        file_format: str,
        read_text_func: Callable[..., pd.DataFrame],
        write_text_func: Optional[Callable[..., None]],
    ) -> None:
        super().__init__()

        self.file_format = file_format
        self.read_text_func = read_text_func
        self.write_text_func = write_text_func

        self._write_paths: List[str] = []

    def _file_format(self) -> str:
        return self.file_format

    def _read_stream(  # type: ignore  # pylint: disable=arguments-differ
        self,
        f: pyarrow.NativeFile,
        path: str,
        path_root: str,
        dataset: bool,
        version_id_dict: Dict[str, Optional[str]],
        s3_additional_kwargs: Optional[Dict[str, str]],
        pandas_kwargs: Dict[str, Any],
        **reader_args: Any,
    ) -> Iterator[pd.DataFrame]:
        s3_path = f"s3://{path}"
        yield from _read_text_chunked(
            path=s3_path,
            chunksize=READER_ROW_BATCH_SIZE,
            parser_func=self.read_text_func,
            path_root=path_root,
            dataset=dataset,
            boto3_session=None,
            pandas_kwargs=pandas_kwargs,
            s3_additional_kwargs=s3_additional_kwargs,
            use_threads=False,
            version_id=version_id_dict.get(s3_path),
        )

    def _read_file(self, f: pyarrow.NativeFile, path: str, **reader_args: Any) -> pd.DataFrame:
        raise NotImplementedError()

    def do_write(  # type: ignore  # pylint: disable=arguments-differ
        self,
        blocks: List[ObjectRef[pd.DataFrame]],
        metadata: List[BlockMetadata],
        path: str,
        dataset_uuid: str,
        filesystem: Optional[pyarrow.fs.FileSystem] = None,
        try_create_dir: bool = True,
        open_stream_args: Optional[Dict[str, Any]] = None,
        block_path_provider: BlockWritePathProvider = DefaultBlockWritePathProvider(),
        write_args_fn: Callable[[], Dict[str, Any]] = lambda: {},
        _block_udf: Optional[Callable[[pd.DataFrame], pd.DataFrame]] = None,
        ray_remote_args: Optional[Dict[str, Any]] = None,
        s3_additional_kwargs: Optional[Dict[str, str]] = None,
        **write_args,
    ) -> List[ObjectRef[WriteResult]]:
        """Create and return write tasks for a file-based datasource."""
        _write_block_to_file = self._write_block

        if open_stream_args is None:
            open_stream_args = {}

        if ray_remote_args is None:
            ray_remote_args = {}

        def write_block(write_path: str, block: pd.DataFrame) -> str:
            _logger.debug("Writing %s file.", write_path)
            if _block_udf is not None:
                block = _block_udf(block)

            with open_s3_object(
                path=write_path,
                mode=write_args.get("mode"),
                use_threads=False,
                s3_additional_kwargs=s3_additional_kwargs,
                encoding=write_args.get("encoding"),
                newline=write_args.get("newline"),
            ) as f:
                _write_block_to_file(
                    f,
                    PandasBlockAccessor(block),
                    writer_args_fn=write_args_fn,
                    **write_args,
                )
                return write_path

        write_block_fn = cached_remote_fn(write_block).options(**ray_remote_args)

        file_format = self._file_format()
        write_tasks = []

        pandas_kwargs = write_args.get("pandas_kwargs", {})
        for block_idx, block in enumerate(blocks):
            write_path = block_path_provider(
                path,
                filesystem=filesystem,
                dataset_uuid=dataset_uuid,
                block=block,
                block_index=block_idx,
                file_format=f"{file_format}{_COMPRESSION_2_EXT.get(pandas_kwargs.get('compression'))}",
            )
            write_task = write_block_fn.remote(write_path, block)
            write_tasks.append(write_task)

        return write_tasks

    def _write_block(  # type: ignore  # pylint: disable=arguments-differ
        self,
        f: io.TextIOWrapper,
        block: PandasBlockAccessor,
        writer_args_fn: Callable[[], Dict[str, Any]],
        pandas_kwargs: Dict[str, Any],
        **writer_args: Any,
    ) -> None:
        if not self.write_text_func:
            raise RuntimeError(f"Write function not support for {self.file_format}")

        frame = block.to_pandas()

        _logger.debug("pandas_kwargs: %s", pandas_kwargs)
        self.write_text_func(frame, f, **pandas_kwargs)

    def on_write_complete(self, write_results: List[Any], **_: Any) -> None:
        """Execute callback on write complete."""
        _logger.debug("Write complete %s.", write_results)

        # Collect and return all write task paths
        self._write_paths.extend(write_results)

    def on_write_failed(self, write_results: List[ObjectRef[Any]], error: Exception, **_: Any) -> None:
        """Execute callback on write failed."""
        _logger.debug("Write failed %s.", write_results)
        raise error

    def get_write_paths(self) -> List[str]:
        """Return S3 paths of where the results have been written."""
        return self._write_paths


class PandasCSVDataSource(PandasTextDatasource):  # pylint: disable=abstract-method
    """Pandas CSV datasource, for reading and writing CSV files using Pandas."""

    def __init__(self) -> None:
        super().__init__("csv", pd.read_csv, pd.DataFrame.to_csv)

        self._supported_params_with_defaults = {
            "delimiter": ",",
        }

    def _read_stream_arrow(  # type: ignore
        self,
        f: pyarrow.NativeFile,
        path: str,
        path_root: str,
        dataset: bool,
        version_id_dict: Dict[str, Optional[str]],
        s3_additional_kwargs: Optional[Dict[str, str]],
        pandas_kwargs: Dict[str, Any],
        **reader_args: Any,
    ) -> Iterator[pd.DataFrame]:
        from pyarrow import csv

        if {key: value for key, value in version_id_dict.items() if value is not None}:
            raise NotImplementedError()

        if s3_additional_kwargs:
            raise NotImplementedError()

        for pandas_arg_key, pandas_arg_value in pandas_kwargs.items():
            if pandas_arg_key not in self._supported_params_with_defaults:
                raise NotImplementedError()

        read_options = csv.ReadOptions(
            use_threads=False,
        )
        parse_options = csv.ParseOptions(
            delimiter=pandas_kwargs.get("delimiter", self._supported_params_with_defaults["delimiter"]),
        )
        convert_options = csv.ConvertOptions()

        reader = csv.open_csv(
            f,
            read_options=read_options,
            parse_options=parse_options,
            convert_options=convert_options,
        )

        schema = None
        path_root = reader_args.get("path_root", None)

        while True:
            try:
                batch = reader.read_next_batch()
                table = pyarrow.Table.from_batches([batch], schema=schema)
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

    def _read_stream(  # type: ignore
        self,
        f: pyarrow.NativeFile,
        path: str,
        path_root: str,
        dataset: bool,
        version_id_dict: Dict[str, Optional[str]],
        s3_additional_kwargs: Optional[Dict[str, str]],
        pandas_kwargs: Dict[str, Any],
        **reader_args: Any,
    ) -> Iterator[pd.DataFrame]:  # type: ignore
        try:
            yield from self._read_stream_arrow(
                f,
                path,
                path_root,
                dataset,
                version_id_dict,
                s3_additional_kwargs,
                pandas_kwargs,
            )
        except NotImplementedError:
            _logger.warning(f"Defaulting to slower Pandas I/O operation for s3://{path}")
            yield from super()._read_stream(
                f,
                path,
                path_root,
                dataset,
                version_id_dict,
                s3_additional_kwargs,
                pandas_kwargs,
            )


class PandasFWFDataSource(PandasTextDatasource):  # pylint: disable=abstract-method
    """Pandas FWF datasource, for reading and writing FWF files using Pandas."""

    def __init__(self) -> None:
        super().__init__("fwf", pd.read_fwf, None)


class PandasJSONDatasource(PandasTextDatasource):  # pylint: disable=abstract-method
    """Pandas JSON datasource, for reading and writing JSON files using Pandas."""

    def __init__(self) -> None:
        super().__init__("json", pd.read_json, pd.DataFrame.to_json)

    def _read_stream(  # type: ignore
        self,
        f: pyarrow.NativeFile,
        path: str,
        path_root: str,
        dataset: bool,
        version_id_dict: Dict[str, Optional[str]],
        s3_additional_kwargs: Optional[Dict[str, str]],
        pandas_kwargs: Dict[str, Any],
        **reader_args: Any,
    ) -> Iterator[pd.DataFrame]:  # type: ignore
        pandas_lines = pandas_kwargs.get("lines", False)
        if pandas_lines:
            yield from super()._read_stream(
                f,
                path,
                path_root,
                dataset,
                version_id_dict,
                s3_additional_kwargs,
                pandas_kwargs,
            )
        else:
            s3_path = f"s3://{path}"
            yield _read_text_file(
                path=s3_path,
                parser_func=pd.read_json,
                path_root=path_root,
                dataset=dataset,
                boto3_session=None,
                pandas_kwargs=pandas_kwargs,
                s3_additional_kwargs=s3_additional_kwargs,
                version_id=version_id_dict.get(s3_path),
            )
