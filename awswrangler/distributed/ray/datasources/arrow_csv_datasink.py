"""Ray PandasTextDatasink Module."""

from __future__ import annotations

import io
import logging
from typing import Any

from pyarrow import csv
from ray.data.block import BlockAccessor
from ray.data.datasource.filename_provider import FilenameProvider

from awswrangler.distributed.ray.datasources.file_datasink import _BlockFileDatasink

_logger: logging.Logger = logging.getLogger(__name__)


class ArrowCSVDatasink(_BlockFileDatasink):
    """A datasink that writes CSV files using Arrow."""

    def __init__(
        self,
        path: str,
        *,
        filename_provider: FilenameProvider | None = None,
        dataset_uuid: str | None = None,
        open_s3_object_args: dict[str, Any] | None = None,
        pandas_kwargs: dict[str, Any] | None = None,
        write_options: dict[str, Any] | None = None,
        **write_args: Any,
    ):
        super().__init__(
            path,
            file_format="csv",
            filename_provider=filename_provider,
            dataset_uuid=dataset_uuid,
            open_s3_object_args=open_s3_object_args,
            pandas_kwargs=pandas_kwargs,
            **write_args,
        )

        self.write_options = write_options or {}

    def write_block(self, file: io.TextIOWrapper, block: BlockAccessor) -> None:
        """
        Write a block of data to a file.

        Parameters
        ----------
        block : BlockAccessor
        file : io.TextIOWrapper
        """
        csv.write_csv(block.to_arrow(), file, csv.WriteOptions(**self.write_options))
