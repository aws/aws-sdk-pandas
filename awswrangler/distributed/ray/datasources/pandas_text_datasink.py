"""Ray PandasTextDatasink Module."""

import io
import logging
from typing import Callable, Optional

import pandas as pd
from ray.data.block import BlockAccessor

from awswrangler.distributed.ray.datasources.file_datasink import _BlockFileDatasink

_logger: logging.Logger = logging.getLogger(__name__)


class _PandasTextDatasink(_BlockFileDatasink):
    """A datasink that writes text files using Pandas IO."""

    def __init__(
        self,
        path: str,
        write_text_func: Optional[Callable[..., None]],
        file_format: str,
        **file_datasink_kwargs,
    ):
        super().__init__(path, file_format=file_format, **file_datasink_kwargs)

        self.write_text_func = write_text_func

    def write_block_to_file(self, file: io.TextIOWrapper, block: BlockAccessor):
        """
        Write a block of data to a file.

        Parameters
        ----------
        block : BlockAccessor
        file : pa.NativeFile
        """
        write_text_func = self.write_text_func

        write_text_func(block.to_pandas(), file, **self.pandas_args)  # type: ignore[misc]


class PandasCSVDatasink(_PandasTextDatasink):  # pylint: disable=abstract-method
    """A datasink that writes CSV files using Pandas IO."""

    def __init__(
        self,
        path: str,
        **file_datasink_kwargs,
    ) -> None:
        super().__init__(
            path,
            pd.DataFrame.to_csv,
            file_format="csv",
            **file_datasink_kwargs,
        )


class PandasJSONDatasink(_PandasTextDatasink):  # pylint: disable=abstract-method
    """A datasink that writes CSV files using Pandas IO."""

    def __init__(
        self,
        path: str,
        **file_datasink_kwargs,
    ) -> None:
        super().__init__(
            path,
            pd.DataFrame.to_json,
            file_format="json",
            **file_datasink_kwargs,
        )
