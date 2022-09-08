"""Distributed Datasources Module."""

from awswrangler.distributed.datasources.pandas_text_datasource import PandasTextDatasource
from awswrangler.distributed.datasources.parquet_datasource import ParquetDatasource

__all__ = [
    "PandasTextDatasource",
    "ParquetDatasource",
]
