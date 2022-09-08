"""Distributed Datasources Module."""

from awswrangler.distributed.datasources.pandas_datasource import PandasDatasource
from awswrangler.distributed.datasources.parquet_datasource import ParquetDatasource

__all__ = [
    "PandasDatasource",
    "ParquetDatasource",
]
