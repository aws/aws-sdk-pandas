"""Distributed Datasources Module."""

from awswrangler.distributed.datasources.csv_datasource import CSVDatasource
from awswrangler.distributed.datasources.parquet_datasource import ParquetDatasource

__all__ = [
    "CSVDatasource",
    "ParquetDatasource",
]
