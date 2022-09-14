"""Distributed Datasources Module."""

from awswrangler.distributed.datasources.parquet_datasource import (
    ParquetDatasource,
    UserProvidedKeyBlockWritePathProvider,
)

__all__ = [
    "ParquetDatasource",
    "UserProvidedKeyBlockWritePathProvider",
]
