"""Ray compatibility module to handle API changes across versions."""

from __future__ import annotations

import logging

_logger = logging.getLogger(__name__)

# Handle ParquetMetadataProvider import
try:
    from ray.data.datasource.parquet_meta_provider import ParquetMetadataProvider

    _logger.debug("Using ParquetMetadataProvider from parquet_meta_provider module")
except ImportError:
    try:
        from ray.data.datasource.file_meta_provider import ParquetMetadataProvider

        _logger.debug("Using ParquetMetadataProvider from file_meta_provider module")
    except ImportError:
        from ray.data.datasource.file_meta_provider import DefaultFileMetadataProvider

        ParquetMetadataProvider = DefaultFileMetadataProvider
        _logger.warning("ParquetMetadataProvider not found, using DefaultFileMetadataProvider as fallback")

# Handle FastFileMetadataProvider import
try:
    from ray.data.datasource import FastFileMetadataProvider

    _logger.debug("Using FastFileMetadataProvider from datasource module")
except ImportError:
    try:
        from ray.data.datasource.file_meta_provider import FastFileMetadataProvider

        _logger.debug("Using FastFileMetadataProvider from file_meta_provider module")
    except ImportError:
        from ray.data.datasource.file_meta_provider import DefaultFileMetadataProvider

        FastFileMetadataProvider = DefaultFileMetadataProvider
        _logger.warning("FastFileMetadataProvider not found, using DefaultFileMetadataProvider as fallback")

__all__ = ["ParquetMetadataProvider", "FastFileMetadataProvider"]
