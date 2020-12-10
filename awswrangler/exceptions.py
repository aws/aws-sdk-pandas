"""Centralized exceptions Module."""


class InvalidCompression(Exception):
    """Invalid compression format."""


class InvalidArgumentValue(Exception):
    """Invalid argument value."""


class InvalidArgumentType(Exception):
    """Invalid argument type."""


class InvalidArgumentCombination(Exception):
    """Invalid argument combination."""


class InvalidArgument(Exception):
    """Invalid argument."""


class UnsupportedType(Exception):
    """UnsupportedType exception."""


class UndetectedType(Exception):
    """UndetectedType exception."""


class ServiceApiError(Exception):
    """ServiceApiError exception."""


class InvalidTable(Exception):
    """InvalidTable exception."""


class QueryFailed(Exception):
    """QueryFailed exception."""


class QueryCancelled(Exception):
    """QueryCancelled exception."""


class EmptyDataFrame(Exception):
    """EmptyDataFrame exception."""


class InvalidConnection(Exception):
    """InvalidConnection exception."""


class InvalidDatabaseType(Exception):
    """InvalidDatabaseEngine exception."""


class RedshiftLoadError(Exception):
    """RedshiftLoadError exception."""


class InvalidRedshiftDiststyle(Exception):
    """InvalidRedshiftDiststyle exception."""


class InvalidRedshiftDistkey(Exception):
    """InvalidRedshiftDistkey exception."""


class InvalidRedshiftSortstyle(Exception):
    """InvalidRedshiftSortstyle exception."""


class InvalidRedshiftSortkey(Exception):
    """InvalidRedshiftSortkey exception."""


class InvalidRedshiftPrimaryKeys(Exception):
    """InvalidRedshiftPrimaryKeys exception."""


class InvalidSchemaConvergence(Exception):
    """InvalidSchemaMerge exception."""


class InvalidCtasApproachQuery(Exception):
    """InvalidCtasApproachQuery exception."""


class InvalidConfiguration(Exception):
    """InvalidConfiguration exception."""


class NoFilesFound(Exception):
    """NoFilesFound exception."""


class InvalidDataFrame(Exception):
    """InvalidDataFrame."""


class InvalidFile(Exception):
    """InvalidFile."""
