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


class AthenaQueryError(Exception):
    """AthenaQueryError exception."""
