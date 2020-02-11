"""Exceptions Module."""


class UnsupportedType(Exception):
    """UnsupportedType exception."""

    pass


class UndetectedType(Exception):
    """UndetectedType exception."""

    pass


class UnsupportedFileFormat(Exception):
    """UnsupportedFileFormat exception."""

    pass


class UnsupportedWriteMode(Exception):
    """UnsupportedWriteMode exception."""

    pass


class InvalidArguments(Exception):
    """InvalidArguments exception."""

    pass


class InvalidDataframeType(Exception):
    """InvalidDataframeType exception."""

    pass


class RedshiftLoadError(Exception):
    """RedshiftLoadError exception."""

    pass


class AuroraLoadError(Exception):
    """AuroraLoadError exception."""

    pass


class AthenaQueryError(Exception):
    """AthenaQueryError exception."""

    pass


class EmptyS3Object(Exception):
    """EmptyS3Object exception."""

    pass


class LineTerminatorNotFound(Exception):
    """LineTerminatorNotFound exception."""

    pass


class MissingBatchDetected(Exception):
    """MissingBatchDetected exception."""

    pass


class InvalidRedshiftDiststyle(Exception):
    """InvalidRedshiftDiststyle exception."""

    pass


class InvalidRedshiftDistkey(Exception):
    """InvalidRedshiftDistkey exception."""

    pass


class InvalidRedshiftSortstyle(Exception):
    """InvalidRedshiftSortstyle exception."""

    pass


class InvalidRedshiftSortkey(Exception):
    """InvalidRedshiftSortkey exception."""

    pass


class InvalidRedshiftPrimaryKeys(Exception):
    """InvalidRedshiftPrimaryKeys exception."""

    pass


class EmptyDataframe(Exception):
    """EmptyDataframe exception."""

    pass


class QueryCancelled(Exception):
    """QueryCancelled exception."""

    pass


class QueryFailed(Exception):
    """QueryFailed exception."""

    pass


class InvalidSerDe(Exception):
    """InvalidSerDe exception."""

    pass


class ApiError(Exception):
    """ApiError exception."""

    pass


class InvalidCompression(Exception):
    """InvalidCompression exception."""

    pass


class InvalidTable(Exception):
    """InvalidTable exception."""

    pass


class InvalidParameters(Exception):
    """InvalidParameters exception."""

    pass


class AWSCredentialsNotFound(Exception):
    """AWSCredentialsNotFound exception."""

    pass


class InvalidEngine(Exception):
    """InvalidEngine exception."""

    pass


class InvalidSagemakerOutput(Exception):
    """InvalidSagemakerOutput exception."""

    pass


class S3WaitObjectTimeout(Exception):
    """S3WaitObjectTimeout exception."""

    pass
