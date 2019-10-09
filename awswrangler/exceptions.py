class UnsupportedType(Exception):
    pass


class UndetectedType(Exception):
    pass


class UnsupportedFileFormat(Exception):
    pass


class UnsupportedWriteMode(Exception):
    pass


class InvalidArguments(Exception):
    pass


class InvalidDataframeType(Exception):
    pass


class RedshiftLoadError(Exception):
    pass


class AthenaQueryError(Exception):
    pass


class EmptyS3Object(Exception):
    pass


class LineTerminatorNotFound(Exception):
    pass


class MissingBatchDetected(Exception):
    pass


class InvalidRedshiftDiststyle(Exception):
    pass


class InvalidRedshiftDistkey(Exception):
    pass


class InvalidRedshiftSortstyle(Exception):
    pass


class InvalidRedshiftSortkey(Exception):
    pass


class EmptyDataframe(Exception):
    pass


class QueryCancelled(Exception):
    pass


class QueryFailed(Exception):
    pass


class InvalidSerDe(Exception):
    pass


class ApiError(Exception):
    pass


class InvalidCompression(Exception):
    pass
