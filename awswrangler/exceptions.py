class UnsupportedType(Exception):
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
