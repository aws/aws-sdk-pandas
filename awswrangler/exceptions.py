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


class MissingBatchDetected(Exception):
    pass
