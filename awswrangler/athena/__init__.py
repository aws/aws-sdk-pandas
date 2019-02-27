from . import read as _read


def read(
    database, query, s3_output=None, region=None, key=None, secret=None, profile=None
):
    return _read.read(**locals())
