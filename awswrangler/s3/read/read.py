from io import BytesIO

import pandas

from awswrangler.common import SessionPrimitives, get_session
from awswrangler.exceptions import UnsupportedFileFormat


def parse_path(path):
    path2 = path.replace("s3://", "")
    parts = path2.partition("/")
    return parts[0], parts[2]


def read(
    path,
    header="infer",
    names=None,
    dtype=None,
    sep=",",
    lineterminator="\n",
    quotechar='"',
    quoting=0,
    escapechar=None,
    parse_dates=False,
    infer_datetime_format=False,
    encoding=None,
    file_format="csv",
    region=None,
    key=None,
    secret=None,
    profile=None,
):
    file_format = file_format.lower()
    if file_format not in ["csv"]:
        raise UnsupportedFileFormat(file_format)
    session_primitives = SessionPrimitives(
        region=region, key=key, secret=secret, profile=profile
    )
    session = get_session(session_primitives=session_primitives)
    bucket_name, key_path = parse_path(path)
    s3_client = session.client("s3", use_ssl=True)
    buff = BytesIO()
    s3_client.download_fileobj(bucket_name, key_path, buff)
    buff.seek(0),
    df = None
    if file_format == "csv":
        df = pandas.read_csv(
            buff,
            header=header,
            names=names,
            sep=sep,
            quotechar=quotechar,
            quoting=quoting,
            escapechar=escapechar,
            parse_dates=parse_dates,
            infer_datetime_format=infer_datetime_format,
            lineterminator=lineterminator,
            dtype=dtype,
            encoding=encoding,
        )
    buff.close()
    return df
