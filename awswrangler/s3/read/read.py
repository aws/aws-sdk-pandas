import sys

import pandas

from awswrangler.common import SessionPrimitives, get_session, calculate_bounders

if sys.version_info.major > 2:
    from io import StringIO  # noqa
else:
    from StringIO import StringIO  # noqa


def parse_path(path):
    path2 = path.replace("s3://", "")
    parts = path2.partition("/")
    return parts[0], parts[2]


def read(
    path,
    file_format="csv",
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
    encoding="utf-8",
    max_size=None,
    region=None,
    key=None,
    secret=None,
    profile=None,
):
    session_primitives = SessionPrimitives(
        region=region, key=key, secret=secret, profile=profile
    )
    session = get_session(session_primitives=session_primitives)
    bucket_name, key_path = parse_path(path)
    bucket = session.resource("s3").Bucket(bucket_name)
    objs = bucket.objects.filter(Prefix=key_path)
    for obj in objs:
        if obj.size > 0:
            bounders = calculate_bounders(obj.size, max_size=max_size)
            bounders_len = len(bounders)
            count = 0
            forgotten_bytes = 0
            cols_names = None
            for ini, end in bounders:
                count += 1
                ini -= forgotten_bytes
                end -= 1  # Range is inclusive, contrary to Python's List
                bytes_range = "bytes={}-{}".format(ini, end)
                body = (
                    obj.get(Range=bytes_range)
                    .get("Body")
                    .read()
                    .decode(encoding, errors="ignore")
                )
                chunk_size = len(body)
                if body[0] == lineterminator:
                    first_char = 1
                else:
                    first_char = 0
                if (count == 1) and (count == bounders_len):
                    last_break_line_idx = chunk_size
                elif count == 1:  # first chunk
                    last_break_line_idx = body.rindex(lineterminator)
                    forgotten_bytes = chunk_size - last_break_line_idx
                elif count == bounders_len:  # Last chunk
                    header = None
                    names = cols_names
                    last_break_line_idx = chunk_size
                else:
                    header = None
                    names = cols_names
                    last_break_line_idx = body.rindex(lineterminator)
                    forgotten_bytes = chunk_size - last_break_line_idx
                df = pandas.read_csv(
                    StringIO(body[first_char:last_break_line_idx]),
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
                yield df
                if count == 1:  # first chunk
                    cols_names = df.columns
