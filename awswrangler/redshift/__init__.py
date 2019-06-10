from .write import write as _write


def write(
    df,
    path,
    glue_connection,
    schema,
    table,
    iam_role,
    preserve_index=False,
    mode="append",
    region=None,
    key=None,
    secret=None,
    profile=None,
):
    return _write(**locals())
