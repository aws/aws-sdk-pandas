import sys


def write_csv_dataframe(df, path, preserve_index, fs):
    if sys.version_info.major > 2:
        csv_buffer = bytes(df.to_csv(None, header=False, index=preserve_index), "utf-8")
    else:
        csv_buffer = bytes(df.to_csv(None, header=False, index=preserve_index))
    with fs.open(path, "wb") as f:
        f.write(csv_buffer)
