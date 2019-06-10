def write_csv_dataframe(df, path, preserve_index, fs):
    csv_buffer = bytes(df.to_csv(None, header=False, index=preserve_index), "utf-8")
    with fs.open(path, "wb") as f:
        f.write(csv_buffer)
