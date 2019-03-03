def write_csv_dataframe(df, path, preserve_index, fs):
    csv_buffer = df.to_csv(None, header=False, index=preserve_index).encode()
    with fs.open(path, "wb") as f:
        f.write(csv_buffer)
