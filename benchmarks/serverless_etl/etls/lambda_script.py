import awswrangler


def lambda_handler(event, context):

    bucket = event.get("--bucket")
    key = event.get("--workload")
    output_name = key.replace(".csv", "")
    path_input = "s3://" + bucket + "/" + key
    path_output = "s3://" + bucket + "/lambda_output/" + output_name

    print("bucket: " + bucket)
    print("key: " + key)
    print("output_name: " + output_name)
    print("path_input: " + path_input)
    print("path_output: " + path_output)

    df = awswrangler.s3.read(path=path_input)
    awswrangler.s3.write(
        df=df, path=path_output, file_format="parquet", partition_cols=["col3"]
    )
