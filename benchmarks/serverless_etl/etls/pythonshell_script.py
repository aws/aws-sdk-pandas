import sys
import awswrangler


def get_arg(arg_str):
    _next = False
    for arg in sys.argv:
        if _next:
            return arg
        if arg == arg_str:
            _next = True


bucket = get_arg("--bucket")
key = get_arg("--workload")
output_name = key.replace(".csv", "")
path_input = "s3://" + bucket + "/" + key
path_output = "s3://" + bucket + "/pythonshell_output/" + output_name

print("bucket: " + bucket)
print("key: " + key)
print("output_name: " + output_name)
print("path_input: " + path_input)
print("path_output: " + path_output)

df = awswrangler.s3.read(path=path_input)
awswrangler.s3.write(
    df=df, path=path_output, file_format="parquet", partition_cols=["col3"]
)
