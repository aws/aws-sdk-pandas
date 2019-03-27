import sys

from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions

args = getResolvedOptions(sys.argv, ["workload", "bucket"])
key = args.get("workload")
bucket = args.get("bucket")
output_name = key.replace(".csv", "")
path_input = "s3://" + bucket + "/" + key
path_output = "s3://" + bucket + "/pyspark_output/" + output_name

print("bucket: " + bucket)
print("key: " + key)
print("output_name: " + output_name)
print("path_input: " + path_input)
print("path_output: " + path_output)

glueContext = GlueContext(SparkContext.getOrCreate())

df = glueContext.create_dynamic_frame_from_options(
    connection_type="s3",
    connection_options={"paths": [path_input]},
    format="csv",
    format_options={"withHeader": True, "separator": ","},
)

glueContext.write_dynamic_frame.from_options(
    frame=df,
    connection_type="s3",
    connection_options={"path": path_output, "partitionKeys": ["col3"]},
    format="parquet",
)
