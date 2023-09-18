import os

import awswrangler as wr

workgroup_name = os.environ["athena-workgroup"]
output_path = os.environ["output-path"]
glue_database = os.environ["glue-database"]
glue_table = os.environ["glue-table"]

# Read 1.5 Gb Parquet data
df = wr.s3.read_parquet(path="s3://ursa-labs-taxi-data/2017/")

# Drop vendor_id column
df.drop("vendor_id", axis=1, inplace=True)

# Filter trips over 1 mile
df1 = df[df["trip_distance"] > 1]

# Write partitioned trips to S3 in Parquet format
wr.s3.to_parquet(
    df1,
    path=f"{output_path}output/{glue_table}/",
    partition_cols=["passenger_count", "payment_type"],
    dataset=True,
    database=glue_database,
    table=glue_table,
)

# Read the data back to a modin df via Athena
df1_athena = wr.athena.read_sql_query(
    f"SELECT * FROM {glue_table}",
    database=glue_database,
    ctas_approach=False,
    unload_approach=True,
    workgroup=workgroup_name,
    s3_output=f"{output_path}unload/{glue_table}/",
)

# Delete table (required due to LF)
wr.catalog.delete_table_if_exists(database=glue_database, table=glue_table)
