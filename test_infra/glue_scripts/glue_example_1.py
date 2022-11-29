import os

import awswrangler as wr


bucket_name = os.environ["glue-ray-data-bucket"]
workgroup_name = os.environ["athena-workgroup"]
category = "toys"
glue_database = "amzn_reviews"
glue_table = f"{category}_5_stars"

wr.catalog.create_database(name=glue_database, exist_ok=True)
wr.s3.delete_objects(f"s3://{bucket_name}/")

# Read Parquet data (1.2 Gb parquet compressed)
df = wr.s3.read_parquet(
    path=f"s3://amazon-reviews-pds/parquet/product_category={category.title()}/",
)

# Drop customer_id column
df.drop("customer_id", axis=1, inplace=True)

# Filter reviews with 5-star rating
df5 = df[df["star_rating"] == 5]

# Write partitioned five stars reviews to S3 in Parquet format
wr.s3.to_parquet(
    df5,
    path=f"s3://{bucket_name}/{category}/",
    partition_cols=["year", "marketplace"],
    dataset=True,
    database=glue_database,
    table=glue_table,
)

# Read the data back to a modin df via Athena
df5_athena = wr.athena.read_sql_query(
    f"SELECT * FROM {glue_table}",
    database=glue_database,
    ctas_approach=False,
    unload_approach=True,
    workgroup=workgroup_name,
    s3_output=f"s3://{bucket_name}/unload/{category}/",
)
