import ray
import awswrangler as wr


import time
import logging
logging.basicConfig(level=logging.INFO, format="[%(name)s][%(funcName)s] %(message)s")
logging.getLogger("awswrangler").setLevel(logging.DEBUG)
logging.getLogger("botocore.credentials").setLevel(logging.CRITICAL)

start = time.time()
df = wr.s3.select_query(
        sql="SELECT * FROM s3object s where s.\"star_rating\" >= 5",
        path="s3://amazon-reviews-pds/parquet/product_category=Gift_Card/part-00000-495c48e6-96d6-4650-aa65-3c36a3516ddd.c000.snappy.parquet",
        input_serialization="Parquet",
        input_serialization_params={},
        use_threads=True,
)
end = time.time()

print(df)
print(f"Elapsed time: {end - start}")