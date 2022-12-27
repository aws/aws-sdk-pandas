from datetime import datetime 
import random 
import awswrangler as wr
import pandas as pd

test_names = [
  "example",
  "example_1",
  "example_2"
]
glue_database = "githubloadtestsanalyticsdatabase78d628b2"
glue_table = "githubloadtestsanalyticstableb54b6dc9"
iterations = 10
path = f"s3://githubloadtests-analyticsbucket99427767-1jtlxn0w1vkdv/"

for i in range(iterations):
  df = pd.DataFrame({
      "date": [datetime.now()],
      "test": [test_names[random.randint(0,2)]],
      "version": [wr.__version__],
      "elapsed_time": [random.randint(1,10)]
  })
  print(df)

  wr.s3.to_parquet(
      df=df,
      path=path,
      dataset=True,
      mode="append",
      partition_cols=["test"],
      database=glue_database,
      table=glue_table,
  )

output_df = wr.s3.read_parquet(path, dataset=True)
print(output_df)