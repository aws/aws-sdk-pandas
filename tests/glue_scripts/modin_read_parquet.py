import os

import modin.pandas as pd

pd.read_parquet(path=f"s3://{os.environ['data-gen-bucket']}/parquet/small/partitioned/")
