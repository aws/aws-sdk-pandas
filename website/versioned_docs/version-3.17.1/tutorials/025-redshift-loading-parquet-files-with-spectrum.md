---
id: 025-redshift-loading-parquet-files-with-spectrum
title: "Redshift - Loading Parquet files with Spectrum"
sidebar_position: 25
sidebar_label: "25 - Redshift - Loading Parquet files with Spectrum"
custom_edit_url: https://github.com/aws/aws-sdk-pandas/blob/main/tutorials/025%20-%20Redshift%20-%20Loading%20Parquet%20files%20with%20Spectrum.ipynb
---

# Redshift - Loading Parquet files with Spectrum

> This page is generated from [`tutorials/025 - Redshift - Loading Parquet files with Spectrum.ipynb`](https://github.com/aws/aws-sdk-pandas/blob/main/tutorials/025%20-%20Redshift%20-%20Loading%20Parquet%20files%20with%20Spectrum.ipynb). Open it in Jupyter to run it yourself.
## Enter your bucket name:

```python
# Install the optional modules first
!pip install 'awswrangler[redshift]'
```

```python
import getpass

bucket = getpass.getpass()
PATH = f"s3://{bucket}/files/"
```

```text
 ···········································
```

## Mocking some Parquet Files on S3

```python
import pandas as pd

import awswrangler as wr

df = pd.DataFrame(
    {
        "col0": [0, 1, 2, 3, 4, 5, 6, 7, 8, 9],
        "col1": ["a", "b", "c", "d", "e", "f", "g", "h", "i", "j"],
    }
)

df
```

```text
   col0 col1
0     0    a
1     1    b
2     2    c
3     3    d
4     4    e
5     5    f
6     6    g
7     7    h
8     8    i
9     9    j
```

```python
wr.s3.to_parquet(df, PATH, max_rows_by_file=2, dataset=True, mode="overwrite")
```

## Crawling the metadata and adding into Glue Catalog

```python
wr.s3.store_parquet_metadata(path=PATH, database="aws_sdk_pandas", table="test", dataset=True, mode="overwrite")
```

```text
({'col0': 'bigint', 'col1': 'string'}, None, None)
```

## Running the CTAS query to load the data into Redshift storage

```python
con = wr.redshift.connect(connection="aws-sdk-pandas-redshift")
```

```python
query = "CREATE TABLE public.test AS (SELECT * FROM aws_sdk_pandas_external.test)"
```

```python
with con.cursor() as cursor:
    cursor.execute(query)
```

## Running an INSERT INTO query to load MORE data into Redshift storage

```python
df = pd.DataFrame(
    {
        "col0": [10, 11],
        "col1": ["k", "l"],
    }
)
wr.s3.to_parquet(df, PATH, dataset=True, mode="overwrite")
```

```python
query = "INSERT INTO public.test (SELECT * FROM aws_sdk_pandas_external.test)"
```

```python
with con.cursor() as cursor:
    cursor.execute(query)
```

## Checking the result

```python
query = "SELECT * FROM public.test"
```

```python
wr.redshift.read_sql_table(con=con, schema="public", table="test")
```

```text
    col0 col1
0      5    f
1      1    b
2      3    d
3      6    g
4      8    i
5     10    k
6      4    e
7      0    a
8      2    c
9      7    h
10     9    j
11    11    l
```

```python
con.close()
```
