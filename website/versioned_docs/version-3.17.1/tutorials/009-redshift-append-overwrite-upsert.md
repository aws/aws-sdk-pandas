---
id: 009-redshift-append-overwrite-upsert
title: "Redshift - Append, Overwrite, Upsert"
sidebar_position: 9
sidebar_label: "9 - Redshift - Append, Overwrite, Upsert"
custom_edit_url: https://github.com/aws/aws-sdk-pandas/blob/main/tutorials/009%20-%20Redshift%20-%20Append%2C%20Overwrite%2C%20Upsert.ipynb
---

# Redshift - Append, Overwrite, Upsert

> This page is generated from [`tutorials/009 - Redshift - Append, Overwrite, Upsert.ipynb`](https://github.com/aws/aws-sdk-pandas/blob/main/tutorials/009%20-%20Redshift%20-%20Append%2C%20Overwrite%2C%20Upsert.ipynb). Open it in Jupyter to run it yourself.
awswrangler's `copy/to_sql` function has three different `mode` options for Redshift.

1 - `append`

2 - `overwrite`

3 - `upsert`

```python
# Install the optional modules first
!pip install 'awswrangler[redshift]'
```

```python
from datetime import date

import pandas as pd

import awswrangler as wr

con = wr.redshift.connect("aws-sdk-pandas-redshift")
```

## Enter your bucket name:

```python
import getpass

bucket = getpass.getpass()
path = f"s3://{bucket}/stage/"
```

```text
 ···········································
```

## Enter your IAM ROLE ARN:

```python
iam_role = getpass.getpass()
```

```text
 ····················································································
```

### Creating the table (Overwriting if it exists)

```python
df = pd.DataFrame({"id": [1, 2], "value": ["foo", "boo"], "date": [date(2020, 1, 1), date(2020, 1, 2)]})

wr.redshift.copy(
    df=df,
    path=path,
    con=con,
    schema="public",
    table="my_table",
    mode="overwrite",
    iam_role=iam_role,
    primary_keys=["id"],
)

wr.redshift.read_sql_table(table="my_table", schema="public", con=con)
```

```text
   id value        date
0   2   boo  2020-01-02
1   1   foo  2020-01-01
```

## Appending

```python
df = pd.DataFrame({"id": [3], "value": ["bar"], "date": [date(2020, 1, 3)]})

wr.redshift.copy(
    df=df, path=path, con=con, schema="public", table="my_table", mode="append", iam_role=iam_role, primary_keys=["id"]
)

wr.redshift.read_sql_table(table="my_table", schema="public", con=con)
```

```text
   id value        date
0   1   foo  2020-01-01
1   2   boo  2020-01-02
2   3   bar  2020-01-03
```

## Upserting

```python
df = pd.DataFrame({"id": [2, 3], "value": ["xoo", "bar"], "date": [date(2020, 1, 2), date(2020, 1, 3)]})

wr.redshift.copy(
    df=df, path=path, con=con, schema="public", table="my_table", mode="upsert", iam_role=iam_role, primary_keys=["id"]
)

wr.redshift.read_sql_table(table="my_table", schema="public", con=con)
```

```text
   id value        date
0   1   foo  2020-01-01
1   2   xoo  2020-01-02
2   3   bar  2020-01-03
```

## Cleaning Up

```python
with con.cursor() as cursor:
    cursor.execute("DROP TABLE public.my_table")
con.close()
```
