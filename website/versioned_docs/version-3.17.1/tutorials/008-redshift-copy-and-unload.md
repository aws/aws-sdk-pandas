---
id: 008-redshift-copy-and-unload
title: "Redshift - Copy & Unload"
sidebar_position: 8
sidebar_label: "8 - Redshift - Copy & Unload"
custom_edit_url: https://github.com/aws/aws-sdk-pandas/blob/main/tutorials/008%20-%20Redshift%20-%20Copy%20%26%20Unload.ipynb
---

# Redshift - Copy & Unload

> This page is generated from [`tutorials/008 - Redshift - Copy & Unload.ipynb`](https://github.com/aws/aws-sdk-pandas/blob/main/tutorials/008%20-%20Redshift%20-%20Copy%20%26%20Unload.ipynb). Open it in Jupyter to run it yourself.
`Amazon Redshift` has two SQL command that help to load and unload large amount of data staging it on `Amazon S3`:

1 - [COPY](https://docs.aws.amazon.com/redshift/latest/dg/r_COPY.html)

2 - [UNLOAD](https://docs.aws.amazon.com/redshift/latest/dg/r_UNLOAD.html)

Let's take a look and how awswrangler can use it.

```python
# Install the optional modules first
!pip install 'awswrangler[redshift]'
```

```python
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

### Creating a DataFrame from the NOAA's CSV files

[Reference](https://registry.opendata.aws/noaa-ghcn/)

```python
cols = ["id", "dt", "element", "value", "m_flag", "q_flag", "s_flag", "obs_time"]

df = wr.s3.read_csv(
    path="s3://noaa-ghcn-pds/csv/by_year/1897.csv", names=cols, parse_dates=["dt", "obs_time"]
)  # ~127MB, ~4MM rows

df
```

```text
                  id         dt element  value m_flag q_flag s_flag obs_time
0        AG000060590 1897-01-01    TMAX    170    NaN    NaN      E      NaN
1        AG000060590 1897-01-01    TMIN    -14    NaN    NaN      E      NaN
2        AG000060590 1897-01-01    PRCP      0    NaN    NaN      E      NaN
3        AGE00135039 1897-01-01    TMAX    140    NaN    NaN      E      NaN
4        AGE00135039 1897-01-01    TMIN     40    NaN    NaN      E      NaN
...              ...        ...     ...    ...    ...    ...    ...      ...
3923594  UZM00038457 1897-12-31    TMIN   -145    NaN    NaN      r      NaN
3923595  UZM00038457 1897-12-31    PRCP      4    NaN    NaN      r      NaN
3923596  UZM00038457 1897-12-31    TAVG    -95    NaN    NaN      r      NaN
3923597  UZM00038618 1897-12-31    PRCP     66    NaN    NaN      r      NaN
3923598  UZM00038618 1897-12-31    TAVG    -45    NaN    NaN      r      NaN

[3923599 rows x 8 columns]
```

## Load and Unload with COPY and UNLOAD commands

> Note: Please use a empty S3 path for the COPY command.

```python
%%time

wr.redshift.copy(
    df=df,
    path=path,
    con=con,
    schema="public",
    table="commands",
    mode="overwrite",
    iam_role=iam_role,
)
```

```text
CPU times: user 2.78 s, sys: 293 ms, total: 3.08 s
Wall time: 20.7 s
```

```python
%%time

wr.redshift.unload(
    sql="SELECT * FROM public.commands",
    con=con,
    iam_role=iam_role,
    path=path,
    keep_files=True,
)
```

```text
CPU times: user 10 s, sys: 1.14 s, total: 11.2 s
Wall time: 27.5 s
```

```text
                  id         dt element  value m_flag q_flag s_flag obs_time
0        AG000060590 1897-01-01    TMAX    170   <NA>   <NA>      E     <NA>
1        AG000060590 1897-01-01    PRCP      0   <NA>   <NA>      E     <NA>
2        AGE00135039 1897-01-01    TMIN     40   <NA>   <NA>      E     <NA>
3        AGE00147705 1897-01-01    TMAX    164   <NA>   <NA>      E     <NA>
4        AGE00147705 1897-01-01    PRCP      0   <NA>   <NA>      E     <NA>
...              ...        ...     ...    ...    ...    ...    ...      ...
3923594  USW00094967 1897-12-31    TMAX   -144   <NA>   <NA>      6     <NA>
3923595  USW00094967 1897-12-31    PRCP      0      P   <NA>      6     <NA>
3923596  UZM00038457 1897-12-31    TMAX    -49   <NA>   <NA>      r     <NA>
3923597  UZM00038457 1897-12-31    PRCP      4   <NA>   <NA>      r     <NA>
3923598  UZM00038618 1897-12-31    PRCP     66   <NA>   <NA>      r     <NA>

[7847198 rows x 8 columns]
```

```python
con.close()
```
