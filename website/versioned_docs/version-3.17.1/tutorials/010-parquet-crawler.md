---
id: 010-parquet-crawler
title: "Parquet Crawler"
sidebar_position: 10
sidebar_label: "10 - Parquet Crawler"
custom_edit_url: https://github.com/aws/aws-sdk-pandas/blob/main/tutorials/010%20-%20Parquet%20Crawler.ipynb
---

# Parquet Crawler

> This page is generated from [`tutorials/010 - Parquet Crawler.ipynb`](https://github.com/aws/aws-sdk-pandas/blob/main/tutorials/010%20-%20Parquet%20Crawler.ipynb). Open it in Jupyter to run it yourself.
[awswrangler](https://github.com/aws/aws-sdk-pandas) can extract only the metadata from Parquet files and Partitions and then add it to the Glue Catalog.

```python
import awswrangler as wr
```

## Enter your bucket name:

```python
import getpass

bucket = getpass.getpass()
path = f"s3://{bucket}/data/"
```

```text
 ············
```

### Creating a Parquet Table from the NOAA's CSV files

[Reference](https://registry.opendata.aws/noaa-ghcn/)

```python
cols = ["id", "dt", "element", "value", "m_flag", "q_flag", "s_flag", "obs_time"]

df = wr.s3.read_csv(
    path="s3://noaa-ghcn-pds/csv/by_year/189", names=cols, parse_dates=["dt", "obs_time"]
)  # Read 10 files from the 1890 decade (~1GB)

df
```

```text
                   id         dt element  value m_flag q_flag s_flag obs_time
0         AGE00135039 1890-01-01    TMAX    160    NaN    NaN      E      NaN
1         AGE00135039 1890-01-01    TMIN     30    NaN    NaN      E      NaN
2         AGE00135039 1890-01-01    PRCP     45    NaN    NaN      E      NaN
3         AGE00147705 1890-01-01    TMAX    140    NaN    NaN      E      NaN
4         AGE00147705 1890-01-01    TMIN     74    NaN    NaN      E      NaN
...               ...        ...     ...    ...    ...    ...    ...      ...
29249753  UZM00038457 1899-12-31    PRCP     16    NaN    NaN      r      NaN
29249754  UZM00038457 1899-12-31    TAVG    -73    NaN    NaN      r      NaN
29249755  UZM00038618 1899-12-31    TMIN    -76    NaN    NaN      r      NaN
29249756  UZM00038618 1899-12-31    PRCP      0    NaN    NaN      r      NaN
29249757  UZM00038618 1899-12-31    TAVG    -60    NaN    NaN      r      NaN

[29249758 rows x 8 columns]
```

```python
df["year"] = df["dt"].dt.year

df.head(3)
```

```text
            id         dt element  value m_flag q_flag s_flag obs_time  year
0  AGE00135039 1890-01-01    TMAX    160    NaN    NaN      E      NaN  1890
1  AGE00135039 1890-01-01    TMIN     30    NaN    NaN      E      NaN  1890
2  AGE00135039 1890-01-01    PRCP     45    NaN    NaN      E      NaN  1890
```

```python
res = wr.s3.to_parquet(
    df=df,
    path=path,
    dataset=True,
    mode="overwrite",
    partition_cols=["year"],
)
```

```python
[x.split("data/", 1)[1] for x in wr.s3.list_objects(path)]
```

```text
['year=1890/06a519afcf8e48c9b08c8908f30adcfe.snappy.parquet',
 'year=1891/5a99c28dbef54008bfc770c946099e02.snappy.parquet',
 'year=1892/9b1ea5d1cfad40f78c920f93540ca8ec.snappy.parquet',
 'year=1893/92259b49c134401eaf772506ee802af6.snappy.parquet',
 'year=1894/c734469ffff944f69dc277c630064a16.snappy.parquet',
 'year=1895/cf7ccde86aaf4d138f86c379c0817aa6.snappy.parquet',
 'year=1896/ce02f4c2c554438786b766b33db451b6.snappy.parquet',
 'year=1897/e04de04ad3c444deadcc9c410ab97ca1.snappy.parquet',
 'year=1898/acb0e02878f04b56a6200f4b5a97be0e.snappy.parquet',
 'year=1899/a269bdbb0f6a48faac55f3bcfef7df7a.snappy.parquet']
```

## Crawling!

```python
%%time

res = wr.s3.store_parquet_metadata(
    path=path, database="awswrangler_test", table="crawler", dataset=True, mode="overwrite", dtype={"year": "int"}
)
```

```text
CPU times: user 1.81 s, sys: 528 ms, total: 2.33 s
Wall time: 3.21 s
```

## Checking

```python
wr.catalog.table(database="awswrangler_test", table="crawler")
```

```text
  Column Name       Type  Partition Comment
0          id     string      False        
1          dt  timestamp      False        
2     element     string      False        
3       value     bigint      False        
4      m_flag     string      False        
5      q_flag     string      False        
6      s_flag     string      False        
7    obs_time     string      False        
8        year        int       True
```

```python
%%time

wr.athena.read_sql_query("SELECT * FROM crawler WHERE year=1890", database="awswrangler_test")
```

```text
CPU times: user 3.52 s, sys: 811 ms, total: 4.33 s
Wall time: 9.6 s
```

```text
               id         dt element  value m_flag q_flag s_flag obs_time  \
0     USC00195145 1890-01-01    TMIN    -28   <NA>   <NA>      6     <NA>   
1     USC00196770 1890-01-01    PRCP      0      P   <NA>      6     <NA>   
2     USC00196770 1890-01-01    SNOW      0   <NA>   <NA>      6     <NA>   
3     USC00196915 1890-01-01    PRCP      0      P   <NA>      6     <NA>   
4     USC00196915 1890-01-01    SNOW      0   <NA>   <NA>      6     <NA>   
...           ...        ...     ...    ...    ...    ...    ...      ...   
6139  ASN00022006 1890-12-03    PRCP      0   <NA>   <NA>      a     <NA>   
6140  ASN00022007 1890-12-03    PRCP      0   <NA>   <NA>      a     <NA>   
6141  ASN00022008 1890-12-03    PRCP      0   <NA>   <NA>      a     <NA>   
6142  ASN00022009 1890-12-03    PRCP      0   <NA>   <NA>      a     <NA>   
6143  ASN00022011 1890-12-03    PRCP      0   <NA>   <NA>      a     <NA>   

      year  
0     1890  
1     1890  
2     1890  
3     1890  
4     1890  
...    ...  
6139  1890  
6140  1890  
6141  1890  
6142  1890  
6143  1890  

[1276246 rows x 9 columns]
```

## Cleaning Up S3

```python
wr.s3.delete_objects(path)
```

## Cleaning Up the Database

```python
for table in wr.catalog.get_tables(database="awswrangler_test"):
    wr.catalog.delete_table_if_exists(database="awswrangler_test", table=table["Name"])
```
