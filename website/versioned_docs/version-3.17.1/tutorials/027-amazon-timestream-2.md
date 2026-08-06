---
id: 027-amazon-timestream-2
title: "Amazon Timestream 2"
sidebar_position: 27
sidebar_label: "27 - Amazon Timestream 2"
custom_edit_url: https://github.com/aws/aws-sdk-pandas/blob/main/tutorials/027%20-%20Amazon%20Timestream%202.ipynb
---

# Amazon Timestream 2

> This page is generated from [`tutorials/027 - Amazon Timestream 2.ipynb`](https://github.com/aws/aws-sdk-pandas/blob/main/tutorials/027%20-%20Amazon%20Timestream%202.ipynb). Open it in Jupyter to run it yourself.
## Reading test data

```python
from datetime import datetime

import pandas as pd

import awswrangler as wr

df = pd.read_csv(
    "https://raw.githubusercontent.com/aws/amazon-timestream-tools/master/sample_apps/data/sample.csv",
    names=[
        "ignore0",
        "region",
        "ignore1",
        "az",
        "ignore2",
        "hostname",
        "measure_kind",
        "measure",
        "ignore3",
        "ignore4",
        "ignore5",
    ],
    usecols=["region", "az", "hostname", "measure_kind", "measure"],
)
df["time"] = datetime.now()
df.reset_index(inplace=True, drop=False)

df
```

```text
         index      region           az    hostname        measure_kind  \
0            0   us-east-1   us-east-1a  host-fj2hx     cpu_utilization   
1            1   us-east-1   us-east-1a  host-fj2hx  memory_utilization   
2            2   us-east-1   us-east-1a  host-6kMPE     cpu_utilization   
3            3   us-east-1   us-east-1a  host-6kMPE  memory_utilization   
4            4   us-east-1   us-east-1a  host-sxj7X     cpu_utilization   
...        ...         ...          ...         ...                 ...   
125995  125995  eu-north-1  eu-north-1c  host-De8RB  memory_utilization   
125996  125996  eu-north-1  eu-north-1c  host-2z8tn  memory_utilization   
125997  125997  eu-north-1  eu-north-1c  host-2z8tn     cpu_utilization   
125998  125998  eu-north-1  eu-north-1c  host-9FczW  memory_utilization   
125999  125999  eu-north-1  eu-north-1c  host-9FczW     cpu_utilization   

          measure                       time  
0       21.394363 2020-12-08 16:18:47.599597  
1       68.563420 2020-12-08 16:18:47.599597  
2       17.144579 2020-12-08 16:18:47.599597  
3       73.507870 2020-12-08 16:18:47.599597  
4       26.584865 2020-12-08 16:18:47.599597  
...           ...                        ...  
125995  68.063468 2020-12-08 16:18:47.599597  
125996  72.203680 2020-12-08 16:18:47.599597  
125997  29.212219 2020-12-08 16:18:47.599597  
125998  71.746134 2020-12-08 16:18:47.599597  
125999   1.677793 2020-12-08 16:18:47.599597  

[126000 rows x 7 columns]
```

## Creating resources

```python
wr.timestream.create_database("sampleDB")
wr.timestream.create_table("sampleDB", "sampleTable", memory_retention_hours=1, magnetic_retention_days=1)
```

## Write CPU_UTILIZATION records

```python
df_cpu = df[df.measure_kind == "cpu_utilization"].copy()
df_cpu.rename(columns={"measure": "cpu_utilization"}, inplace=True)
df_cpu
```

```text
         index      region           az    hostname     measure_kind  \
0            0   us-east-1   us-east-1a  host-fj2hx  cpu_utilization   
2            2   us-east-1   us-east-1a  host-6kMPE  cpu_utilization   
4            4   us-east-1   us-east-1a  host-sxj7X  cpu_utilization   
6            6   us-east-1   us-east-1a  host-ExOui  cpu_utilization   
8            8   us-east-1   us-east-1a  host-Bwb3j  cpu_utilization   
...        ...         ...          ...         ...              ...   
125990  125990  eu-north-1  eu-north-1c  host-aPtc6  cpu_utilization   
125992  125992  eu-north-1  eu-north-1c  host-7ZF9L  cpu_utilization   
125994  125994  eu-north-1  eu-north-1c  host-De8RB  cpu_utilization   
125997  125997  eu-north-1  eu-north-1c  host-2z8tn  cpu_utilization   
125999  125999  eu-north-1  eu-north-1c  host-9FczW  cpu_utilization   

        cpu_utilization                       time  
0             21.394363 2020-12-08 16:18:47.599597  
2             17.144579 2020-12-08 16:18:47.599597  
4             26.584865 2020-12-08 16:18:47.599597  
6             52.930970 2020-12-08 16:18:47.599597  
8             99.134110 2020-12-08 16:18:47.599597  
...                 ...                        ...  
125990        89.566125 2020-12-08 16:18:47.599597  
125992        75.510598 2020-12-08 16:18:47.599597  
125994         2.771261 2020-12-08 16:18:47.599597  
125997        29.212219 2020-12-08 16:18:47.599597  
125999         1.677793 2020-12-08 16:18:47.599597  

[63000 rows x 7 columns]
```

```python
rejected_records = wr.timestream.write(
    df=df_cpu,
    database="sampleDB",
    table="sampleTable",
    time_col="time",
    measure_col="cpu_utilization",
    dimensions_cols=["index", "region", "az", "hostname"],
)

assert len(rejected_records) == 0
```

## Batch Load MEMORY_UTILIZATION records

```python
df_memory = df[df.measure_kind == "memory_utilization"].copy()
df_memory.rename(columns={"measure": "memory_utilization"}, inplace=True)

df_memory
```

```text
         index      region           az    hostname        measure_kind  \
1            1   us-east-1   us-east-1a  host-fj2hx  memory_utilization   
3            3   us-east-1   us-east-1a  host-6kMPE  memory_utilization   
5            5   us-east-1   us-east-1a  host-sxj7X  memory_utilization   
7            7   us-east-1   us-east-1a  host-ExOui  memory_utilization   
9            9   us-east-1   us-east-1a  host-Bwb3j  memory_utilization   
...        ...         ...          ...         ...                 ...   
125991  125991  eu-north-1  eu-north-1c  host-aPtc6  memory_utilization   
125993  125993  eu-north-1  eu-north-1c  host-7ZF9L  memory_utilization   
125995  125995  eu-north-1  eu-north-1c  host-De8RB  memory_utilization   
125996  125996  eu-north-1  eu-north-1c  host-2z8tn  memory_utilization   
125998  125998  eu-north-1  eu-north-1c  host-9FczW  memory_utilization   

        memory_utilization                       time  
1                68.563420 2020-12-08 16:18:47.599597  
3                73.507870 2020-12-08 16:18:47.599597  
5                22.401424 2020-12-08 16:18:47.599597  
7                45.440135 2020-12-08 16:18:47.599597  
9                15.042701 2020-12-08 16:18:47.599597  
...                    ...                        ...  
125991           75.686739 2020-12-08 16:18:47.599597  
125993           18.386152 2020-12-08 16:18:47.599597  
125995           68.063468 2020-12-08 16:18:47.599597  
125996           72.203680 2020-12-08 16:18:47.599597  
125998           71.746134 2020-12-08 16:18:47.599597  

[63000 rows x 7 columns]
```

```python
response = wr.timestream.batch_load(
    df=df_memory,
    path="s3://bucket/prefix/",
    database="sampleDB",
    table="sampleTable",
    time_col="time",
    measure_cols=["memory_utilization"],
    dimensions_cols=["index", "region", "az", "hostname"],
    measure_name_col="measure_kind",
    report_s3_configuration={"BucketName": "error_bucket", "ObjectKeyPrefix": "error_prefix"},
)
assert response["BatchLoadTaskDescription"]["ProgressReport"]["RecordIngestionFailures"] == 0
```

## Querying CPU_UTILIZATION

```python
wr.timestream.query(
    """
    SELECT
        hostname, region, az, measure_name, measure_value::double, time
    FROM "sampleDB"."sampleTable"
    WHERE measure_name = 'cpu_utilization'
    ORDER BY time DESC
    LIMIT 10
"""
)
```

```text
     hostname      region           az     measure_name  \
0  host-OgvFx   us-west-1   us-west-1a  cpu_utilization   
1  host-rZUNx  eu-north-1  eu-north-1a  cpu_utilization   
2  host-t1kAB   us-east-2   us-east-2b  cpu_utilization   
3  host-RdQRf   us-east-1   us-east-1c  cpu_utilization   
4  host-4Llhu   us-east-1   us-east-1c  cpu_utilization   
5  host-2plqa   us-west-1   us-west-1a  cpu_utilization   
6  host-J3Q4z   us-east-1   us-east-1b  cpu_utilization   
7  host-VIR5T   ap-east-1   ap-east-1a  cpu_utilization   
8  host-G042D   us-east-1   us-east-1c  cpu_utilization   
9  host-8EBHm   us-west-2   us-west-2c  cpu_utilization   

   measure_value::double                    time  
0              39.617911 2020-12-08 19:18:47.600  
1              30.793332 2020-12-08 19:18:47.600  
2              74.453239 2020-12-08 19:18:47.600  
3              76.984448 2020-12-08 19:18:47.600  
4              41.862733 2020-12-08 19:18:47.600  
5              34.864762 2020-12-08 19:18:47.600  
6              71.574266 2020-12-08 19:18:47.600  
7              14.017491 2020-12-08 19:18:47.600  
8              60.199068 2020-12-08 19:18:47.600  
9              96.631624 2020-12-08 19:18:47.600
```

## Querying MEMORY_UTILIZATION

```python
wr.timestream.query(
    """
    SELECT
        hostname, region, az, measure_name, measure_value::double, time
    FROM "sampleDB"."sampleTable"
    WHERE measure_name = 'memory_utilization'
    ORDER BY time DESC
    LIMIT 10
"""
)
```

```text
     hostname      region           az        measure_name  \
0  host-7c897   us-west-2   us-west-2b  memory_utilization   
1  host-2z8tn  eu-north-1  eu-north-1c  memory_utilization   
2  host-J3Q4z   us-east-1   us-east-1b  memory_utilization   
3  host-mjrQb   us-east-1   us-east-1b  memory_utilization   
4  host-AyWSI   us-east-1   us-east-1c  memory_utilization   
5  host-Axf0g   us-west-2   us-west-2a  memory_utilization   
6  host-ilMBa   us-east-2   us-east-2b  memory_utilization   
7  host-CWdXX   us-west-2   us-west-2c  memory_utilization   
8  host-8EBHm   us-west-2   us-west-2c  memory_utilization   
9  host-dRIJj   us-east-1   us-east-1c  memory_utilization   

   measure_value::double                    time  
0              63.427726 2020-12-08 19:18:47.600  
1              41.071368 2020-12-08 19:18:47.600  
2              23.944388 2020-12-08 19:18:47.600  
3              69.173431 2020-12-08 19:18:47.600  
4              75.591467 2020-12-08 19:18:47.600  
5              29.720739 2020-12-08 19:18:47.600  
6              71.544134 2020-12-08 19:18:47.600  
7              79.792799 2020-12-08 19:18:47.600  
8              66.082554 2020-12-08 19:18:47.600  
9              86.748960 2020-12-08 19:18:47.600
```

## Deleting resources

```python
wr.timestream.delete_table("sampleDB", "sampleTable")
wr.timestream.delete_database("sampleDB")
```
