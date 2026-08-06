---
id: 034-distributing-calls-using-ray
title: "Distributing Calls using Ray"
sidebar_position: 34
sidebar_label: "34 - Distributing Calls using Ray"
custom_edit_url: https://github.com/aws/aws-sdk-pandas/blob/main/tutorials/034%20-%20Distributing%20Calls%20using%20Ray.ipynb
---

# Distributing Calls using Ray

> This page is generated from [`tutorials/034 - Distributing Calls using Ray.ipynb`](https://github.com/aws/aws-sdk-pandas/blob/main/tutorials/034%20-%20Distributing%20Calls%20using%20Ray.ipynb). Open it in Jupyter to run it yourself.
AWS SDK for pandas supports distribution of specific calls using [ray](https://docs.ray.io/) and [modin](https://modin.readthedocs.io/en/stable/).

When enabled, data loading methods return modin dataframes instead of pandas dataframes. Modin provides seamless integration and compatibility with existing pandas code, with the benefit of distributing operations across your Ray instance and operating at a much larger scale.

```python
!pip install "awswrangler[modin,ray,redshift]"
```

Importing `awswrangler` when `ray` and `modin` are installed will automatically initialize a local Ray instance.

```python
import awswrangler as wr

print(f"Execution Engine: {wr.engine.get()}")
print(f"Memory Format: {wr.memory_format.get()}")
```

```text
Execution Engine: EngineEnum.RAY
Memory Format: MemoryFormatEnum.MODIN
```

#### Read data at scale

Data is read using all cores on a single machine or multiple nodes on a cluster

```python
df = wr.s3.read_parquet(path="s3://ursa-labs-taxi-data/2019/")
df.head(5)
```

```text
2023-09-15 12:24:44,457	INFO worker.py:1621 -- Started a local Ray instance.
2023-09-15 12:25:10,728	INFO read_api.py:374 -- To satisfy the requested parallelism of 200, each read task output will be split into 34 smaller blocks.
```

```text
[dataset]: Run `pip install tqdm` to enable progress reporting.
```

```text
UserWarning: When using a pre-initialized Ray cluster, please ensure that the runtime env sets environment variable __MODIN_AUTOIMPORT_PANDAS__ to 1
```

```text
  vendor_id           pickup_at          dropoff_at  passenger_count  \
0         1 2019-01-01 00:46:40 2019-01-01 00:53:20                1   
1         1 2019-01-01 00:59:47 2019-01-01 01:18:59                1   
2         2 2018-12-21 13:48:30 2018-12-21 13:52:40                3   
3         2 2018-11-28 15:52:25 2018-11-28 15:55:45                5   
4         2 2018-11-28 15:56:57 2018-11-28 15:58:33                5   

   trip_distance rate_code_id store_and_fwd_flag  pickup_location_id  \
0            1.5            1                  N                 151   
1            2.6            1                  N                 239   
2            0.0            1                  N                 236   
3            0.0            1                  N                 193   
4            0.0            2                  N                 193   

   dropoff_location_id payment_type  fare_amount  extra  mta_tax  tip_amount  \
0                  239            1          7.0    0.5      0.5        1.65   
1                  246            1         14.0    0.5      0.5        1.00   
2                  236            1          4.5    0.5      0.5        0.00   
3                  193            2          3.5    0.5      0.5        0.00   
4                  193            2         52.0    0.0      0.5        0.00   

   tolls_amount  improvement_surcharge  total_amount  congestion_surcharge  
0           0.0                    0.3      9.950000                   NaN  
1           0.0                    0.3     16.299999                   NaN  
2           0.0                    0.3      5.800000                   NaN  
3           0.0                    0.3      7.550000                   NaN  
4           0.0                    0.3     55.549999                   NaN
```

The data type is a modin DataFrame

```python
type(df)
```

```text
modin.pandas.dataframe.DataFrame
```

However, this type is interoperable with standard pandas calls:

```python
filtered_df = df[df.trip_distance > 30]
excluded_columns = ["vendor_id", "passenger_count", "store_and_fwd_flag"]
filtered_df = filtered_df.loc[:, ~filtered_df.columns.isin(excluded_columns)]
```

Enter your bucket name:

```python
bucket = "BUCKET"
```

#### Write data at scale

The write operation is parallelized, leading to significant speed-ups

```python
result = wr.s3.to_parquet(
    filtered_df,
    path=f"s3://{bucket}/taxi/",
    dataset=True,
)
print(f"Data has been written to {len(result['paths'])} files")
```

```text
Data has been written to 408 files
```

```text
2023-09-15 12:32:28,917	WARNING plan.py:567 -- Warning: The Ray cluster currently does not have any available CPUs. The Dataset job will hang unless more CPUs are freed up. A common reason is that cluster resources are used by Actors or Tune trials; see the following link for more details: https://docs.ray.io/en/master/data/dataset-internals.html#datasets-and-tune
2023-09-15 12:32:31,094	INFO streaming_executor.py:92 -- Executing DAG InputDataBuffer[Input] -> TaskPoolMapOperator[Write]
2023-09-15 12:32:31,095	INFO streaming_executor.py:93 -- Execution config: ExecutionOptions(resource_limits=ExecutionResources(cpu=None, gpu=None, object_store_memory=None), locality_with_output=False, preserve_order=False, actor_locality_enabled=True, verbose_progress=False)
2023-09-15 12:32:31,096	INFO streaming_executor.py:95 -- Tip: For detailed progress reporting, run `ray.data.DataContext.get_current().execution_options.verbose_progress = True`
```

```text
Data has been written to 408 files
```

#### Copy to Redshift at scale...

Data is first staged in S3 then a [COPY](https://docs.aws.amazon.com/redshift/latest/dg/r_COPY.html) command is executed against the Redshift cluster to load it. Both operations are distributed: S3 write with Ray and COPY in the Redshift cluster

```python
# Connect to the Redshift instance
con = wr.redshift.connect("aws-sdk-pandas-redshift")

path = f"s3://{bucket}/stage/"
iam_role = "ROLE"
schema = "public"
table = "taxi"

wr.redshift.copy(
    df=filtered_df,
    path=path,
    con=con,
    schema=schema,
    table=table,
    mode="overwrite",
    iam_role=iam_role,
    max_rows_by_file=None,
)
```

```text
2023-09-15 12:52:24,155	INFO streaming_executor.py:92 -- Executing DAG InputDataBuffer[Input] -> TaskPoolMapOperator[Write]
2023-09-15 12:52:24,157	INFO streaming_executor.py:93 -- Execution config: ExecutionOptions(resource_limits=ExecutionResources(cpu=None, gpu=None, object_store_memory=None), locality_with_output=False, preserve_order=False, actor_locality_enabled=True, verbose_progress=False)
2023-09-15 12:52:24,157	INFO streaming_executor.py:95 -- Tip: For detailed progress reporting, run `ray.data.DataContext.get_current().execution_options.verbose_progress = True`
```

#### ... and UNLOAD it back

Parallel calls can also be leveraged when reading from the cluster. The [UNLOAD](https://docs.aws.amazon.com/redshift/latest/dg/r_UNLOAD.html) command distributes query processing in Redshift to dump files in S3 which are then read in parallel into a dataframe

```python
df = wr.redshift.unload(
    sql=f"SELECT * FROM {schema}.{table} where trip_distance > 30",
    con=con,
    iam_role=iam_role,
    path=path,
    keep_files=True,
)

df.head()
```

```text
2023-09-15 12:56:53,838	INFO read_api.py:374 -- To satisfy the requested parallelism of 16, each read task output will be split into 8 smaller blocks.
```

```text
            pickup_at          dropoff_at  trip_distance rate_code_id  \
0 2019-01-22 17:40:04 2019-01-22 18:33:48      30.469999            4   
1 2019-01-22 18:36:34 2019-01-22 19:52:50      33.330002            5   
2 2019-01-22 19:11:08 2019-01-22 20:16:10      32.599998            1   
3 2019-01-22 19:14:15 2019-01-22 20:09:57      36.220001            4   
4 2019-01-22 19:51:56 2019-01-22 20:48:39      33.040001            5   

   pickup_location_id  dropoff_location_id payment_type  fare_amount  extra  \
0                 132                  265            1   142.000000    1.0   
1                  51                  221            1    96.019997    0.0   
2                 231                  205            1    88.000000    1.0   
3                 132                  265            1   130.500000    1.0   
4                 132                  265            1   130.000000    0.0   

   mta_tax  tip_amount  tolls_amount  improvement_surcharge  total_amount  \
0      0.5   28.760000          0.00                    0.3    172.559998   
1      0.5    0.000000         11.52                    0.3    108.339996   
2      0.5    0.000000          0.00                    0.3     89.800003   
3      0.5   27.610001          5.76                    0.3    165.669998   
4      0.5   29.410000         16.26                    0.3    176.470001   

   congestion_surcharge  
0                   0.0  
1                   0.0  
2                   0.0  
3                   0.0  
4                   0.0
```

#### Find a needle in a hay stack with S3 Select

```python
import awswrangler as wr

# Run S3 Select query against all objects for 2019 year to find trips starting from a particular location
wr.s3.select_query(
    sql='SELECT * FROM s3object s where s."pickup_location_id" = 138',
    path="s3://ursa-labs-taxi-data/2019/",
    input_serialization="Parquet",
    input_serialization_params={},
    scan_range_chunk_size=32 * 1024 * 1024,
)
```

```text
        vendor_id                 pickup_at                dropoff_at  \
0               1  2019-01-01T00:19:55.000Z  2019-01-01T00:57:56.000Z   
1               2  2019-01-01T00:48:10.000Z  2019-01-01T01:36:58.000Z   
2               1  2019-01-01T00:39:58.000Z  2019-01-01T00:58:58.000Z   
3               1  2019-01-01T00:07:45.000Z  2019-01-01T00:34:12.000Z   
4               2  2019-01-01T00:27:40.000Z  2019-01-01T00:52:15.000Z   
...           ...                       ...                       ...   
1167508         2  2019-06-30T23:42:24.000Z  2019-07-01T00:10:28.000Z   
1167509         2  2019-06-30T23:07:34.000Z  2019-06-30T23:25:09.000Z   
1167510         2  2019-06-30T23:00:36.000Z  2019-06-30T23:20:18.000Z   
1167511         1  2019-06-30T23:08:06.000Z  2019-06-30T23:30:20.000Z   
1167512         2  2019-06-30T23:15:13.000Z  2019-06-30T23:35:18.000Z   

         passenger_count  trip_distance rate_code_id store_and_fwd_flag  \
0                      1          12.30            1                  N   
1                      1          31.57            1                  N   
2                      2           8.90            1                  N   
3                      4           9.60            1                  N   
4                      1          12.89            1                  N   
...                  ...            ...          ...                ...   
1167508                1          15.66            1                  N   
1167509                1           7.38            1                  N   
1167510                1          11.24            1                  N   
1167511                1           7.50            1                  N   
1167512                2           8.73            1                  N   

         pickup_location_id  dropoff_location_id payment_type  fare_amount  \
0                       138                   50            1         38.0   
1                       138                  138            2         82.5   
2                       138                  224            1         26.0   
3                       138                  239            1         29.0   
4                       138                   87            2         36.0   
...                     ...                  ...          ...          ...   
1167508                 138                  265            2         44.0   
1167509                 138                  262            1         22.0   
1167510                 138                  107            1         31.0   
1167511                 138                  229            1         24.0   
1167512                 138                  262            1         25.5   

         extra  mta_tax  tip_amount  tolls_amount  improvement_surcharge  \
… (output truncated, 26 more lines)
```
