---
id: 029-s3-select
title: "S3 Select"
sidebar_position: 29
sidebar_label: "29 - S3 Select"
custom_edit_url: https://github.com/aws/aws-sdk-pandas/blob/main/tutorials/029%20-%20S3%20Select.ipynb
---

# S3 Select

> This page is generated from [`tutorials/029 - S3 Select.ipynb`](https://github.com/aws/aws-sdk-pandas/blob/main/tutorials/029%20-%20S3%20Select.ipynb). Open it in Jupyter to run it yourself.
AWS SDK for pandas supports [Amazon S3 Select](https://aws.amazon.com/blogs/aws/s3-glacier-select/), enabling applications to use SQL statements in order to query and filter the contents of a single S3 object. It works on objects stored in CSV, JSON or Apache Parquet, including compressed and large files of several TBs.

With S3 Select, the query workload is delegated to Amazon S3, leading to lower latency and cost, and to higher performance (up to 400% improvement). This is in comparison with other awswrangler operations such as `read_parquet` where the S3 object is downloaded and filtered on the client-side.

This feature has a number of limitations however:

- The maximum length of a record in the input or result is 1 MB
- The maximum uncompressed row group size is 256 MB (Parquet only)
- It can only emit nested data in JSON format
- Certain SQL operations are not supported (e.g. ORDER BY)

## Read multiple Parquet files from an S3 prefix

```python
import awswrangler as wr

df = wr.s3.select_query(
    sql='SELECT * FROM s3object s where s."trip_distance" > 30',
    path="s3://ursa-labs-taxi-data/2019/01/",
    input_serialization="Parquet",
    input_serialization_params={},
)

df.head()
```

```text
  vendor_id                 pickup_at                dropoff_at  \
0         2  2019-01-01T00:48:10.000Z  2019-01-01T01:36:58.000Z   
1         2  2019-01-01T00:38:36.000Z  2019-01-01T01:21:33.000Z   
2         2  2019-01-01T00:10:43.000Z  2019-01-01T01:23:59.000Z   
3         1  2019-01-01T00:13:17.000Z  2019-01-01T01:06:13.000Z   
4         2  2019-01-01T00:29:11.000Z  2019-01-01T01:29:05.000Z   

   passenger_count  trip_distance rate_code_id store_and_fwd_flag  \
0                1      31.570000            1                  N   
1                2      33.189999            5                  N   
2                1      33.060001            1                  N   
3                1      44.099998            5                  N   
4                2      31.100000            1                  N   

   pickup_location_id  dropoff_location_id payment_type  fare_amount  extra  \
0                 138                  138            2         82.5    0.5   
1                 107                  265            1        121.0    0.0   
2                 243                   42            2         92.0    0.5   
3                 132                  265            2        150.0    0.0   
4                 169                  201            1         85.5    0.5   

   mta_tax  tip_amount  tolls_amount  improvement_surcharge  total_amount  \
0      0.5        0.00          0.00                    0.3     83.800003   
1      0.0        0.08         10.50                    0.3    131.880005   
2      0.5        0.00          5.76                    0.3     99.059998   
3      0.0        0.00          0.00                    0.3    150.300003   
4      0.5        0.00          7.92                    0.3     94.720001   

   congestion_surcharge  
0                   NaN  
1                   NaN  
2                   NaN  
3                   NaN  
4                   NaN
```

## Read full CSV file

```python
df = wr.s3.select_query(
    sql="SELECT * FROM s3object",
    path="s3://humor-detection-pds/Humorous.csv",
    input_serialization="CSV",
    input_serialization_params={
        "FileHeaderInfo": "Use",
        "RecordDelimiter": "\r\n",
    },
    scan_range_chunk_size=1024 * 1024 * 32,  # override range of bytes to query, by default 1Mb
    use_threads=True,
)
df.head()
```

```text
                                            question  \
0         Will the volca sample get me a girlfriend?   
1   Can u communicate with spirits even on Saturday?   
2                          I won't get hunted right?   
3  I have a few questions.. Can you get possessed...   
4  Has anyone asked where the treasure is? What w...   

                 product_description  \
0    Korg Amplifier Part VOLCASAMPLE   
1  Winning Moves Games Classic Ouija   
2  Winning Moves Games Classic Ouija   
3  Winning Moves Games Classic Ouija   
4  Winning Moves Games Classic Ouija   

                                           image_url label  
0  http://ecx.images-amazon.com/images/I/81I1XZea...     1  
1  http://ecx.images-amazon.com/images/I/81kcYEG5...     1  
2  http://ecx.images-amazon.com/images/I/81kcYEG5...     1  
3  http://ecx.images-amazon.com/images/I/81kcYEG5...     1  
4  http://ecx.images-amazon.com/images/I/81kcYEG5...     1
```

## Filter JSON file

```python
wr.s3.select_query(
    sql="SELECT * FROM s3object[*] s where s.\"family_name\" = 'Biden'",
    path="s3://awsglue-datasets/examples/us-legislators/all/persons.json",
    input_serialization="JSON",
    input_serialization_params={
        "Type": "Document",
    },
)
```

```text
  family_name                             contact_details               name  \
0       Biden  [{'type': 'twitter', 'value': 'joebiden'}]  Joseph Biden, Jr.   

                                               links gender  \
0  [{'note': 'Wikipedia (ace)', 'url': 'https://a...   male   

                                               image  \
0  https://theunitedstates.io/images/congress/ori...   

                                         identifiers  \
0  [{'identifier': 'B000444', 'scheme': 'bioguide...   

                                         other_names      sort_name  \
0  [{'lang': None, 'name': 'Joe Biden', 'note': '...  Biden, Joseph   

                                              images given_name  birth_date  \
0  [{'url': 'https://theunitedstates.io/images/co...     Joseph  1942-11-20   

                                     id  
0  64239edf-8e06-4d2d-acc0-33d96bc79774
```
