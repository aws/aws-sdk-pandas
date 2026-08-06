---
id: 003-amazon-s3
title: "Amazon S3"
sidebar_position: 3
sidebar_label: "3 - Amazon S3"
custom_edit_url: https://github.com/aws/aws-sdk-pandas/blob/main/tutorials/003%20-%20Amazon%20S3.ipynb
---

# Amazon S3

> This page is generated from [`tutorials/003 - Amazon S3.ipynb`](https://github.com/aws/aws-sdk-pandas/blob/main/tutorials/003%20-%20Amazon%20S3.ipynb). Open it in Jupyter to run it yourself.
## Table of Contents
* [1. CSV files](#1.-CSV-files)
	* [1.1 Writing CSV files](#1.1-Writing-CSV-files)
	* [1.2 Reading single CSV file](#1.2-Reading-single-CSV-file)
	* [1.3 Reading multiple CSV files](#1.3-Reading-multiple-CSV-files)
		* [1.3.1 Reading CSV by list](#1.3.1-Reading-CSV-by-list)
		* [1.3.2 Reading CSV by prefix](#1.3.2-Reading-CSV-by-prefix)
* [2. JSON files](#2.-JSON-files)
	* [2.1 Writing JSON files](#2.1-Writing-JSON-files)
	* [2.2 Reading single JSON file](#2.2-Reading-single-JSON-file)
	* [2.3 Reading multiple JSON files](#2.3-Reading-multiple-JSON-files)
		* [2.3.1 Reading JSON by list](#2.3.1-Reading-JSON-by-list)
		* [2.3.2 Reading JSON by prefix](#2.3.2-Reading-JSON-by-prefix)
* [3. Parquet files](#3.-Parquet-files)
	* [3.1 Writing Parquet files](#3.1-Writing-Parquet-files)
	* [3.2 Reading single Parquet file](#3.2-Reading-single-Parquet-file)
	* [3.3 Reading multiple Parquet files](#3.3-Reading-multiple-Parquet-files)
		* [3.3.1 Reading Parquet by list](#3.3.1-Reading-Parquet-by-list)
		* [3.3.2 Reading Parquet by prefix](#3.3.2-Reading-Parquet-by-prefix)
* [4. Fixed-width formatted files (only read)](#4.-Fixed-width-formatted-files-(only-read))
	* [4.1 Reading single FWF file](#4.1-Reading-single-FWF-file)
	* [4.2 Reading multiple FWF files](#4.2-Reading-multiple-FWF-files)
		* [4.2.1 Reading FWF by list](#4.2.1-Reading-FWF-by-list)
		* [4.2.2 Reading FWF by prefix](#4.2.2-Reading-FWF-by-prefix)
* [5. Excel files](#5.-Excel-files)
	* [5.1 Writing Excel file](#5.1-Writing-Excel-file)
	* [5.2 Reading Excel file](#5.2-Reading-Excel-file)
* [6. Reading with lastModified filter](#6.-Reading-with-lastModified-filter)
	* [6.1 Define the Date time with UTC Timezone](#6.1-Define-the-Date-time-with-UTC-Timezone)
	* [6.2 Define the Date time and specify the Timezone](#6.2-Define-the-Date-time-and-specify-the-Timezone)
	* [6.3 Read json using the LastModified filters](#6.3-Read-json-using-the-LastModified-filters)
* [7. Download Objects](#7.-Download-objects)
    * [7.1 Download object to a file path](#7.1-Download-object-to-a-file-path)
    * [7.2 Download object to a file-like object in binary mode](#7.2-Download-object-to-a-file-like-object-in-binary-mode)
* [8. Upload Objects](#8.-Upload-objects)
    * [8.1 Upload object from a file path](#8.1-Upload-object-from-a-file-path)
    * [8.2 Upload object from a file-like object in binary mode](#8.2-Upload-object-from-a-file-like-object-in-binary-mode)
* [9. Delete objects](#9.-Delete-objects)

```python
from datetime import datetime

import boto3
import pandas as pd
import pytz

import awswrangler as wr

df1 = pd.DataFrame({"id": [1, 2], "name": ["foo", "boo"]})

df2 = pd.DataFrame({"id": [3], "name": ["bar"]})
```

## Enter your bucket name:

```python
import getpass

bucket = getpass.getpass()
```

## 1. CSV files

### 1.1 Writing CSV files

```python
path1 = f"s3://{bucket}/csv/file1.csv"
path2 = f"s3://{bucket}/csv/file2.csv"

wr.s3.to_csv(df1, path1, index=False)
wr.s3.to_csv(df2, path2, index=False)
```

### 1.2 Reading single CSV file

```python
wr.s3.read_csv([path1])
```

```text
   id name
0   1  foo
1   2  boo
```

### 1.3 Reading multiple CSV files

#### 1.3.1 Reading CSV by list

```python
wr.s3.read_csv([path1, path2])
```

```text
   id name
0   1  foo
1   2  boo
2   3  bar
```

#### 1.3.2 Reading CSV by prefix

```python
wr.s3.read_csv(f"s3://{bucket}/csv/")
```

```text
   id name
0   1  foo
1   2  boo
2   3  bar
```

## 2. JSON files

### 2.1 Writing JSON files

```python
path1 = f"s3://{bucket}/json/file1.json"
path2 = f"s3://{bucket}/json/file2.json"

wr.s3.to_json(df1, path1)
wr.s3.to_json(df2, path2)
```

```text
['s3://woodadw-test/json/file2.json']
```

### 2.2 Reading single JSON file

```python
wr.s3.read_json([path1])
```

```text
   id name
0   1  foo
1   2  boo
```

### 2.3 Reading multiple JSON files

#### 2.3.1 Reading JSON by list

```python
wr.s3.read_json([path1, path2])
```

```text
   id name
0   1  foo
1   2  boo
0   3  bar
```

#### 2.3.2 Reading JSON by prefix

```python
wr.s3.read_json(f"s3://{bucket}/json/")
```

```text
   id name
0   1  foo
1   2  boo
0   3  bar
```

## 3. Parquet files

For more complex features releated to Parquet Dataset check the tutorial number 4.

### 3.1 Writing Parquet files

```python
path1 = f"s3://{bucket}/parquet/file1.parquet"
path2 = f"s3://{bucket}/parquet/file2.parquet"

wr.s3.to_parquet(df1, path1)
wr.s3.to_parquet(df2, path2)
```

### 3.2 Reading single Parquet file

```python
wr.s3.read_parquet([path1])
```

```text
   id name
0   1  foo
1   2  boo
```

### 3.3 Reading multiple Parquet files

#### 3.3.1 Reading Parquet by list

```python
wr.s3.read_parquet([path1, path2])
```

```text
   id name
0   1  foo
1   2  boo
2   3  bar
```

#### 3.3.2 Reading Parquet by prefix

```python
wr.s3.read_parquet(f"s3://{bucket}/parquet/")
```

```text
   id name
0   1  foo
1   2  boo
2   3  bar
```

## 4. Fixed-width formatted files (only read)

As of today, Pandas doesn't implement a `to_fwf` functionality, so let's manually write two files:

```python
content = "1  Herfelingen 27-12-18\n2    Lambusart 14-06-18\n3 Spormaggiore 15-04-18"
boto3.client("s3").put_object(Body=content, Bucket=bucket, Key="fwf/file1.txt")

content = "4    Buizingen 05-09-19\n5   San Rafael 04-09-19"
boto3.client("s3").put_object(Body=content, Bucket=bucket, Key="fwf/file2.txt")

path1 = f"s3://{bucket}/fwf/file1.txt"
path2 = f"s3://{bucket}/fwf/file2.txt"
```

### 4.1 Reading single FWF file

```python
wr.s3.read_fwf([path1], names=["id", "name", "date"])
```

```text
   id          name      date
0   1   Herfelingen  27-12-18
1   2     Lambusart  14-06-18
2   3  Spormaggiore  15-04-18
```

### 4.2 Reading multiple FWF files

#### 4.2.1 Reading FWF by list

```python
wr.s3.read_fwf([path1, path2], names=["id", "name", "date"])
```

```text
   id          name      date
0   1   Herfelingen  27-12-18
1   2     Lambusart  14-06-18
2   3  Spormaggiore  15-04-18
3   4     Buizingen  05-09-19
4   5    San Rafael  04-09-19
```

#### 4.2.2 Reading FWF by prefix

```python
wr.s3.read_fwf(f"s3://{bucket}/fwf/", names=["id", "name", "date"])
```

```text
   id          name      date
0   1   Herfelingen  27-12-18
1   2     Lambusart  14-06-18
2   3  Spormaggiore  15-04-18
3   4     Buizingen  05-09-19
4   5    San Rafael  04-09-19
```

## 5. Excel files

### 5.1 Writing Excel file

```python
path = f"s3://{bucket}/file0.xlsx"

wr.s3.to_excel(df1, path, index=False)
```

```text
's3://woodadw-test/file0.xlsx'
```

### 5.2 Reading Excel file

```python
wr.s3.read_excel(path)
```

```text
   id name
0   1  foo
1   2  boo
```

## 6. Reading with lastModified filter

Specify the filter by LastModified Date.

The filter needs to be specified as datime with time zone

Internally the path needs to be listed, after that the filter is applied.

The filter compare the s3 content with the variables lastModified_begin and lastModified_end

https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html

### 6.1 Define the Date time with UTC Timezone

```python
begin = datetime.strptime("20-07-31 20:30", "%y-%m-%d %H:%M")
end = datetime.strptime("21-07-31 20:30", "%y-%m-%d %H:%M")

begin_utc = pytz.utc.localize(begin)
end_utc = pytz.utc.localize(end)
```

### 6.2 Define the Date time and specify the Timezone

```python
begin = datetime.strptime("20-07-31 20:30", "%y-%m-%d %H:%M")
end = datetime.strptime("21-07-31 20:30", "%y-%m-%d %H:%M")

timezone = pytz.timezone("America/Los_Angeles")

begin_Los_Angeles = timezone.localize(begin)
end_Los_Angeles = timezone.localize(end)
```

### 6.3 Read json using the LastModified filters

```python
wr.s3.read_fwf(
    f"s3://{bucket}/fwf/", names=["id", "name", "date"], last_modified_begin=begin_utc, last_modified_end=end_utc
)
wr.s3.read_json(f"s3://{bucket}/json/", last_modified_begin=begin_utc, last_modified_end=end_utc)
wr.s3.read_csv(f"s3://{bucket}/csv/", last_modified_begin=begin_utc, last_modified_end=end_utc)
wr.s3.read_parquet(f"s3://{bucket}/parquet/", last_modified_begin=begin_utc, last_modified_end=end_utc)
```

## 7. Download objects

Objects can be downloaded from S3 using either a path to a local file or a file-like object in binary mode.

### 7.1 Download object to a file path

```python
local_file_dir = getpass.getpass()
```

```python
import os

path1 = f"s3://{bucket}/csv/file1.csv"
local_file = os.path.join(local_file_dir, "file1.csv")
wr.s3.download(path=path1, local_file=local_file)

pd.read_csv(local_file)
```

```text
   id name
0   1  foo
1   2  boo
```

### 7.2 Download object to a file-like object in binary mode

```python
path2 = f"s3://{bucket}/csv/file2.csv"
local_file = os.path.join(local_file_dir, "file2.csv")
with open(local_file, mode="wb") as local_f:
    wr.s3.download(path=path2, local_file=local_f)

pd.read_csv(local_file)
```

```text
   id name
0   3  bar
```

## 8. Upload objects

Objects can be uploaded to S3 using either a path to a local file or a file-like object in binary mode.

### 8.1 Upload object from a file path

```python
local_file = os.path.join(local_file_dir, "file1.csv")
wr.s3.upload(local_file=local_file, path=path1)

wr.s3.read_csv(path1)
```

```text
   id name
0   1  foo
1   2  boo
```

### 8.2 Upload object from a file-like object in binary mode

```python
local_file = os.path.join(local_file_dir, "file2.csv")
with open(local_file, "rb") as local_f:
    wr.s3.upload(local_file=local_f, path=path2)

wr.s3.read_csv(path2)
```

```text
   id name
0   3  bar
```

## 9. Delete objects

```python
wr.s3.delete_objects(f"s3://{bucket}/")
```
