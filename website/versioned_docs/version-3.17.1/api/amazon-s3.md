---
id: amazon-s3
title: "Amazon S3"
sidebar_position: 1
---

# Amazon S3

Module: `wr.s3`

### copy_objects

```python
wr.s3.copy_objects(
    paths: 'list[str]',
    source_path: 'str',
    target_path: 'str',
    replace_filenames: 'dict[str, str] | None' = None,
    use_threads: 'bool | int' = True,
    boto3_session: 'boto3.Session | None' = None,
    s3_additional_kwargs: 'dict[str, Any] | None' = None
) -> 'list[str]'
```

Copy a list of S3 objects to another S3 directory.

:::note
In case of `use_threads=True` the number of threads
that will be spawned will be gotten from `os.cpu_count()`.
:::

:::note
Following arguments are not supported in distributed mode with engine `EngineEnum.RAY`:

- boto3_session
:::

**Parameters**

- **`paths`** — List of S3 objects paths (e.g. `["s3://bucket/dir0/key0", "s3://bucket/dir0/key1"]`).
- **`source_path`** — S3 Path for the source directory.
- **`target_path`** — S3 Path for the target directory.
- **`replace_filenames`** — e.g. `{"old_name.csv": "new_name.csv", "old_name2.csv": "new_name2.csv"}`
- **`use_threads`** — True to enable concurrent requests, False to disable multiple threads. If enabled `os.cpu_count()` will be used as the max number of threads. If integer is provided, specified number is used.
- **`boto3_session`** — Boto3 Session. The default boto3 session will be used if boto3_session receive None.
- **`s3_additional_kwargs`** — Forwarded to botocore requests. e.g. `s3_additional_kwargs={'ServerSideEncryption': 'aws:kms', 'SSEKMSKeyId': 'YOUR_KMS_KEY_ARN'}`

**Returns**

- List of new objects paths.

**Examples**

Copying

```python
>>> import awswrangler as wr
>>> wr.s3.copy_objects(
...     paths=["s3://bucket0/dir0/key0", "s3://bucket0/dir0/key1"],
...     source_path="s3://bucket0/dir0/",
...     target_path="s3://bucket1/dir1/"
... )
["s3://bucket1/dir1/key0", "s3://bucket1/dir1/key1"]
```

Copying with a KMS key

```python
>>> import awswrangler as wr
>>> wr.s3.copy_objects(
...     paths=["s3://bucket0/dir0/key0", "s3://bucket0/dir0/key1"],
...     source_path="s3://bucket0/dir0/",
...     target_path="s3://bucket1/dir1/",
...     s3_additional_kwargs={
...         'ServerSideEncryption': 'aws:kms',
...         'SSEKMSKeyId': 'YOUR_KMS_KEY_ARN'
...     }
... )
["s3://bucket1/dir1/key0", "s3://bucket1/dir1/key1"]
```

---

### delete_objects

```python
wr.s3.delete_objects(
    path: 'str | list[str]',
    use_threads: 'bool | int' = True,
    last_modified_begin: 'datetime.datetime | None' = None,
    last_modified_end: 'datetime.datetime | None' = None,
    s3_additional_kwargs: 'dict[str, Any] | None' = None,
    boto3_session: 'boto3.Session | None' = None
) -> 'None'
```

Delete Amazon S3 objects from a received S3 prefix or list of S3 objects paths.

This function accepts Unix shell-style wildcards in the path argument.
* (matches everything), ? (matches any single character),
[seq] (matches any character in seq), [!seq] (matches any character not in seq).
If you want to use a path which includes Unix shell-style wildcard characters (`*, ?, []`),
you can use `glob.escape(path)` before passing the path to this function.

:::note
In case of `use_threads=True` the number of threads
that will be spawned will be gotten from os.cpu_count().
:::
:::note
The filter by last_modified begin last_modified end is applied after list all S3 files
:::

:::note
Following arguments are not supported in distributed mode with engine `EngineEnum.RAY`:

- boto3_session
:::

**Parameters**

- **`path`** — S3 prefix (accepts Unix shell-style wildcards) (e.g. s3://bucket/prefix) or list of S3 objects paths (e.g. [s3://bucket/key0, s3://bucket/key1]).
- **`use_threads`** — True to enable concurrent requests, False to disable multiple threads. If enabled os.cpu_count() will be used as the max number of threads. If integer is provided, specified number is used.
- **`last_modified_begin`** — Filter the s3 files by the Last modified date of the object. The filter is applied only after list all s3 files.
- **`last_modified_end`** — Filter the s3 files by the Last modified date of the object. The filter is applied only after list all s3 files.
- **`s3_additional_kwargs`** — Forwarded to botocore requests. e.g. s3_additional_kwargs={'RequestPayer': 'requester'}
- **`boto3_session`** — Boto3 Session. The default boto3 session will be used if boto3_session receive None.

**Returns**

- None

**Examples**

```python
>>> import awswrangler as wr
>>> wr.s3.delete_objects(['s3://bucket/key0', 's3://bucket/key1'])  # Delete both objects
>>> wr.s3.delete_objects('s3://bucket/prefix')  # Delete all objects under the received prefix
```

---

### describe_objects

```python
wr.s3.describe_objects(
    path: 'str | list[str]',
    version_id: 'str | dict[str, str] | None' = None,
    use_threads: 'bool | int' = True,
    last_modified_begin: 'datetime.datetime | None' = None,
    last_modified_end: 'datetime.datetime | None' = None,
    s3_additional_kwargs: 'dict[str, Any] | None' = None,
    boto3_session: 'boto3.Session | None' = None
) -> 'dict[str, dict[str, Any]]'
```

Describe Amazon S3 objects from a received S3 prefix or list of S3 objects paths.

Fetch attributes like ContentLength, DeleteMarker, last_modified, ContentType, etc
The full list of attributes can be explored under the boto3 head_object documentation:
https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Client.head_object

This function accepts Unix shell-style wildcards in the path argument.
* (matches everything), ? (matches any single character),
[seq] (matches any character in seq), [!seq] (matches any character not in seq).
If you want to use a path which includes Unix shell-style wildcard characters (`*, ?, []`),
you can use `glob.escape(path)` before passing the path to this function.

:::note
In case of `use_threads=True` the number of threads
that will be spawned will be gotten from os.cpu_count().
:::
:::note
The filter by last_modified begin last_modified end is applied after list all S3 files
:::

:::note
Following arguments are not supported in distributed mode with engine `EngineEnum.RAY`:

- boto3_session

- s3_additional_kwargs
:::

**Parameters**

- **`path`** — S3 prefix (accepts Unix shell-style wildcards) (e.g. s3://bucket/prefix) or list of S3 objects paths (e.g. [s3://bucket/key0, s3://bucket/key1]).
- **`version_id`** — Version id of the object or mapping of object path to version id. (e.g. {'s3://bucket/key0': '121212', 's3://bucket/key1': '343434'})
- **`use_threads`** — True to enable concurrent requests, False to disable multiple threads. If enabled os.cpu_count() will be used as the max number of threads. If integer is provided, specified number is used.
- **`last_modified_begin`** — Filter the s3 files by the Last modified date of the object. The filter is applied only after list all s3 files.
- **`last_modified_end`** — Filter the s3 files by the Last modified date of the object. The filter is applied only after list all s3 files.
- **`s3_additional_kwargs`** — Forwarded to botocore requests. e.g. s3_additional_kwargs={'RequestPayer': 'requester'}
- **`boto3_session`** — Boto3 Session. The default boto3 session will be used if boto3_session receive None.

**Returns**

- Return a dictionary of objects returned from head_objects where the key is the object path. The response object can be explored here: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Client.head_object

**Examples**

```python
>>> import awswrangler as wr
>>> descs0 = wr.s3.describe_objects(['s3://bucket/key0', 's3://bucket/key1'])  # Describe both objects
>>> descs1 = wr.s3.describe_objects('s3://bucket/prefix')  # Describe all objects under the prefix
```

---

### does_object_exist

```python
wr.s3.does_object_exist(
    path: 'str',
    s3_additional_kwargs: 'dict[str, Any] | None' = None,
    boto3_session: 'boto3.Session | None' = None,
    version_id: 'str | None' = None
) -> 'bool'
```

Check if object exists on S3.

**Parameters**

- **`path`** — S3 path (e.g. s3://bucket/key).
- **`s3_additional_kwargs`** — Forwarded to botocore requests. e.g. s3_additional_kwargs={'RequestPayer': 'requester'}
- **`boto3_session`** — Boto3 Session. The default boto3 session will be used if boto3_session receive None.
- **`version_id`** — Specific version of the object that should exist.

**Returns**

- True if exists, False otherwise.

**Examples**

Using the default boto3 session

```python
>>> import awswrangler as wr
>>> wr.s3.does_object_exist('s3://bucket/key_real')
True
>>> wr.s3.does_object_exist('s3://bucket/key_unreal')
False
```

Using a custom boto3 session

```python
>>> import boto3
>>> import awswrangler as wr
>>> wr.s3.does_object_exist('s3://bucket/key_real', boto3_session=boto3.Session())
True
>>> wr.s3.does_object_exist('s3://bucket/key_unreal', boto3_session=boto3.Session())
False
```

---

### download

```python
wr.s3.download(
    path: 'str',
    local_file: 'str | Any',
    version_id: 'str | None' = None,
    use_threads: 'bool | int' = True,
    boto3_session: 'boto3.Session | None' = None,
    s3_additional_kwargs: 'dict[str, Any] | None' = None
) -> 'None'
```

Download file from a received S3 path to local file.

:::note
In case of `use_threads=True` the number of threads
that will be spawned will be gotten from os.cpu_count().
:::

**Parameters**

- **`path`** — S3 path (e.g. `s3://bucket/key0`).
- **`local_file`** — A file-like object in binary mode or a path to local file (e.g. `./local/path/to/key0`).
- **`version_id`** — Version id of the object.
- **`use_threads`** — True to enable concurrent requests, False to disable multiple threads. If enabled os.cpu_count() will be used as the max number of threads. If integer is provided, specified number is used.
- **`boto3_session`** — Boto3 Session. The default boto3 session will be used if boto3_session receive None.
- **`s3_additional_kwargs`** — Forward to botocore requests, only "SSECustomerAlgorithm", "SSECustomerKey" and "RequestPayer" arguments will be considered.

**Returns**

- None

**Examples**

Downloading a file using a path to local file

```python
>>> import awswrangler as wr
>>> wr.s3.download(path='s3://bucket/key', local_file='./key')
```

Downloading a file using a file-like object

```python
>>> import awswrangler as wr
>>> with open(file='./key', mode='wb') as local_f:
>>>     wr.s3.download(path='s3://bucket/key', local_file=local_f)
```

---

### get_bucket_region

```python
wr.s3.get_bucket_region(bucket: 'str', boto3_session: 'boto3.Session | None' = None) -> 'str'
```

Get bucket region name.

**Parameters**

- **`bucket`** — Bucket name.
- **`boto3_session`** — Boto3 Session. The default boto3 session will be used if boto3_session receive None.

**Returns**

- Region code (e.g. 'us-east-1').

**Examples**

Using the default boto3 session

```python
>>> import awswrangler as wr
>>> region = wr.s3.get_bucket_region('bucket-name')
```

Using a custom boto3 session

```python
>>> import boto3
>>> import awswrangler as wr
>>> region = wr.s3.get_bucket_region('bucket-name', boto3_session=boto3.Session())
```

---

### list_buckets

```python
wr.s3.list_buckets(boto3_session: 'boto3.Session | None' = None) -> 'list[str]'
```

List Amazon S3 buckets.

**Parameters**

- **`boto3_session`** — Boto3 Session. The default boto3 session to use, default to None.

**Returns**

- List of bucket names.

---

### list_directories

```python
wr.s3.list_directories(
    path: 'str',
    chunked: 'bool' = False,
    s3_additional_kwargs: 'dict[str, Any] | None' = None,
    boto3_session: 'boto3.Session | None' = None
) -> 'list[str] | Iterator[list[str]]'
```

List Amazon S3 objects from a prefix.

This function accepts Unix shell-style wildcards in the path argument.
* (matches everything), ? (matches any single character),
[seq] (matches any character in seq), [!seq] (matches any character not in seq).
If you want to use a path which includes Unix shell-style wildcard characters (`*, ?, []`),
you can use `glob.escape(path)` before passing the path to this function.


:::note
Following arguments are not supported in distributed mode with engine `EngineEnum.RAY`:

- boto3_session

- s3_additional_kwargs
:::

**Parameters**

- **`path`** — S3 path (e.g. s3://bucket/prefix).
- **`chunked`** — If True returns iterator, and a single list otherwise. False by default.
- **`s3_additional_kwargs`** — Forwarded to botocore requests. e.g. s3_additional_kwargs={'RequestPayer': 'requester'}
- **`boto3_session`** — Boto3 Session. The default boto3 session will be used if boto3_session receive None.

**Returns**

- List of objects paths.

**Examples**

Using the default boto3 session

```python
>>> import awswrangler as wr
>>> wr.s3.list_directories('s3://bucket/prefix/')
['s3://bucket/prefix/dir0/', 's3://bucket/prefix/dir1/', 's3://bucket/prefix/dir2/']
```

Using a custom boto3 session

```python
>>> import boto3
>>> import awswrangler as wr
>>> wr.s3.list_directories('s3://bucket/prefix/', boto3_session=boto3.Session())
['s3://bucket/prefix/dir0/', 's3://bucket/prefix/dir1/', 's3://bucket/prefix/dir2/']
```

---

### list_objects

```python
wr.s3.list_objects(
    path: 'str',
    suffix: 'str | list[str] | None' = None,
    ignore_suffix: 'str | list[str] | None' = None,
    last_modified_begin: 'datetime.datetime | None' = None,
    last_modified_end: 'datetime.datetime | None' = None,
    ignore_empty: 'bool' = False,
    chunked: 'bool' = False,
    s3_additional_kwargs: 'dict[str, Any] | None' = None,
    boto3_session: 'boto3.Session | None' = None
) -> 'list[str] | Iterator[list[str]]'
```

List Amazon S3 objects from a prefix.

This function accepts Unix shell-style wildcards in the path argument.
* (matches everything), ? (matches any single character),
[seq] (matches any character in seq), [!seq] (matches any character not in seq).
If you want to use a path which includes Unix shell-style wildcard characters (`*, ?, []`),
you can use `glob.escape(path)` before passing the path to this function.

:::note
The filter by last_modified begin last_modified end is applied after list all S3 files
:::

:::note
Following arguments are not supported in distributed mode with engine `EngineEnum.RAY`:

- boto3_session

- s3_additional_kwargs
:::

**Parameters**

- **`path`** — S3 path (e.g. s3://bucket/prefix).
- **`suffix`** — Suffix or List of suffixes for filtering S3 keys.
- **`ignore_suffix`** — Suffix or List of suffixes for S3 keys to be ignored.
- **`last_modified_begin`** — Filter the s3 files by the Last modified date of the object. The filter is applied only after list all s3 files.
- **`last_modified_end`** — Filter the s3 files by the Last modified date of the object. The filter is applied only after list all s3 files.
- **`ignore_empty`** — Ignore files with 0 bytes.
- **`chunked`** — If True returns iterator, and a single list otherwise. False by default.
- **`s3_additional_kwargs`** — Forwarded to botocore requests. e.g. s3_additional_kwargs={'RequestPayer': 'requester'}
- **`boto3_session`** — Boto3 Session. The default boto3 session will be used if boto3_session receive None.

**Returns**

- List of objects paths.

**Examples**

Using the default boto3 session

```python
>>> import awswrangler as wr
>>> wr.s3.list_objects('s3://bucket/prefix')
['s3://bucket/prefix0', 's3://bucket/prefix1', 's3://bucket/prefix2']
```

Using a custom boto3 session

```python
>>> import boto3
>>> import awswrangler as wr
>>> wr.s3.list_objects('s3://bucket/prefix', boto3_session=boto3.Session())
['s3://bucket/prefix0', 's3://bucket/prefix1', 's3://bucket/prefix2']
```

---

### merge_datasets

```python
wr.s3.merge_datasets(
    source_path: 'str',
    target_path: 'str',
    mode: "Literal['append', 'overwrite', 'overwrite_partitions']" = 'append',
    ignore_empty: 'bool' = False,
    use_threads: 'bool | int' = True,
    boto3_session: 'boto3.Session | None' = None,
    s3_additional_kwargs: 'dict[str, Any] | None' = None
) -> 'list[str]'
```

Merge a source dataset into a target dataset.

This function accepts Unix shell-style wildcards in the source_path argument.
* (matches everything), ? (matches any single character),
[seq] (matches any character in seq), [!seq] (matches any character not in seq).
If you want to use a path which includes Unix shell-style wildcard characters (`*, ?, []`),
you can use `glob.escape(source_path)` before passing the path to this function.

:::note
If you are merging tables (S3 datasets + Glue Catalog metadata),
remember that you will also need to update your partitions metadata in some cases.
(e.g. wr.athena.repair_table(table='...', database='...'))
:::
:::note
In case of `use_threads=True` the number of threads
that will be spawned will be gotten from os.cpu_count().
:::

:::note
Following arguments are not supported in distributed mode with engine `EngineEnum.RAY`:

- boto3_session
:::

**Parameters**

- **`source_path`** — S3 Path for the source directory.
- **`target_path`** — S3 Path for the target directory.
- **`mode`** — `append` (Default), `overwrite`, `overwrite_partitions`.
- **`ignore_empty`** — Ignore files with 0 bytes.
- **`use_threads`** — True to enable concurrent requests, False to disable multiple threads. If enabled os.cpu_count() will be used as the max number of threads. If integer is provided, specified number is used.
- **`boto3_session`** — Boto3 Session. The default boto3 session will be used if boto3_session receive None.
- **`s3_additional_kwargs`** — Forwarded to botocore requests. e.g. s3_additional_kwargs={'ServerSideEncryption': 'aws:kms', 'SSEKMSKeyId': 'YOUR_KMS_KEY_ARN'}

**Returns**

- List of new objects paths.

**Examples**

Merging

```python
>>> import awswrangler as wr
>>> wr.s3.merge_datasets(
...     source_path="s3://bucket0/dir0/",
...     target_path="s3://bucket1/dir1/",
...     mode="append"
... )
["s3://bucket1/dir1/key0", "s3://bucket1/dir1/key1"]
```

Merging with a KMS key

```python
>>> import awswrangler as wr
>>> wr.s3.merge_datasets(
...     source_path="s3://bucket0/dir0/",
...     target_path="s3://bucket1/dir1/",
...     mode="append",
...     s3_additional_kwargs={
...         'ServerSideEncryption': 'aws:kms',
...         'SSEKMSKeyId': 'YOUR_KMS_KEY_ARN'
...     }
... )
["s3://bucket1/dir1/key0", "s3://bucket1/dir1/key1"]
```

---

### read_csv

```python
wr.s3.read_csv(
    path: 'str | list[str]',
    path_suffix: 'str | list[str] | None' = None,
    path_ignore_suffix: 'str | list[str] | None' = None,
    version_id: 'str | dict[str, str] | None' = None,
    ignore_empty: 'bool' = True,
    use_threads: 'bool | int' = True,
    last_modified_begin: 'datetime.datetime | None' = None,
    last_modified_end: 'datetime.datetime | None' = None,
    boto3_session: 'boto3.Session | None' = None,
    s3_additional_kwargs: 'dict[str, Any] | None' = None,
    dtype_backend: "Literal['numpy_nullable', 'pyarrow']" = 'numpy_nullable',
    chunksize: 'int | None' = None,
    dataset: 'bool' = False,
    partition_filter: 'Callable[[dict[str, str]], bool] | None' = None,
    ray_args: 'RaySettings | None' = None,
    **pandas_kwargs: 'Any'
) -> 'pd.DataFrame | Iterator[pd.DataFrame]'
```

Read CSV file(s) from a received S3 prefix or list of S3 objects paths.

This function accepts Unix shell-style wildcards in the path argument.
* (matches everything), ? (matches any single character),
[seq] (matches any character in seq), [!seq] (matches any character not in seq).
If you want to use a path which includes Unix shell-style wildcard characters (`*, ?, []`),
you can use `glob.escape(path)` before passing the path to this function.

:::note
For partial and gradual reading use the argument `chunksize` instead of `iterator`.
:::
:::note
In case of `use_threads=True` the number of threads
that will be spawned will be gotten from os.cpu_count().
:::
:::note
The filter by last_modified begin last_modified end is applied after list all S3 files
:::

:::note
Following arguments are not supported in distributed mode with engine `EngineEnum.RAY`:

- boto3_session
:::

**Parameters**

- **`path`** — S3 prefix (accepts Unix shell-style wildcards) (e.g. s3://bucket/prefix) or list of S3 objects paths (e.g. `[s3://bucket/key0, s3://bucket/key1]`).
- **`path_suffix`** — Suffix or List of suffixes to be read (e.g. [".csv"]). If None, will try to read all files. (default)
- **`path_ignore_suffix`** — Suffix or List of suffixes for S3 keys to be ignored.(e.g. ["_SUCCESS"]). If None, will try to read all files. (default)
- **`version_id`** — Version id of the object or mapping of object path to version id. (e.g. {'s3://bucket/key0': '121212', 's3://bucket/key1': '343434'})
- **`ignore_empty`** — Ignore files with 0 bytes.
- **`use_threads`** — True to enable concurrent requests, False to disable multiple threads. If enabled os.cpu_count() will be used as the max number of threads. If integer is provided, specified number is used.
- **`last_modified_begin`** — Filter the s3 files by the Last modified date of the object. The filter is applied only after list all s3 files.
- **`last_modified_end`** — Filter the s3 files by the Last modified date of the object. The filter is applied only after list all s3 files.
- **`boto3_session`** — Boto3 Session. The default boto3 session will be used if boto3_session receive None.
- **`pyarrow_additional_kwargs`** — Forward to botocore requests, only "SSECustomerAlgorithm" and "SSECustomerKey" arguments will be considered.
- **`dtype_backend`** — Which dtype_backend to use, e.g. whether a DataFrame should have NumPy arrays, nullable dtypes are used for all dtypes that have a nullable implementation when “numpy_nullable” is set, pyarrow is used for all dtypes if “pyarrow” is set. The dtype_backends are still experimential. The "pyarrow" backend is only supported with Pandas 2.0 or above.
- **`chunksize`** — If specified, return an generator where chunksize is the number of rows to include in each chunk.
- **`dataset`** — If `True` read a CSV dataset instead of simple file(s) loading all the related partitions as columns.
- **`partition_filter`** — Callback Function filters to apply on PARTITION columns (PUSH-DOWN filter). This function MUST receive a single argument (Dict[str, str]) where keys are partitions names and values are partitions values. Partitions values will be always strings extracted from S3. This function MUST return a bool, True to read the partition or False to ignore it. Ignored if `dataset=False`. E.g `lambda x: True if x["year"] == "2020" and x["month"] == "1" else False` https://aws-sdk-pandas.readthedocs.io/en/3.17.1/tutorials/023%20-%20Flexible%20Partitions%20Filter.html
- **`s3_additional_kwargs`** — Forwarded to botocore requests.
- **`ray_args`** — Parameters of the Ray Modin settings. Only used when distributed computing is used with Ray and Modin installed.
- **`pandas_kwargs`** — KEYWORD arguments forwarded to pandas.read_csv(). You can NOT pass `pandas_kwargs` explicitly, just add valid Pandas arguments in the function call and awswrangler will accept it. e.g. wr.s3.read_csv('s3://bucket/prefix/', sep='|', na_values=['null', 'none'], skip_blank_lines=True) https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.read_csv.html

**Returns**

- Pandas DataFrame or a Generator in case of `chunksize != None`.

**Examples**

Reading all CSV files under a prefix

```python
>>> import awswrangler as wr
>>> df = wr.s3.read_csv(path='s3://bucket/prefix/')
```

Reading all CSV files under a prefix and using pandas_kwargs

```python
>>> import awswrangler as wr
>>> df = wr.s3.read_csv('s3://bucket/prefix/', sep='|', na_values=['null', 'none'], skip_blank_lines=True)
```

Reading all CSV files from a list

```python
>>> import awswrangler as wr
>>> df = wr.s3.read_csv(path=['s3://bucket/filename0.csv', 's3://bucket/filename1.csv'])
```

Reading in chunks of 100 lines

```python
>>> import awswrangler as wr
>>> dfs = wr.s3.read_csv(path=['s3://bucket/filename0.csv', 's3://bucket/filename1.csv'], chunksize=100)
>>> for df in dfs:
>>>     print(df)  # 100 lines Pandas DataFrame
```

Reading CSV Dataset with PUSH-DOWN filter over partitions

```python
>>> import awswrangler as wr
>>> my_filter = lambda x: True if x["city"].startswith("new") else False
>>> df = wr.s3.read_csv(path, dataset=True, partition_filter=my_filter)
```

---

### read_excel

```python
wr.s3.read_excel(
    path: 'str',
    version_id: 'str | None' = None,
    use_threads: 'bool | int' = True,
    boto3_session: 'boto3.Session | None' = None,
    s3_additional_kwargs: 'dict[str, Any] | None' = None,
    **pandas_kwargs: 'Any'
) -> 'pd.DataFrame'
```

Read EXCEL file(s) from a received S3 path.

:::note
This function accepts any Pandas's read_excel() argument.
https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.read_excel.html
:::
:::note
Depending on the file extension ('xlsx', 'xls', 'odf'...), an additional library
might have to be installed first.
:::
:::note
In case of `use_threads=True` the number of threads
that will be spawned will be gotten from os.cpu_count().
:::

**Parameters**

- **`path`** — S3 path (e.g. `s3://bucket/key.xlsx`).
- **`version_id`** — Version id of the object.
- **`use_threads`** — True to enable concurrent requests, False to disable multiple threads. If enabled os.cpu_count() will be used as the max number of threads. If given an int will use the given amount of threads. If integer is provided, specified number is used.
- **`boto3_session`** — Boto3 Session. The default boto3 session will be used if boto3_session receive None.
- **`s3_additional_kwargs`** — Forward to botocore requests, only "SSECustomerAlgorithm" and "SSECustomerKey" arguments will be considered.
- **`pandas_kwargs:`** — KEYWORD arguments forwarded to pandas.read_excel(). You can NOT pass `pandas_kwargs` explicit, just add valid Pandas arguments in the function call and awswrangler will accept it. e.g. wr.s3.read_excel("s3://bucket/key.xlsx", na_rep="", verbose=True) https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.read_excel.html

**Returns**

- Pandas DataFrame.

**Examples**

Reading an EXCEL file

```python
>>> import awswrangler as wr
>>> df = wr.s3.read_excel('s3://bucket/key.xlsx')
```

---

### read_fwf

```python
wr.s3.read_fwf(
    path: 'str | list[str]',
    path_suffix: 'str | list[str] | None' = None,
    path_ignore_suffix: 'str | list[str] | None' = None,
    version_id: 'str | dict[str, str] | None' = None,
    ignore_empty: 'bool' = True,
    use_threads: 'bool | int' = True,
    last_modified_begin: 'datetime.datetime | None' = None,
    last_modified_end: 'datetime.datetime | None' = None,
    boto3_session: 'boto3.Session | None' = None,
    s3_additional_kwargs: 'dict[str, Any] | None' = None,
    chunksize: 'int | None' = None,
    dataset: 'bool' = False,
    partition_filter: 'Callable[[dict[str, str]], bool] | None' = None,
    ray_args: 'RaySettings | None' = None,
    **pandas_kwargs: 'Any'
) -> 'pd.DataFrame | Iterator[pd.DataFrame]'
```

Read fixed-width formatted file(s) from a received S3 prefix or list of S3 objects paths.

This function accepts Unix shell-style wildcards in the path argument.
* (matches everything), ? (matches any single character),
[seq] (matches any character in seq), [!seq] (matches any character not in seq).
If you want to use a path which includes Unix shell-style wildcard characters (`*, ?, []`),
you can use `glob.escape(path)` before passing the path to this function.

:::note
For partial and gradual reading use the argument `chunksize` instead of `iterator`.
:::
:::note
In case of `use_threads=True` the number of threads
that will be spawned will be gotten from os.cpu_count().
:::
:::note
The filter by last_modified begin last_modified end is applied after list all S3 files
:::

:::note
Following arguments are not supported in distributed mode with engine `EngineEnum.RAY`:

- boto3_session
:::

**Parameters**

- **`path`** — S3 prefix (accepts Unix shell-style wildcards) (e.g. s3://bucket/prefix) or list of S3 objects paths (e.g. `[s3://bucket/key0, s3://bucket/key1]`).
- **`path_suffix`** — Suffix or List of suffixes to be read (e.g. [".txt"]). If None, will try to read all files. (default)
- **`path_ignore_suffix`** — Suffix or List of suffixes for S3 keys to be ignored.(e.g. ["_SUCCESS"]). If None, will try to read all files. (default)
- **`version_id`** — Version id of the object or mapping of object path to version id. (e.g. {'s3://bucket/key0': '121212', 's3://bucket/key1': '343434'})
- **`ignore_empty`** — Ignore files with 0 bytes.
- **`use_threads`** — True to enable concurrent requests, False to disable multiple threads. If enabled os.cpu_count() will be used as the max number of threads. If integer is provided, specified number is used.
- **`last_modified_begin`** — Filter the s3 files by the Last modified date of the object. The filter is applied only after list all s3 files.
- **`last_modified_end`** — Filter the s3 files by the Last modified date of the object. The filter is applied only after list all s3 files.
- **`boto3_session`** — Boto3 Session. The default boto3 session will be used if boto3_session receive None.
- **`pyarrow_additional_kwargs`** — Forward to botocore requests, only "SSECustomerAlgorithm" and "SSECustomerKey" arguments will be considered.
- **`chunksize`** — If specified, return an generator where chunksize is the number of rows to include in each chunk.
- **`dataset`** — If `True` read a FWF dataset instead of simple file(s) loading all the related partitions as columns.
- **`partition_filter`** — Callback Function filters to apply on PARTITION columns (PUSH-DOWN filter). This function MUST receive a single argument (Dict[str, str]) where keys are partitions names and values are partitions values. Partitions values will be always strings extracted from S3. This function MUST return a bool, True to read the partition or False to ignore it. Ignored if `dataset=False`. E.g `lambda x: True if x["year"] == "2020" and x["month"] == "1" else False` https://aws-sdk-pandas.readthedocs.io/en/3.17.1/tutorials/023%20-%20Flexible%20Partitions%20Filter.html
- **`s3_additional_kwargs`** — Forwarded to botocore requests.
- **`ray_args`** — Parameters of the Ray Modin settings. Only used when distributed computing is used with Ray and Modin installed.
- **`pandas_kwargs:`** — KEYWORD arguments forwarded to pandas.read_fwf(). You can NOT pass `pandas_kwargs` explicit, just add valid Pandas arguments in the function call and awswrangler will accept it. e.g. wr.s3.read_fwf(path='s3://bucket/prefix/', widths=[1, 3], names=["c0", "c1"]) https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.read_fwf.html

**Returns**

- Pandas DataFrame or a Generator in case of `chunksize != None`.

**Examples**

Reading all fixed-width formatted (FWF) files under a prefix

```python
>>> import awswrangler as wr
>>> df = wr.s3.read_fwf(path='s3://bucket/prefix/', widths=[1, 3], names=['c0', 'c1'])
```

Reading all fixed-width formatted (FWF) files from a list

```python
>>> import awswrangler as wr
>>> df = wr.s3.read_fwf(path=['s3://bucket/0.txt', 's3://bucket/1.txt'], widths=[1, 3], names=['c0', 'c1'])
```

Reading in chunks of 100 lines

```python
>>> import awswrangler as wr
>>> dfs = wr.s3.read_fwf(
...     path=['s3://bucket/0.txt', 's3://bucket/1.txt'],
...     chunksize=100,
...     widths=[1, 3],
...     names=["c0", "c1"]
... )
>>> for df in dfs:
>>>     print(df)  # 100 lines Pandas DataFrame
```

Reading FWF Dataset with PUSH-DOWN filter over partitions

```python
>>> import awswrangler as wr
>>> my_filter = lambda x: True if x["city"].startswith("new") else False
>>> df = wr.s3.read_fwf(path, dataset=True, partition_filter=my_filter, widths=[1, 3], names=["c0", "c1"])
```

---

### read_json

```python
wr.s3.read_json(
    path: 'str | list[str]',
    path_suffix: 'str | list[str] | None' = None,
    path_ignore_suffix: 'str | list[str] | None' = None,
    version_id: 'str | dict[str, str] | None' = None,
    ignore_empty: 'bool' = True,
    orient: 'str' = 'columns',
    use_threads: 'bool | int' = True,
    last_modified_begin: 'datetime.datetime | None' = None,
    last_modified_end: 'datetime.datetime | None' = None,
    boto3_session: 'boto3.Session | None' = None,
    s3_additional_kwargs: 'dict[str, Any] | None' = None,
    dtype_backend: "Literal['numpy_nullable', 'pyarrow']" = 'numpy_nullable',
    chunksize: 'int | None' = None,
    dataset: 'bool' = False,
    partition_filter: 'Callable[[dict[str, str]], bool] | None' = None,
    ray_args: 'RaySettings | None' = None,
    **pandas_kwargs: 'Any'
) -> 'pd.DataFrame | Iterator[pd.DataFrame]'
```

Read JSON file(s) from a received S3 prefix or list of S3 objects paths.

This function accepts Unix shell-style wildcards in the path argument.
* (matches everything), ? (matches any single character),
[seq] (matches any character in seq), [!seq] (matches any character not in seq).
If you want to use a path which includes Unix shell-style wildcard characters (`*, ?, []`),
you can use `glob.escape(path)` before passing the path to this function.

:::note
For partial and gradual reading use the argument `chunksize` instead of `iterator`.
:::
:::note
In case of `use_threads=True` the number of threads
that will be spawned will be gotten from os.cpu_count().
:::
:::note
The filter by last_modified begin last_modified end is applied after list all S3 files
:::

:::note
Following arguments are not supported in distributed mode with engine `EngineEnum.RAY`:

- boto3_session
:::

**Parameters**

- **`path`** — S3 prefix (accepts Unix shell-style wildcards) (e.g. s3://bucket/prefix) or list of S3 objects paths (e.g. `[s3://bucket/key0, s3://bucket/key1]`).
- **`path_suffix`** — Suffix or List of suffixes to be read (e.g. [".json"]). If None, will try to read all files. (default)
- **`path_ignore_suffix`** — Suffix or List of suffixes for S3 keys to be ignored.(e.g. ["_SUCCESS"]). If None, will try to read all files. (default)
- **`version_id`** — Version id of the object or mapping of object path to version id. (e.g. {'s3://bucket/key0': '121212', 's3://bucket/key1': '343434'})
- **`ignore_empty`** — Ignore files with 0 bytes.
- **`orient`** — Same as Pandas: https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.read_json.html
- **`use_threads`** — True to enable concurrent requests, False to disable multiple threads. If enabled os.cpu_count() will be used as the max number of threads. If integer is provided, specified number is used.
- **`last_modified_begin`** — Filter the s3 files by the Last modified date of the object. The filter is applied only after list all s3 files.
- **`last_modified_end`** — Filter the s3 files by the Last modified date of the object. The filter is applied only after list all s3 files.
- **`boto3_session`** — Boto3 Session. The default boto3 session will be used if boto3_session receive None.
- **`pyarrow_additional_kwargs`** — Forward to botocore requests, only "SSECustomerAlgorithm" and "SSECustomerKey" arguments will be considered.
- **`dtype_backend`** — Which dtype_backend to use, e.g. whether a DataFrame should have NumPy arrays, nullable dtypes are used for all dtypes that have a nullable implementation when “numpy_nullable” is set, pyarrow is used for all dtypes if “pyarrow” is set. The dtype_backends are still experimential. The "pyarrow" backend is only supported with Pandas 2.0 or above.
- **`chunksize`** — If specified, return an generator where chunksize is the number of rows to include in each chunk.
- **`dataset`** — If `True` read a JSON dataset instead of simple file(s) loading all the related partitions as columns. If `True`, the `lines=True` will be assumed by default.
- **`partition_filter`** — Callback Function filters to apply on PARTITION columns (PUSH-DOWN filter). This function MUST receive a single argument (Dict[str, str]) where keys are partitions names and values are partitions values. Partitions values will be always strings extracted from S3. This function MUST return a bool, True to read the partition or False to ignore it. Ignored if `dataset=False`. E.g `lambda x: True if x["year"] == "2020" and x["month"] == "1" else False` https://aws-sdk-pandas.readthedocs.io/en/3.17.1/tutorials/023%20-%20Flexible%20Partitions%20Filter.html
- **`s3_additional_kwargs`** — Forwarded to botocore requests.
- **`ray_args`** — Parameters of the Ray Modin settings. Only used when distributed computing is used with Ray and Modin installed.
- **`pandas_kwargs:`** — KEYWORD arguments forwarded to pandas.read_json(). You can NOT pass `pandas_kwargs` explicit, just add valid Pandas arguments in the function call and awswrangler will accept it. e.g. wr.s3.read_json('s3://bucket/prefix/', lines=True, keep_default_dates=True) https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.read_json.html

**Returns**

- Pandas DataFrame or a Generator in case of `chunksize != None`.

**Examples**

Reading all JSON files under a prefix

```python
>>> import awswrangler as wr
>>> df = wr.s3.read_json(path='s3://bucket/prefix/')
```

Reading all CSV files under a prefix and using pandas_kwargs

```python
>>> import awswrangler as wr
>>> df = wr.s3.read_json('s3://bucket/prefix/', lines=True, keep_default_dates=True)
```

Reading all JSON files from a list

```python
>>> import awswrangler as wr
>>> df = wr.s3.read_json(path=['s3://bucket/filename0.json', 's3://bucket/filename1.json'])
```

Reading in chunks of 100 lines

```python
>>> import awswrangler as wr
>>> dfs = wr.s3.read_json(path=['s3://bucket/0.json', 's3://bucket/1.json'], chunksize=100, lines=True)
>>> for df in dfs:
>>>     print(df)  # 100 lines Pandas DataFrame
```

Reading JSON Dataset with PUSH-DOWN filter over partitions

```python
>>> import awswrangler as wr
>>> my_filter = lambda x: True if x["city"].startswith("new") else False
>>> df = wr.s3.read_json(path, dataset=True, partition_filter=my_filter)
```

---

### read_parquet

```python
wr.s3.read_parquet(
    path: 'str | list[str]',
    path_root: 'str | None' = None,
    dataset: 'bool' = False,
    path_suffix: 'str | list[str] | None' = None,
    path_ignore_suffix: 'str | list[str] | None' = None,
    ignore_empty: 'bool' = True,
    partition_filter: 'Callable[[dict[str, str]], bool] | None' = None,
    columns: 'list[str] | None' = None,
    validate_schema: 'bool' = False,
    coerce_int96_timestamp_unit: 'str | None' = None,
    schema: 'pa.Schema | None' = None,
    last_modified_begin: 'datetime.datetime | None' = None,
    last_modified_end: 'datetime.datetime | None' = None,
    version_id: 'str | dict[str, str] | None' = None,
    dtype_backend: "Literal['numpy_nullable', 'pyarrow']" = 'numpy_nullable',
    chunked: 'bool | int' = False,
    use_threads: 'bool | int' = True,
    ray_args: 'RayReadParquetSettings | None' = None,
    boto3_session: 'boto3.Session | None' = None,
    s3_additional_kwargs: 'dict[str, Any] | None' = None,
    pyarrow_additional_kwargs: 'dict[str, Any] | None' = None,
    decryption_configuration: 'ArrowDecryptionConfiguration | None' = None
) -> 'pd.DataFrame | Iterator[pd.DataFrame]'
```

Read Parquet file(s) from an S3 prefix or list of S3 objects paths.

The concept of `dataset` enables more complex features like partitioning
and catalog integration (AWS Glue Catalog).

This function accepts Unix shell-style wildcards in the path argument.
* (matches everything), ? (matches any single character),
[seq] (matches any character in seq), [!seq] (matches any character not in seq).
If you want to use a path which includes Unix shell-style wildcard characters (`*, ?, []`),
you can use `glob.escape(path)` before passing the argument to this function.

:::note
`Batching` (`chunked` argument) (Memory Friendly):

Used to return an Iterable of DataFrames instead of a regular DataFrame.

Two batching strategies are available:

- If **chunked=True**, depending on the size of the data, one or more data frames are returned per file in the path/dataset.
  Unlike **chunked=INTEGER**, rows from different files are not mixed in the resulting data frames.

- If **chunked=INTEGER**, awswrangler iterates on the data by number of rows equal to the received INTEGER.

`P.S.` `chunked=True` is faster and uses less memory while `chunked=INTEGER` is more precise
in the number of rows.
:::
:::note
If `use_threads=True`, the number of threads is obtained from os.cpu_count().
:::
:::note
Filtering by `last_modified begin` and `last_modified end` is applied after listing all S3 files
:::

:::note
This function has arguments which can be configured globally through *wr.config* or environment variables:

- dtype_backend

Check out the `Global Configurations Tutorial <https://github.com/aws/aws-sdk-pandas/blob/main/tutorials/021%20-%20Global%20Configurations.ipynb>`_ for details.
:::



:::note
Following arguments are not supported in distributed mode with engine `EngineEnum.RAY`:

- boto3_session

- version_id

- s3_additional_kwargs

- dtype_backend
:::

**Parameters**

- **`path`** — S3 prefix (accepts Unix shell-style wildcards) (e.g. s3://bucket/prefix) or list of S3 objects paths (e.g. [s3://bucket/key0, s3://bucket/key1]).
- **`path_root`** — Root path of the dataset. If dataset=`True`, it is used as a starting point to load partition columns.
- **`dataset`** — If `True`, read a parquet dataset instead of individual file(s), loading all related partitions as columns.
- **`path_suffix`** — Suffix or List of suffixes to be read (e.g. [".gz.parquet", ".snappy.parquet"]). If None, reads all files. (default)
- **`path_ignore_suffix`** — Suffix or List of suffixes to be ignored.(e.g. [".csv", "_SUCCESS"]). If None, reads all files. (default)
- **`ignore_empty`** — Ignore files with 0 bytes.
- **`partition_filter`** — Callback Function filters to apply on PARTITION columns (PUSH-DOWN filter). This function must receive a single argument (Dict[str, str]) where keys are partitions names and values are partitions values. Partitions values must be strings and the function must return a bool, True to read the partition or False to ignore it. Ignored if `dataset=False`. E.g `lambda x: True if x["year"] == "2020" and x["month"] == "1" else False` https://aws-sdk-pandas.readthedocs.io/en/3.17.1/tutorials/023%20-%20Flexible%20Partitions%20Filter.html
- **`columns`** — List of columns to read from the file(s).
- **`validate_schema`** — Check that the schema is consistent across individual files.
- **`coerce_int96_timestamp_unit`** — Cast timestamps that are stored in INT96 format to a particular resolution (e.g. "ms"). Setting to None is equivalent to "ns" and therefore INT96 timestamps are inferred as in nanoseconds.
- **`schema`** — Schema to use whem reading the file.
- **`last_modified_begin`** — Filter S3 objects by Last modified date. Filter is only applied after listing all objects.
- **`last_modified_end`** — Filter S3 objects by Last modified date. Filter is only applied after listing all objects.
- **`version_id`** — Version id of the object or mapping of object path to version id. (e.g. {'s3://bucket/key0': '121212', 's3://bucket/key1': '343434'})
- **`dtype_backend`** — Which dtype_backend to use, e.g. whether a DataFrame should have NumPy arrays, nullable dtypes are used for all dtypes that have a nullable implementation when “numpy_nullable” is set, pyarrow is used for all dtypes if “pyarrow” is set. The dtype_backends are still experimential. The "pyarrow" backend is only supported with Pandas 2.0 or above.
- **`chunked`** — If passed, the data is split into an iterable of DataFrames (Memory friendly). If `True` an iterable of DataFrames is returned without guarantee of chunksize. If an `INTEGER` is passed, an iterable of DataFrames is returned with maximum rows equal to the received INTEGER.
- **`use_threads`** — True to enable concurrent requests, False to disable multiple threads. If enabled, os.cpu_count() is used as the max number of threads. If integer is provided, specified number is used.
- **`ray_args`** — Parameters of the Ray Modin settings. Only used when distributed computing is used with Ray and Modin installed.
- **`boto3_session`** — Boto3 Session. The default boto3 session is used if None is received.
- **`s3_additional_kwargs`** — Forward to S3 botocore requests.
- **`pyarrow_additional_kwargs`** — Forwarded to `to_pandas` method converting from PyArrow tables to Pandas DataFrame. Valid values include "split_blocks", "self_destruct", "ignore_metadata". e.g. pyarrow_additional_kwargs={'split_blocks': True}.
- **`decryption_configuration`** — `pyarrow.parquet.encryption.CryptoFactory` and `pyarrow.parquet.encryption.KmsConnectionConfig` objects dict used to create a PyArrow `CryptoFactory.file_decryption_properties` object to forward to PyArrow reader. see: https://arrow.apache.org/docs/python/parquet.html#decryption-configuration Client Decryption is not supported in distributed mode.

**Returns**

- Pandas DataFrame or a Generator in case of `chunked=True`.

**Examples**

Reading all Parquet files under a prefix

```python
>>> import awswrangler as wr
>>> df = wr.s3.read_parquet(path='s3://bucket/prefix/')
```

Reading all Parquet files from a list

```python
>>> import awswrangler as wr
>>> df = wr.s3.read_parquet(path=['s3://bucket/filename0.parquet', 's3://bucket/filename1.parquet'])
```

Reading in chunks (Chunk by file)

```python
>>> import awswrangler as wr
>>> dfs = wr.s3.read_parquet(path=['s3://bucket/filename0.parquet', 's3://bucket/filename1.parquet'], chunked=True)
>>> for df in dfs:
>>>     print(df)  # Smaller Pandas DataFrame
```

Reading in chunks (Chunk by 1MM rows)

```python
>>> import awswrangler as wr
>>> dfs = wr.s3.read_parquet(
...     path=['s3://bucket/filename0.parquet', 's3://bucket/filename1.parquet'],
...     chunked=1_000_000
... )
>>> for df in dfs:
>>>     print(df)  # 1MM Pandas DataFrame
```

Reading Parquet Dataset with PUSH-DOWN filter over partitions

```python
>>> import awswrangler as wr
>>> my_filter = lambda x: True if x["city"].startswith("new") else False
>>> df = wr.s3.read_parquet(path, dataset=True, partition_filter=my_filter)
```

---

### read_parquet_metadata

```python
wr.s3.read_parquet_metadata(
    path: 'str | list[str]',
    dataset: 'bool' = False,
    version_id: 'str | dict[str, str] | None' = None,
    path_suffix: 'str | None' = None,
    path_ignore_suffix: 'str | list[str] | None' = None,
    ignore_empty: 'bool' = True,
    ignore_null: 'bool' = False,
    dtype: 'dict[str, str] | None' = None,
    sampling: 'float' = 1.0,
    coerce_int96_timestamp_unit: 'str | None' = None,
    use_threads: 'bool | int' = True,
    boto3_session: 'boto3.Session | None' = None,
    s3_additional_kwargs: 'dict[str, Any] | None' = None
) -> '_ReadTableMetadataReturnValue'
```

Read Apache Parquet file(s) metadata from an S3 prefix or list of S3 objects paths.

The concept of `dataset` enables more complex features like partitioning
and catalog integration (AWS Glue Catalog).

This function accepts Unix shell-style wildcards in the path argument.
* (matches everything), ? (matches any single character),
[seq] (matches any character in seq), [!seq] (matches any character not in seq).
If you want to use a path which includes Unix shell-style wildcard characters (`*, ?, []`),
you can use `glob.escape(path)` before passing the argument to this function.

:::note
If `use_threads=True`, the number of threads is obtained from os.cpu_count().
:::

:::note
Following arguments are not supported in distributed mode with engine `EngineEnum.RAY`:

- boto3_session
:::



:::note
This function has arguments which can be configured globally through *wr.config* or environment variables:
:::

Check out the `Global Configurations Tutorial <https://github.com/aws/aws-sdk-pandas/blob/main/tutorials/021%20-%20Global%20Configurations.ipynb>`_ for details.

**Parameters**

- **`path`** — S3 prefix (accepts Unix shell-style wildcards) (e.g. s3://bucket/prefix) or list of S3 objects paths (e.g. [s3://bucket/key0, s3://bucket/key1]).
- **`dataset`** — If `True`, read a parquet dataset instead of individual file(s), loading all related partitions as columns.
- **`version_id`** — Version id of the object or mapping of object path to version id. (e.g. {'s3://bucket/key0': '121212', 's3://bucket/key1': '343434'})
- **`path_suffix`** — Suffix or List of suffixes to be read (e.g. [".gz.parquet", ".snappy.parquet"]). If None, reads all files. (default)
- **`path_ignore_suffix`** — Suffix or List of suffixes to be ignored.(e.g. [".csv", "_SUCCESS"]). If None, reads all files. (default)
- **`ignore_empty`** — Ignore files with 0 bytes.
- **`ignore_null`** — Ignore columns with null type.
- **`dtype`** — Dictionary of columns names and Athena/Glue types to cast. Use when you have columns with undetermined data types as partitions columns. (e.g. {'col name': 'bigint', 'col2 name': 'int'})
- **`sampling`** — Ratio of files metadata to inspect. Must be `0.0 < sampling <= 1.0`. The higher, the more accurate. The lower, the faster.
- **`use_threads`** — True to enable concurrent requests, False to disable multiple threads. If enabled os.cpu_count() will be used as the max number of threads. If integer is provided, specified number is used.
- **`boto3_session`** — Boto3 Session. The default boto3 session will be used if boto3_session receive None.
- **`s3_additional_kwargs`** — Forward to S3 botocore requests.

**Returns**

- columns_types: Dictionary with keys as column names and values as data types (e.g. {'col0': 'bigint', 'col1': 'double'}). / partitions_types: Dictionary with keys as partition names and values as data types (e.g. {'col2': 'date'}).

**Examples**

Reading all Parquet files (with partitions) metadata under a prefix

```python
>>> import awswrangler as wr
>>> columns_types, partitions_types = wr.s3.read_parquet_metadata(path='s3://bucket/prefix/', dataset=True)
```

Reading all Parquet files metadata from a list

```python
>>> import awswrangler as wr
>>> columns_types, partitions_types = wr.s3.read_parquet_metadata(path=[
...     's3://bucket/filename0.parquet',
...     's3://bucket/filename1.parquet'
... ])
```

---

### read_parquet_table

```python
wr.s3.read_parquet_table(
    table: 'str',
    database: 'str',
    filename_suffix: 'str | list[str] | None' = None,
    filename_ignore_suffix: 'str | list[str] | None' = None,
    catalog_id: 'str | None' = None,
    partition_filter: 'Callable[[dict[str, str]], bool] | None' = None,
    columns: 'list[str] | None' = None,
    validate_schema: 'bool' = True,
    coerce_int96_timestamp_unit: 'str | None' = None,
    dtype_backend: "Literal['numpy_nullable', 'pyarrow']" = 'numpy_nullable',
    chunked: 'bool | int' = False,
    use_threads: 'bool | int' = True,
    ray_args: 'RayReadParquetSettings | None' = None,
    boto3_session: 'boto3.Session | None' = None,
    s3_additional_kwargs: 'dict[str, Any] | None' = None,
    pyarrow_additional_kwargs: 'dict[str, Any] | None' = None,
    decryption_configuration: 'ArrowDecryptionConfiguration | None' = None
) -> 'pd.DataFrame | Iterator[pd.DataFrame]'
```

Read Apache Parquet table registered in the AWS Glue Catalog.

:::note
`Batching` (`chunked` argument) (Memory Friendly):

Used to return an Iterable of DataFrames instead of a regular DataFrame.

Two batching strategies are available:

- If **chunked=True**, depending on the size of the data, one or more data frames are returned per file in the path/dataset.
  Unlike **chunked=INTEGER**, rows from different files will not be mixed in the resulting data frames.

- If **chunked=INTEGER**, awswrangler will iterate on the data by number of rows equal the received INTEGER.

`P.S.` `chunked=True` is faster and uses less memory while `chunked=INTEGER` is more precise
in the number of rows.
:::
:::note
If `use_threads=True`, the number of threads is obtained from os.cpu_count().
:::

:::note
This function has arguments which can be configured globally through *wr.config* or environment variables:

- catalog_id

- database

- dtype_backend

Check out the `Global Configurations Tutorial <https://github.com/aws/aws-sdk-pandas/blob/main/tutorials/021%20-%20Global%20Configurations.ipynb>`_ for details.
:::



:::note
Following arguments are not supported in distributed mode with engine `EngineEnum.RAY`:

- boto3_session

- s3_additional_kwargs

- dtype_backend
:::

**Parameters**

- **`table`** — AWS Glue Catalog table name.
- **`database`** — AWS Glue Catalog database name.
- **`filename_suffix`** — Suffix or List of suffixes to be read (e.g. [".gz.parquet", ".snappy.parquet"]). If None, read all files. (default)
- **`filename_ignore_suffix`** — Suffix or List of suffixes for S3 keys to be ignored.(e.g. [".csv", "_SUCCESS"]). If None, read all files. (default)
- **`catalog_id`** — The ID of the Data Catalog from which to retrieve Databases. If none is provided, the AWS account ID is used by default.
- **`partition_filter`** — Callback Function filters to apply on PARTITION columns (PUSH-DOWN filter). This function must receive a single argument (Dict[str, str]) where keys are partitions names and values are partitions values. Partitions values must be strings and the function must return a bool, True to read the partition or False to ignore it. Ignored if `dataset=False`. E.g `lambda x: True if x["year"] == "2020" and x["month"] == "1" else False` https://aws-sdk-pandas.readthedocs.io/en/3.17.1/tutorials/023%20-%20Flexible%20Partitions%20Filter.html
- **`columns`** — List of columns to read from the file(s).
- **`validate_schema`** — Check that the schema is consistent across individual files.
- **`coerce_int96_timestamp_unit`** — Cast timestamps that are stored in INT96 format to a particular resolution (e.g. "ms"). Setting to None is equivalent to "ns" and therefore INT96 timestamps are inferred as in nanoseconds.
- **`dtype_backend`** — Which dtype_backend to use, e.g. whether a DataFrame should have NumPy arrays, nullable dtypes are used for all dtypes that have a nullable implementation when “numpy_nullable” is set, pyarrow is used for all dtypes if “pyarrow” is set. The dtype_backends are still experimential. The "pyarrow" backend is only supported with Pandas 2.0 or above.
- **`chunked`** — If passed, the data is split into an iterable of DataFrames (Memory friendly). If `True` an iterable of DataFrames is returned without guarantee of chunksize. If an `INTEGER` is passed, an iterable of DataFrames is returned with maximum rows equal to the received INTEGER.
- **`use_threads`** — True to enable concurrent requests, False to disable multiple threads. If enabled, os.cpu_count() is used as the max number of threads. If integer is provided, specified number is used.
- **`ray_args`** — Parameters of the Ray Modin settings. Only used when distributed computing is used with Ray and Modin installed.
- **`boto3_session`** — Boto3 Session. The default boto3 session is used if None is received.
- **`s3_additional_kwargs`** — Forward to S3 botocore requests.
- **`pyarrow_additional_kwargs`** — Forwarded to `to_pandas` method converting from PyArrow tables to Pandas DataFrame. Valid values include "split_blocks", "self_destruct", "ignore_metadata". e.g. pyarrow_additional_kwargs={'split_blocks': True}.
- **`decryption_configuration`** — `pyarrow.parquet.encryption.CryptoFactory` and `pyarrow.parquet.encryption.KmsConnectionConfig` objects dict used to create a PyArrow `CryptoFactory.file_decryption_properties` object to forward to PyArrow reader. Client Decryption is not supported in distributed mode.

**Returns**

- Pandas DataFrame or a Generator in case of `chunked=True`.

**Examples**

Reading Parquet Table

```python
>>> import awswrangler as wr
>>> df = wr.s3.read_parquet_table(database='...', table='...')
```

Reading Parquet Table in chunks (Chunk by file)

```python
>>> import awswrangler as wr
>>> dfs = wr.s3.read_parquet_table(database='...', table='...', chunked=True)
>>> for df in dfs:
>>>     print(df)  # Smaller Pandas DataFrame
```

Reading Parquet Dataset with PUSH-DOWN filter over partitions

```python
>>> import awswrangler as wr
>>> my_filter = lambda x: True if x["city"].startswith("new") else False
>>> df = wr.s3.read_parquet_table(path, dataset=True, partition_filter=my_filter)
```

---

### read_orc

```python
wr.s3.read_orc(
    path: 'str | list[str]',
    path_root: 'str | None' = None,
    dataset: 'bool' = False,
    path_suffix: 'str | list[str] | None' = None,
    path_ignore_suffix: 'str | list[str] | None' = None,
    ignore_empty: 'bool' = True,
    partition_filter: 'Callable[[dict[str, str]], bool] | None' = None,
    columns: 'list[str] | None' = None,
    validate_schema: 'bool' = False,
    last_modified_begin: 'datetime.datetime | None' = None,
    last_modified_end: 'datetime.datetime | None' = None,
    version_id: 'str | dict[str, str] | None' = None,
    dtype_backend: "Literal['numpy_nullable', 'pyarrow']" = 'numpy_nullable',
    use_threads: 'bool | int' = True,
    ray_args: 'RaySettings | None' = None,
    boto3_session: 'boto3.Session | None' = None,
    s3_additional_kwargs: 'dict[str, Any] | None' = None,
    pyarrow_additional_kwargs: 'dict[str, Any] | None' = None
) -> 'pd.DataFrame'
```

Read ORC file(s) from an S3 prefix or list of S3 objects paths.

The concept of `dataset` enables more complex features like partitioning
and catalog integration (AWS Glue Catalog).

This function accepts Unix shell-style wildcards in the path argument.
* (matches everything), ? (matches any single character),
[seq] (matches any character in seq), [!seq] (matches any character not in seq).
If you want to use a path which includes Unix shell-style wildcard characters (`*, ?, []`),
you can use `glob.escape(path)` before passing the argument to this function.

:::note
If `use_threads=True`, the number of threads is obtained from os.cpu_count().
:::
:::note
Filtering by `last_modified begin` and `last_modified end` is applied after listing all S3 files
:::

:::note
This function has arguments which can be configured globally through *wr.config* or environment variables:

- dtype_backend

Check out the `Global Configurations Tutorial <https://github.com/aws/aws-sdk-pandas/blob/main/tutorials/021%20-%20Global%20Configurations.ipynb>`_ for details.
:::



:::note
Following arguments are not supported in distributed mode with engine `EngineEnum.RAY`:

- boto3_session

- version_id

- s3_additional_kwargs

- dtype_backend
:::

**Parameters**

- **`path`** — S3 prefix (accepts Unix shell-style wildcards) (e.g. s3://bucket/prefix) or list of S3 objects paths (e.g. [s3://bucket/key0, s3://bucket/key1]).
- **`path_root`** — Root path of the dataset. If dataset=`True`, it is used as a starting point to load partition columns.
- **`dataset`** — If `True`, read an ORC dataset instead of individual file(s), loading all related partitions as columns.
- **`path_suffix`** — Suffix or List of suffixes to be read (e.g. [".gz.orc", ".snappy.orc"]). If None, reads all files. (default)
- **`path_ignore_suffix`** — Suffix or List of suffixes to be ignored.(e.g. [".csv", "_SUCCESS"]). If None, reads all files. (default)
- **`ignore_empty`** — Ignore files with 0 bytes.
- **`partition_filter`** — Callback Function filters to apply on PARTITION columns (PUSH-DOWN filter). This function must receive a single argument (Dict[str, str]) where keys are partitions names and values are partitions values. Partitions values must be strings and the function must return a bool, True to read the partition or False to ignore it. Ignored if `dataset=False`. E.g `lambda x: True if x["year"] == "2020" and x["month"] == "1" else False` https://aws-sdk-pandas.readthedocs.io/en/3.17.1/tutorials/023%20-%20Flexible%20Partitions%20Filter.html
- **`columns`** — List of columns to read from the file(s).
- **`validate_schema`** — Check that the schema is consistent across individual files.
- **`last_modified_begin`** — Filter S3 objects by Last modified date. Filter is only applied after listing all objects.
- **`last_modified_end`** — Filter S3 objects by Last modified date. Filter is only applied after listing all objects.
- **`version_id`** — Version id of the object or mapping of object path to version id. (e.g. {'s3://bucket/key0': '121212', 's3://bucket/key1': '343434'})
- **`dtype_backend`** — Which dtype_backend to use, e.g. whether a DataFrame should have NumPy arrays, nullable dtypes are used for all dtypes that have a nullable implementation when “numpy_nullable” is set, pyarrow is used for all dtypes if “pyarrow” is set. The dtype_backends are still experimential. The "pyarrow" backend is only supported with Pandas 2.0 or above.
- **`use_threads`** — True to enable concurrent requests, False to disable multiple threads. If enabled, os.cpu_count() is used as the max number of threads. If integer is provided, specified number is used.
- **`ray_args`** — Parameters of the Ray Modin settings. Only used when distributed computing is used with Ray and Modin installed.
- **`boto3_session`** — Boto3 Session. The default boto3 session is used if None is received.
- **`s3_additional_kwargs`** — Forward to S3 botocore requests.
- **`pyarrow_additional_kwargs`** — Forwarded to `to_pandas` method converting from PyArrow tables to Pandas DataFrame. Valid values include "split_blocks", "self_destruct", "ignore_metadata". e.g. pyarrow_additional_kwargs={'split_blocks': True}.

**Returns**

- Pandas DataFrame.

**Examples**

Reading all ORC files under a prefix

```python
>>> import awswrangler as wr
>>> df = wr.s3.read_orc(path='s3://bucket/prefix/')
```

Reading all ORC files from a list

```python
>>> import awswrangler as wr
>>> df = wr.s3.read_orc(path=['s3://bucket/filename0.orc', 's3://bucket/filename1.orc'])
```

Reading ORC Dataset with PUSH-DOWN filter over partitions

```python
>>> import awswrangler as wr
>>> my_filter = lambda x: True if x["city"].startswith("new") else False
>>> df = wr.s3.read_orc(path, dataset=True, partition_filter=my_filter)
```

---

### read_orc_metadata

```python
wr.s3.read_orc_metadata(
    path: 'str | list[str]',
    dataset: 'bool' = False,
    version_id: 'str | dict[str, str] | None' = None,
    path_suffix: 'str | None' = None,
    path_ignore_suffix: 'str | list[str] | None' = None,
    ignore_empty: 'bool' = True,
    ignore_null: 'bool' = False,
    dtype: 'dict[str, str] | None' = None,
    sampling: 'float' = 1.0,
    use_threads: 'bool | int' = True,
    boto3_session: 'boto3.Session | None' = None,
    s3_additional_kwargs: 'dict[str, Any] | None' = None
) -> '_ReadTableMetadataReturnValue'
```

Read Apache ORC file(s) metadata from an S3 prefix or list of S3 objects paths.

The concept of `dataset` enables more complex features like partitioning
and catalog integration (AWS Glue Catalog).

This function accepts Unix shell-style wildcards in the path argument.
* (matches everything), ? (matches any single character),
[seq] (matches any character in seq), [!seq] (matches any character not in seq).
If you want to use a path which includes Unix shell-style wildcard characters (`*, ?, []`),
you can use `glob.escape(path)` before passing the argument to this function.

:::note
If `use_threads=True`, the number of threads is obtained from os.cpu_count().
:::

:::note
This function has arguments which can be configured globally through *wr.config* or environment variables:
:::

Check out the `Global Configurations Tutorial <https://github.com/aws/aws-sdk-pandas/blob/main/tutorials/021%20-%20Global%20Configurations.ipynb>`_ for details.




:::note
Following arguments are not supported in distributed mode with engine `EngineEnum.RAY`:

- boto3_session
:::

**Parameters**

- **`path`** — S3 prefix (accepts Unix shell-style wildcards) (e.g. s3://bucket/prefix) or list of S3 objects paths (e.g. [s3://bucket/key0, s3://bucket/key1]).
- **`dataset`** — If `True`, read an ORC dataset instead of individual file(s), loading all related partitions as columns.
- **`version_id`** — Version id of the object or mapping of object path to version id. (e.g. {'s3://bucket/key0': '121212', 's3://bucket/key1': '343434'})
- **`path_suffix`** — Suffix or List of suffixes to be read (e.g. [".gz.orc", ".snappy.orc"]). If None, reads all files. (default)
- **`path_ignore_suffix`** — Suffix or List of suffixes to be ignored.(e.g. [".csv", "_SUCCESS"]). If None, reads all files. (default)
- **`ignore_empty`** — Ignore files with 0 bytes.
- **`ignore_null`** — Ignore columns with null type.
- **`dtype`** — Dictionary of columns names and Athena/Glue types to cast. Use when you have columns with undetermined data types as partitions columns. (e.g. {'col name': 'bigint', 'col2 name': 'int'})
- **`sampling`** — Ratio of files metadata to inspect. Must be `0.0 < sampling <= 1.0`. The higher, the more accurate. The lower, the faster.
- **`use_threads`** — True to enable concurrent requests, False to disable multiple threads. If enabled os.cpu_count() will be used as the max number of threads. If integer is provided, specified number is used.
- **`boto3_session`** — Boto3 Session. The default boto3 session will be used if boto3_session receive None.
- **`s3_additional_kwargs`** — Forward to S3 botocore requests.

**Returns**

- columns_types: Dictionary with keys as column names and values as data types (e.g. {'col0': 'bigint', 'col1': 'double'}). / partitions_types: Dictionary with keys as partition names and values as data types (e.g. {'col2': 'date'}).

**Examples**

Reading all ORC files (with partitions) metadata under a prefix

```python
>>> import awswrangler as wr
>>> columns_types, partitions_types = wr.s3.read_orc_metadata(path='s3://bucket/prefix/', dataset=True)
```

Reading all ORC files metadata from a list

```python
>>> import awswrangler as wr
>>> columns_types, partitions_types = wr.s3.read_orc_metadata(path=[
...     's3://bucket/filename0.orc',
...     's3://bucket/filename1.orc',
... ])
```

---

### read_orc_table

```python
wr.s3.read_orc_table(
    table: 'str',
    database: 'str',
    filename_suffix: 'str | list[str] | None' = None,
    filename_ignore_suffix: 'str | list[str] | None' = None,
    catalog_id: 'str | None' = None,
    partition_filter: 'Callable[[dict[str, str]], bool] | None' = None,
    columns: 'list[str] | None' = None,
    validate_schema: 'bool' = True,
    dtype_backend: "Literal['numpy_nullable', 'pyarrow']" = 'numpy_nullable',
    use_threads: 'bool | int' = True,
    ray_args: 'RaySettings | None' = None,
    boto3_session: 'boto3.Session | None' = None,
    s3_additional_kwargs: 'dict[str, Any] | None' = None,
    pyarrow_additional_kwargs: 'dict[str, Any] | None' = None
) -> 'pd.DataFrame'
```

Read Apache ORC table registered in the AWS Glue Catalog.

:::note
If `use_threads=True`, the number of threads is obtained from os.cpu_count().
:::

:::note
This function has arguments which can be configured globally through *wr.config* or environment variables:

- catalog_id

- database

- dtype_backend

Check out the `Global Configurations Tutorial <https://github.com/aws/aws-sdk-pandas/blob/main/tutorials/021%20-%20Global%20Configurations.ipynb>`_ for details.
:::



:::note
Following arguments are not supported in distributed mode with engine `EngineEnum.RAY`:

- boto3_session

- s3_additional_kwargs

- dtype_backend
:::

**Parameters**

- **`table`** — AWS Glue Catalog table name.
- **`database`** — AWS Glue Catalog database name.
- **`filename_suffix`** — Suffix or List of suffixes to be read (e.g. [".gz.orc", ".snappy.orc"]). If None, read all files. (default)
- **`filename_ignore_suffix`** — Suffix or List of suffixes for S3 keys to be ignored.(e.g. [".csv", "_SUCCESS"]). If None, read all files. (default)
- **`catalog_id`** — The ID of the Data Catalog from which to retrieve Databases. If none is provided, the AWS account ID is used by default.
- **`partition_filter`** — Callback Function filters to apply on PARTITION columns (PUSH-DOWN filter). This function must receive a single argument (Dict[str, str]) where keys are partitions names and values are partitions values. Partitions values must be strings and the function must return a bool, True to read the partition or False to ignore it. Ignored if `dataset=False`. E.g `lambda x: True if x["year"] == "2020" and x["month"] == "1" else False` https://aws-sdk-pandas.readthedocs.io/en/3.17.1/tutorials/023%20-%20Flexible%20Partitions%20Filter.html
- **`columns`** — List of columns to read from the file(s).
- **`validate_schema`** — Check that the schema is consistent across individual files.
- **`dtype_backend`** — Which dtype_backend to use, e.g. whether a DataFrame should have NumPy arrays, nullable dtypes are used for all dtypes that have a nullable implementation when “numpy_nullable” is set, pyarrow is used for all dtypes if “pyarrow” is set. The dtype_backends are still experimential. The "pyarrow" backend is only supported with Pandas 2.0 or above.
- **`use_threads`** — True to enable concurrent requests, False to disable multiple threads. If enabled, os.cpu_count() is used as the max number of threads. If integer is provided, specified number is used.
- **`ray_args`** — Parameters of the Ray Modin settings. Only used when distributed computing is used with Ray and Modin installed.
- **`boto3_session`** — Boto3 Session. The default boto3 session is used if None is received.
- **`s3_additional_kwargs`** — Forward to S3 botocore requests.
- **`pyarrow_additional_kwargs`** — Forwarded to `to_pandas` method converting from PyArrow tables to Pandas DataFrame. Valid values include "split_blocks", "self_destruct", "ignore_metadata". e.g. pyarrow_additional_kwargs={'split_blocks': True}.

**Returns**

- Pandas DataFrame.

**Examples**

Reading ORC Table

```python
>>> import awswrangler as wr
>>> df = wr.s3.read_orc_table(database='...', table='...')
```

Reading ORC Dataset with PUSH-DOWN filter over partitions

```python
>>> import awswrangler as wr
>>> my_filter = lambda x: True if x["city"].startswith("new") else False
>>> df = wr.s3.read_orc_table(path, dataset=True, partition_filter=my_filter)
```

---

### read_deltalake

```python
wr.s3.read_deltalake(
    path: 'str',
    version: 'int | None' = None,
    partitions: 'list[tuple[str, str, Any]] | None' = None,
    columns: 'list[str] | None' = None,
    without_files: 'bool' = False,
    dtype_backend: "Literal['numpy_nullable', 'pyarrow']" = 'numpy_nullable',
    use_threads: 'bool' = True,
    boto3_session: 'boto3.Session | None' = None,
    s3_additional_kwargs: 'dict[str, str] | None' = None,
    pyarrow_additional_kwargs: 'dict[str, Any] | None' = None
) -> 'pd.DataFrame'
```

Load a Deltalake table data from an S3 path.

This function requires the `deltalake package
<https://delta-io.github.io/delta-rs/python>`__.
See the `How to load a Delta table
<https://delta-io.github.io/delta-rs/python/usage.html#loading-a-delta-table>`__
guide for loading instructions.


:::note
This function has arguments which can be configured globally through *wr.config* or environment variables:

- dtype_backend

Check out the `Global Configurations Tutorial <https://github.com/aws/aws-sdk-pandas/blob/main/tutorials/021%20-%20Global%20Configurations.ipynb>`_ for details.
:::

**Parameters**

- **`path`** — The path of the DeltaTable.
- **`version`** — The version of the DeltaTable.
- **`partitions`** — A list of partition filters, see help(DeltaTable.files_by_partitions) for filter syntax.
- **`columns`** — The columns to project. This can be a list of column names to include (order and duplicates are preserved).
- **`without_files`** — If True, load the table without tracking files (memory-friendly). Some append-only applications might not need to track files.
- **`dtype_backend`** — Which dtype_backend to use, e.g. whether a DataFrame should have NumPy arrays, nullable dtypes are used for all dtypes that have a nullable implementation when “numpy_nullable” is set, pyarrow is used for all dtypes if “pyarrow” is set. The dtype_backends are still experimential. The "pyarrow" backend is only supported with Pandas 2.0 or above.
- **`use_threads`** — True to enable concurrent requests, False to disable multiple threads. When enabled, os.cpu_count() is used as the max number of threads.
- **`boto3_session`** — Boto3 Session. If None, the default boto3 session is used.
- **`s3_additional_kwargs`** — Forwarded to the Delta Table class for the storage options of the S3 backend.
- **`pyarrow_additional_kwargs`** — Forwarded to the PyArrow to_pandas method.

**Returns**

- DataFrame with the results.

**See Also**

deltalake.DeltaTable : Create a DeltaTable instance with the deltalake library.

---

### select_query

```python
wr.s3.select_query(
    sql: 'str',
    path: 'str | list[str]',
    input_serialization: 'str',
    input_serialization_params: 'dict[str, bool | str]',
    compression: 'str | None' = None,
    scan_range_chunk_size: 'int | None' = None,
    path_suffix: 'str | list[str] | None' = None,
    path_ignore_suffix: 'str | list[str] | None' = None,
    ignore_empty: 'bool' = True,
    use_threads: 'bool | int' = True,
    last_modified_begin: 'datetime.datetime | None' = None,
    last_modified_end: 'datetime.datetime | None' = None,
    dtype_backend: "Literal['numpy_nullable', 'pyarrow']" = 'numpy_nullable',
    boto3_session: 'boto3.Session | None' = None,
    s3_additional_kwargs: 'dict[str, Any] | None' = None,
    pyarrow_additional_kwargs: 'dict[str, Any] | None' = None
) -> 'pd.DataFrame'
```

Filter contents of Amazon S3 objects based on SQL statement.

Note: Scan ranges are only supported for uncompressed CSV/JSON, CSV (without quoted delimiters)
and JSON objects (in LINES mode only). It means scanning cannot be split across threads if the
aforementioned conditions are not met, leading to lower performance.


:::note
Following arguments are not supported in distributed mode with engine `EngineEnum.RAY`:

- boto3_session
:::



:::warning
This API is deprecated and will be removed in future AWS SDK for Pandas releases.
:::

**Parameters**

- **`sql`** — SQL statement used to query the object.
- **`path`** — S3 prefix (accepts Unix shell-style wildcards) (e.g. s3://bucket/prefix) or list of S3 objects paths (e.g. `[s3://bucket/key0, s3://bucket/key1]`).
- **`input_serialization`** — Format of the S3 object queried. Valid values: "CSV", "JSON", or "Parquet". Case sensitive.
- **`input_serialization_params`** — Dictionary describing the serialization of the S3 object.
- **`compression`** — Compression type of the S3 object. Valid values: None, "gzip", or "bzip2". gzip and bzip2 are only valid for CSV and JSON objects.
- **`scan_range_chunk_size`** — Chunk size used to split the S3 object into scan ranges. 1,048,576 by default.
- **`path_suffix`** — Suffix or List of suffixes to be read (e.g. [".csv"]). If None, read all files. (default)
- **`path_ignore_suffix`** — Suffix or List of suffixes for S3 keys to be ignored. (e.g. ["_SUCCESS"]). If None, read all files. (default)
- **`ignore_empty`** — Ignore files with 0 bytes.
- **`use_threads`** — True (default) to enable concurrent requests, False to disable multiple threads. If enabled os.cpu_count() is used as the max number of threads. If integer is provided, specified number is used.
- **`last_modified_begin`** — Filter S3 objects by Last modified date. Filter is only applied after listing all objects.
- **`last_modified_end`** — Filter S3 objects by Last modified date. Filter is only applied after listing all objects.
- **`dtype_backend`** — Which dtype_backend to use, e.g. whether a DataFrame should have NumPy arrays, nullable dtypes are used for all dtypes that have a nullable implementation when “numpy_nullable” is set, pyarrow is used for all dtypes if “pyarrow” is set. The dtype_backends are still experimential. The "pyarrow" backend is only supported with Pandas 2.0 or above.
- **`boto3_session`** — The default boto3 session is used if none is provided.
- **`s3_additional_kwargs`** — Forwarded to botocore requests. Valid values: "SSECustomerAlgorithm", "SSECustomerKey", "ExpectedBucketOwner". e.g. s3_additional_kwargs={'SSECustomerAlgorithm': 'md5'}.
- **`pyarrow_additional_kwargs`** — Forwarded to `to_pandas` method converting from PyArrow tables to Pandas DataFrame. Valid values include "split_blocks", "self_destruct", "ignore_metadata". e.g. pyarrow_additional_kwargs={'split_blocks': True}.

**Returns**

- Pandas DataFrame with results from query.

**Examples**

Reading a gzip compressed JSON document

```python
>>> import awswrangler as wr
>>> df = wr.s3.select_query(
...     sql='SELECT * FROM s3object[*][*]',
...     path='s3://bucket/key.json.gzip',
...     input_serialization='JSON',
...     input_serialization_params={
...         'Type': 'Document',
...     },
...     compression="gzip",
... )
```

Reading multiple CSV objects from a prefix

```python
>>> import awswrangler as wr
>>> df = wr.s3.select_query(
...     sql='SELECT * FROM s3object',
...     path='s3://bucket/prefix/',
...     input_serialization='CSV',
...     input_serialization_params={
...         'FileHeaderInfo': 'Use',
...         'RecordDelimiter': '\r\n'
...     },
... )
```

Reading a single column from Parquet object with pushdown filter

```python
>>> import awswrangler as wr
>>> df = wr.s3.select_query(
...     sql='SELECT s.\"id\" FROM s3object s where s.\"id\" = 1.0',
...     path='s3://bucket/key.snappy.parquet',
...     input_serialization='Parquet',
... )
```

---

### size_objects

```python
wr.s3.size_objects(
    path: 'str | list[str]',
    version_id: 'str | dict[str, str] | None' = None,
    use_threads: 'bool | int' = True,
    s3_additional_kwargs: 'dict[str, Any] | None' = None,
    boto3_session: 'boto3.Session | None' = None
) -> 'dict[str, int | None]'
```

Get the size (ContentLength) in bytes of Amazon S3 objects from a received S3 prefix or list of S3 objects paths.

This function accepts Unix shell-style wildcards in the path argument.
* (matches everything), ? (matches any single character),
[seq] (matches any character in seq), [!seq] (matches any character not in seq).
If you want to use a path which includes Unix shell-style wildcard characters (`*, ?, []`),
you can use `glob.escape(path)` before passing the path to this function.

:::note
In case of `use_threads=True` the number of threads
that will be spawned will be gotten from os.cpu_count().
:::

:::note
Following arguments are not supported in distributed mode with engine `EngineEnum.RAY`:

- boto3_session

- s3_additional_kwargs
:::

**Parameters**

- **`path`** — S3 prefix (accepts Unix shell-style wildcards) (e.g. s3://bucket/prefix) or list of S3 objects paths (e.g. [s3://bucket/key0, s3://bucket/key1]).
- **`version_id`** — Version id of the object or mapping of object path to version id. (e.g. {'s3://bucket/key0': '121212', 's3://bucket/key1': '343434'})
- **`use_threads`** — True to enable concurrent requests, False to disable multiple threads. If enabled os.cpu_count() will be used as the max number of threads. If integer is provided, specified number is used.
- **`s3_additional_kwargs`** — Forwarded to botocore requests. e.g. s3_additional_kwargs={'RequestPayer': 'requester'}
- **`boto3_session`** — Boto3 Session. The default boto3 session will be used if boto3_session receive None.

**Returns**

- Dictionary where the key is the object path and the value is the object size.

**Examples**

```python
>>> import awswrangler as wr
>>> sizes0 = wr.s3.size_objects(['s3://bucket/key0', 's3://bucket/key1'])  # Get the sizes of both objects
>>> sizes1 = wr.s3.size_objects('s3://bucket/prefix')  # Get the sizes of all objects under the received prefix
```

---

### store_parquet_metadata

```python
wr.s3.store_parquet_metadata(
    path: 'str',
    database: 'str',
    table: 'str',
    catalog_id: 'str | None' = None,
    path_suffix: 'str | None' = None,
    path_ignore_suffix: 'str | list[str] | None' = None,
    ignore_empty: 'bool' = True,
    ignore_null: 'bool' = False,
    dtype: 'dict[str, str] | None' = None,
    sampling: 'float' = 1.0,
    dataset: 'bool' = False,
    use_threads: 'bool | int' = True,
    description: 'str | None' = None,
    parameters: 'dict[str, str] | None' = None,
    columns_comments: 'dict[str, str] | None' = None,
    compression: 'str | None' = None,
    mode: "Literal['append', 'overwrite']" = 'overwrite',
    catalog_versioning: 'bool' = False,
    regular_partitions: 'bool' = True,
    athena_partition_projection_settings: 'typing.AthenaPartitionProjectionSettings | None' = None,
    s3_additional_kwargs: 'dict[str, Any] | None' = None,
    boto3_session: 'boto3.Session | None' = None
) -> 'tuple[dict[str, str], dict[str, str] | None, dict[str, list[str]] | None]'
```

Infer and store parquet metadata on AWS Glue Catalog.

Infer Apache Parquet file(s) metadata from a received S3 prefix
And then stores it on AWS Glue Catalog including all inferred partitions
(No need for 'MSCK REPAIR TABLE')

The concept of Dataset goes beyond the simple idea of files and enables more
complex features like partitioning and catalog integration (AWS Glue Catalog).

This function accepts Unix shell-style wildcards in the path argument.
* (matches everything), ? (matches any single character),
[seq] (matches any character in seq), [!seq] (matches any character not in seq).
If you want to use a path which includes Unix shell-style wildcard characters (`*, ?, []`),
you can use `glob.escape(path)` before passing the path to this function.

:::note
In case of `use_threads=True` the number of threads
that will be spawned will be gotten from os.cpu_count().
:::

:::note
Following arguments are not supported in distributed mode with engine `EngineEnum.RAY`:

- boto3_session
:::



:::note
This function has arguments which can be configured globally through *wr.config* or environment variables:

- catalog_id

- database

Check out the `Global Configurations Tutorial <https://github.com/aws/aws-sdk-pandas/blob/main/tutorials/021%20-%20Global%20Configurations.ipynb>`_ for details.
:::

**Parameters**

- **`path`** — S3 prefix (accepts Unix shell-style wildcards) (e.g. s3://bucket/prefix).
- **`table`** — Glue/Athena catalog: Table name.
- **`database`** — AWS Glue Catalog database name.
- **`catalog_id`** — The ID of the Data Catalog from which to retrieve Databases. If none is provided, the AWS account ID is used by default.
- **`path_suffix`** — Suffix or List of suffixes for filtering S3 keys.
- **`path_ignore_suffix`** — Suffix or List of suffixes for S3 keys to be ignored.
- **`ignore_empty`** — Ignore files with 0 bytes.
- **`ignore_null`** — Ignore columns with null type.
- **`dtype`** — Dictionary of columns names and Athena/Glue types to be casted. Useful when you have columns with undetermined data types as partitions columns. (e.g. {'col name': 'bigint', 'col2 name': 'int'})
- **`sampling`** — Random sample ratio of files that will have the metadata inspected. Must be `0.0 < sampling <= 1.0`. The higher, the more accurate. The lower, the faster.
- **`dataset`** — If True read a parquet dataset instead of simple file(s) loading all the related partitions as columns.
- **`use_threads`** — True to enable concurrent requests, False to disable multiple threads. If enabled os.cpu_count() will be used as the max number of threads. If integer is provided, specified number is used.
- **`description`** — Glue/Athena catalog: Table description
- **`parameters`** — Glue/Athena catalog: Key/value pairs to tag the table.
- **`columns_comments`** — Glue/Athena catalog: Columns names and the related comments (e.g. {'col0': 'Column 0.', 'col1': 'Column 1.', 'col2': 'Partition.'}).
- **`compression`** — Compression style (`None`, `snappy`, `gzip`, etc).
- **`mode`** — 'overwrite' to recreate any possible existing table or 'append' to keep any possible existing table.
- **`catalog_versioning`** — If True and `mode="overwrite"`, creates an archived version of the table catalog before updating it.
- **`regular_partitions`** — Create regular partitions (Non projected partitions) on Glue Catalog. Disable when you will work only with Partition Projection. Keep enabled even when working with projections is useful to keep Redshift Spectrum working with the regular partitions.
- **`athena_partition_projection_settings`** — Parameters of the Athena Partition Projection (https://docs.aws.amazon.com/athena/latest/ug/partition-projection.html). AthenaPartitionProjectionSettings is a `TypedDict`, meaning the passed parameter can be instantiated either as an instance of AthenaPartitionProjectionSettings or as a regular Python dict. Following projection parameters are supported: .. list-table:: Projection Parameters :header-rows: 1 * - Name - Type - Description * - projection_types - Optional[Dict[str, str]] - Dictionary of partitions names and Athena projections types. Valid types: "enum", "integer", "date", "injected" https://docs.aws.amazon.com/athena/latest/ug/partition-projection-supported-types.html (e.g. {'col_name': 'enum', 'col2_name': 'integer'}) * - projection_ranges - Optional[Dict[str, str]] - Dictionary of partitions names and Athena projections ranges. https://docs.aws.amazon.com/athena/latest/ug/partition-projection-supported-types.html (e.g. {'col_name': '0,10', 'col2_name': '-1,8675309'}) * - projection_values - Optional[Dict[str, str]] - Dictionary of partitions names and Athena projections values. https://docs.aws.amazon.com/athena/latest/ug/partition-projection-supported-types.html (e.g. {'col_name': 'A,B,Unknown', 'col2_name': 'foo,boo,bar'}) * - projection_intervals - Optional[Dict[str, str]] - Dictionary of partitions names and Athena projections intervals. https://docs.aws.amazon.com/athena/latest/ug/partition-projection-supported-types.html (e.g. {'col_name': '1', 'col2_name': '5'}) * - projection_digits - Optional[Dict[str, str]] - Dictionary of partitions names and Athena projections digits. https://docs.aws.amazon.com/athena/latest/ug/partition-projection-supported-types.html (e.g. {'col_name': '1', 'col2_name': '2'}) * - projection_formats - Optional[Dict[str, str]] - Dictionary of partitions names and Athena projections formats. https://docs.aws.amazon.com/athena/latest/ug/partition-projection-supported-types.html (e.g. {'col_date': 'yyyy-MM-dd', 'col2_timestamp': 'yyyy-MM-dd HH:mm:ss'}) * - projection_storage_location_template - Optional[str] - Value which is allows Athena to properly map partition values if the S3 file locations do not follow a typical `.../column=value/...` pattern. https://docs.aws.amazon.com/athena/latest/ug/partition-projection-setting-up.html (e.g. s3://bucket/table_root/a=${a}/${b}/some_static_subdirectory/${c}/)
- **`s3_additional_kwargs`** — Forwarded to botocore requests. e.g. s3_additional_kwargs={'ServerSideEncryption': 'aws:kms', 'SSEKMSKeyId': 'YOUR_KMS_KEY_ARN'}
- **`boto3_session`** — The default boto3 session will be used if boto3_session receive None.

**Returns**

- The metadata used to create the Glue Table. columns_types: Dictionary with keys as column names and values as data types (e.g. {'col0': 'bigint', 'col1': 'double'}). / partitions_types: Dictionary with keys as partition names and values as data types (e.g. {'col2': 'date'}). / partitions_values: Dictionary with keys as S3 path locations and values as a list of partitions values as str (e.g. {'s3://bucket/prefix/y=2020/m=10/': ['2020', '10']}).

**Examples**

Reading all Parquet files metadata under a prefix

```python
>>> import awswrangler as wr
>>> columns_types, partitions_types, partitions_values = wr.s3.store_parquet_metadata(
...     path='s3://bucket/prefix/',
...     database='...',
...     table='...',
...     dataset=True
... )
```

---

### to_csv

```python
wr.s3.to_csv(
    df: 'pd.DataFrame',
    path: 'str | None' = None,
    sep: 'str' = ',',
    index: 'bool' = True,
    columns: 'list[str] | None' = None,
    use_threads: 'bool | int' = True,
    boto3_session: 'boto3.Session | None' = None,
    s3_additional_kwargs: 'dict[str, Any] | None' = None,
    sanitize_columns: 'bool' = False,
    dataset: 'bool' = False,
    filename_prefix: 'str | None' = None,
    partition_cols: 'list[str] | None' = None,
    bucketing_info: 'BucketingInfoTuple | None' = None,
    concurrent_partitioning: 'bool' = False,
    mode: "Literal['append', 'overwrite', 'overwrite_partitions'] | None" = None,
    catalog_versioning: 'bool' = False,
    schema_evolution: 'bool' = False,
    dtype: 'dict[str, str] | None' = None,
    database: 'str | None' = None,
    table: 'str | None' = None,
    glue_table_settings: 'GlueTableSettings | None' = None,
    athena_partition_projection_settings: 'typing.AthenaPartitionProjectionSettings | None' = None,
    catalog_id: 'str | None' = None,
    **pandas_kwargs: 'Any'
) -> '_S3WriteDataReturnValue'
```

Write CSV file or dataset on Amazon S3.

The concept of Dataset goes beyond the simple idea of ordinary files and enable more
complex features like partitioning and catalog integration (Amazon Athena/AWS Glue Catalog).

:::note
If database` and `table` arguments are passed, the table name and all column names
will be automatically sanitized using `wr.catalog.sanitize_table_name` and `wr.catalog.sanitize_column_name`.
Please, pass `sanitize_columns=True` to enforce this behaviour always.
:::
:::note
If `table` and `database` arguments are passed, `pandas_kwargs` will be ignored due
restrictive quoting, date_format, escapechar and encoding required by Athena/Glue Catalog.
:::
:::note
In case of `use_threads=True` the number of threads
that will be spawned will be gotten from os.cpu_count().
:::

:::note
Following arguments are not supported in distributed mode with engine `EngineEnum.RAY`:

- boto3_session
:::



:::note
This function has arguments which can be configured globally through *wr.config* or environment variables:

- catalog_id

- concurrent_partitioning

- database

Check out the `Global Configurations Tutorial <https://github.com/aws/aws-sdk-pandas/blob/main/tutorials/021%20-%20Global%20Configurations.ipynb>`_ for details.
:::

**Parameters**

- **`df`** — Pandas DataFrame https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html
- **`path`** — Amazon S3 path (e.g. s3://bucket/prefix/filename.csv) (for dataset e.g. `s3://bucket/prefix`). Required if dataset=False or when creating a new dataset
- **`sep`** — String of length 1. Field delimiter for the output file.
- **`index`** — Write row names (index).
- **`columns`** — Columns to write.
- **`use_threads`** — True to enable concurrent requests, False to disable multiple threads. If enabled os.cpu_count() will be used as the max number of threads. If integer is provided, specified number is used.
- **`boto3_session`** — Boto3 Session. The default boto3 Session will be used if boto3_session receive None.
- **`s3_additional_kwargs`** — Forwarded to botocore requests. e.g. s3_additional_kwargs={'ServerSideEncryption': 'aws:kms', 'SSEKMSKeyId': 'YOUR_KMS_KEY_ARN'}
- **`sanitize_columns`** — True to sanitize columns names or False to keep it as is. True value is forced if `dataset=True`.
- **`dataset`** — If True store as a dataset instead of ordinary file(s) If True, enable all follow arguments: partition_cols, mode, database, table, description, parameters, columns_comments, concurrent_partitioning, catalog_versioning, projection_params, catalog_id, schema_evolution.
- **`filename_prefix`** — If dataset=True, add a filename prefix to the output files.
- **`partition_cols`** — List of column names that will be used to create partitions. Only takes effect if dataset=True.
- **`bucketing_info`** — Tuple consisting of the column names used for bucketing as the first element and the number of buckets as the second element. Only `str`, `int` and `bool` are supported as column data types for bucketing.
- **`concurrent_partitioning`** — If True will increase the parallelism level during the partitions writing. It will decrease the writing time and increase the memory usage. https://aws-sdk-pandas.readthedocs.io/en/3.17.1/tutorials/022%20-%20Writing%20Partitions%20Concurrently.html
- **`mode`** — `append` (Default), `overwrite`, `overwrite_partitions`. Only takes effect if dataset=True. For details check the related tutorial: https://aws-sdk-pandas.readthedocs.io/en/3.17.1/stubs/awswrangler.s3.to_parquet.html#awswrangler.s3.to_parquet
- **`catalog_versioning`** — If True and `mode="overwrite"`, creates an archived version of the table catalog before updating it.
- **`schema_evolution`** — If True allows schema evolution (new or missing columns), otherwise a exception will be raised. (Only considered if dataset=True and mode in ("append", "overwrite_partitions")). False by default. Related tutorial: https://aws-sdk-pandas.readthedocs.io/en/3.17.1/tutorials/014%20-%20Schema%20Evolution.html
- **`database`** — Glue/Athena catalog: Database name.
- **`table`** — Glue/Athena catalog: Table name.
- **`glue_table_settings`** — Settings for writing to the Glue table.
- **`dtype`** — Dictionary of columns names and Athena/Glue types to be casted. Useful when you have columns with undetermined or mixed data types. (e.g. {'col name': 'bigint', 'col2 name': 'int'})
- **`athena_partition_projection_settings`** — Parameters of the Athena Partition Projection (https://docs.aws.amazon.com/athena/latest/ug/partition-projection.html). AthenaPartitionProjectionSettings is a `TypedDict`, meaning the passed parameter can be instantiated either as an instance of AthenaPartitionProjectionSettings or as a regular Python dict. Following projection parameters are supported: .. list-table:: Projection Parameters :header-rows: 1 * - Name - Type - Description * - projection_types - Optional[Dict[str, str]] - Dictionary of partitions names and Athena projections types. Valid types: "enum", "integer", "date", "injected" https://docs.aws.amazon.com/athena/latest/ug/partition-projection-supported-types.html (e.g. {'col_name': 'enum', 'col2_name': 'integer'}) * - projection_ranges - Optional[Dict[str, str]] - Dictionary of partitions names and Athena projections ranges. https://docs.aws.amazon.com/athena/latest/ug/partition-projection-supported-types.html (e.g. {'col_name': '0,10', 'col2_name': '-1,8675309'}) * - projection_values - Optional[Dict[str, str]] - Dictionary of partitions names and Athena projections values. https://docs.aws.amazon.com/athena/latest/ug/partition-projection-supported-types.html (e.g. {'col_name': 'A,B,Unknown', 'col2_name': 'foo,boo,bar'}) * - projection_intervals - Optional[Dict[str, str]] - Dictionary of partitions names and Athena projections intervals. https://docs.aws.amazon.com/athena/latest/ug/partition-projection-supported-types.html (e.g. {'col_name': '1', 'col2_name': '5'}) * - projection_digits - Optional[Dict[str, str]] - Dictionary of partitions names and Athena projections digits. https://docs.aws.amazon.com/athena/latest/ug/partition-projection-supported-types.html (e.g. {'col_name': '1', 'col2_name': '2'}) * - projection_formats - Optional[Dict[str, str]] - Dictionary of partitions names and Athena projections formats. https://docs.aws.amazon.com/athena/latest/ug/partition-projection-supported-types.html (e.g. {'col_date': 'yyyy-MM-dd', 'col2_timestamp': 'yyyy-MM-dd HH:mm:ss'}) * - projection_storage_location_template - Optional[str] - Value which is allows Athena to properly map partition values if the S3 file locations do not follow a typical `.../column=value/...` pattern. https://docs.aws.amazon.com/athena/latest/ug/partition-projection-setting-up.html (e.g. s3://bucket/table_root/a=${a}/${b}/some_static_subdirectory/${c}/)
- **`catalog_id`** — The ID of the Data Catalog from which to retrieve Databases. If none is provided, the AWS account ID is used by default.
- **`pandas_kwargs`** — KEYWORD arguments forwarded to pandas.DataFrame.to_csv(). You can NOT pass `pandas_kwargs` explicit, just add valid Pandas arguments in the function call and awswrangler will accept it. e.g. wr.s3.to_csv(df, path, sep='|', na_rep='NULL', decimal=',') https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.to_csv.html

**Returns**

- Dictionary with: * 'paths': List of all stored files paths on S3. * 'partitions_values': Dictionary of partitions added with keys as S3 path locations and values as a list of partitions values as str.

**Examples**

Writing single file

```python
>>> import awswrangler as wr
>>> import pandas as pd
>>> wr.s3.to_csv(
...     df=pd.DataFrame({'col': [1, 2, 3]}),
...     path='s3://bucket/prefix/my_file.csv',
... )
{
'paths': ['s3://bucket/prefix/my_file.csv'],
'partitions_values': {}
}
```

Writing single file with pandas_kwargs

```python
>>> import awswrangler as wr
>>> import pandas as pd
>>> wr.s3.to_csv(
...     df=pd.DataFrame({'col': [1, 2, 3]}),
...     path='s3://bucket/prefix/my_file.csv',
...     sep='|',
...     na_rep='NULL',
...     decimal=','
... )
{
'paths': ['s3://bucket/prefix/my_file.csv'],
'partitions_values': {}
}
```

Writing single file encrypted with a KMS key

```python
>>> import awswrangler as wr
>>> import pandas as pd
>>> wr.s3.to_csv(
...     df=pd.DataFrame({'col': [1, 2, 3]}),
...     path='s3://bucket/prefix/my_file.csv',
...     s3_additional_kwargs={
...         'ServerSideEncryption': 'aws:kms',
...         'SSEKMSKeyId': 'YOUR_KMS_KEY_ARN'
...     }
... )
{
'paths': ['s3://bucket/prefix/my_file.csv'],
'partitions_values': {}
}
```

Writing partitioned dataset

```python
>>> import awswrangler as wr
>>> import pandas as pd
>>> wr.s3.to_csv(
...     df=pd.DataFrame({
...         'col': [1, 2, 3],
...         'col2': ['A', 'A', 'B']
...     }),
...     path='s3://bucket/prefix',
...     dataset=True,
...     partition_cols=['col2']
... )
{
'paths': ['s3://.../col2=A/x.csv', 's3://.../col2=B/y.csv'],
'partitions_values: {
's3://.../col2=A/': ['A'],
's3://.../col2=B/': ['B']
}
}
```

Writing partitioned dataset with partition projection

```python
>>> import awswrangler as wr
>>> import pandas as pd
>>> from datetime import datetime
>>> dt = lambda x: datetime.strptime(x, "%Y-%m-%d").date()
>>> wr.s3.to_csv(
...     df=pd.DataFrame({
...         "id": [1, 2, 3],
...         "value": [1000, 1001, 1002],
...         "category": ['A', 'B', 'C'],
...     }),
...     path='s3://bucket/prefix',
...     dataset=True,
...     partition_cols=['value', 'category'],
...     athena_partition_projection_settings={
...        "projection_types": {
...             "value": "integer",
...             "category": "enum",
...         },
...         "projection_ranges": {
...             "value": "1000,2000",
...             "category": "A,B,C",
...         },
...     },
... )
{
'paths': [
's3://.../value=1000/category=A/x.json', ...
],
'partitions_values': {
's3://.../value=1000/category=A/': [
'1000',
'A',
], ...
}
}
```

Writing bucketed dataset

```python
>>> import awswrangler as wr
>>> import pandas as pd
>>> wr.s3.to_csv(
...     df=pd.DataFrame({
...         'col': [1, 2, 3],
...         'col2': ['A', 'A', 'B']
...     }),
...     path='s3://bucket/prefix',
...     dataset=True,
...     bucketing_info=(["col2"], 2)
... )
{
'paths': ['s3://.../x_bucket-00000.csv', 's3://.../col2=B/x_bucket-00001.csv'],
'partitions_values: {}
}
```

Writing dataset to S3 with metadata on Athena/Glue Catalog.

```python
>>> import awswrangler as wr
>>> import pandas as pd
>>> wr.s3.to_csv(
...     df=pd.DataFrame({
...         'col': [1, 2, 3],
...         'col2': ['A', 'A', 'B']
...     }),
...     path='s3://bucket/prefix',
...     dataset=True,
...     partition_cols=['col2'],
...     database='default',  # Athena/Glue database
...     table='my_table'  # Athena/Glue table
... )
{
'paths': ['s3://.../col2=A/x.csv', 's3://.../col2=B/y.csv'],
'partitions_values: {
's3://.../col2=A/': ['A'],
's3://.../col2=B/': ['B']
}
}
```

Writing dataset casting empty column data type

```python
>>> import awswrangler as wr
>>> import pandas as pd
>>> wr.s3.to_csv(
...     df=pd.DataFrame({
...         'col': [1, 2, 3],
...         'col2': ['A', 'A', 'B'],
...         'col3': [None, None, None]
...     }),
...     path='s3://bucket/prefix',
...     dataset=True,
...     database='default',  # Athena/Glue database
...     table='my_table'  # Athena/Glue table
...     dtype={'col3': 'date'}
... )
{
'paths': ['s3://.../x.csv'],
'partitions_values: {}
}
```

---

### to_excel

```python
wr.s3.to_excel(
    df: 'pd.DataFrame',
    path: 'str',
    boto3_session: 'boto3.Session | None' = None,
    s3_additional_kwargs: 'dict[str, Any] | None' = None,
    use_threads: 'bool | int' = True,
    **pandas_kwargs: 'Any'
) -> 'str'
```

Write EXCEL file on Amazon S3.

:::note
This function accepts any Pandas's read_excel() argument.
https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.read_excel.html
:::
:::note
Depending on the file extension ('xlsx', 'xls', 'odf'...), an additional library
might have to be installed first.
:::
:::note
In case of `use_threads=True` the number of threads
that will be spawned will be gotten from os.cpu_count().
:::

**Parameters**

- **`df`** — Pandas DataFrame https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html
- **`path`** — Amazon S3 path (e.g. s3://bucket/filename.xlsx).
- **`boto3_session`** — Boto3 Session. The default boto3 Session will be used if boto3_session receive None.
- **`pyarrow_additional_kwargs`** — Forwarded to botocore requests. e.g. s3_additional_kwargs={'ServerSideEncryption': 'aws:kms', 'SSEKMSKeyId': 'YOUR_KMS_KEY_ARN'}
- **`use_threads`** — True to enable concurrent requests, False to disable multiple threads. If enabled os.cpu_count() will be used as the max number of threads. If integer is provided, specified number is used.
- **`pandas_kwargs`** — KEYWORD arguments forwarded to pandas.DataFrame.to_excel(). You can NOT pass `pandas_kwargs` explicit, just add valid Pandas arguments in the function call and awswrangler will accept it. e.g. wr.s3.to_excel(df, path, na_rep="", index=False) https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.to_excel.html

**Returns**

- Written S3 path.

**Examples**

Writing EXCEL file

```python
>>> import awswrangler as wr
>>> import pandas as pd
>>> wr.s3.to_excel(df, 's3://bucket/filename.xlsx')
```

---

### to_json

```python
wr.s3.to_json(
    df: 'pd.DataFrame',
    path: 'str | None' = None,
    index: 'bool' = True,
    columns: 'list[str] | None' = None,
    use_threads: 'bool | int' = True,
    boto3_session: 'boto3.Session | None' = None,
    s3_additional_kwargs: 'dict[str, Any] | None' = None,
    sanitize_columns: 'bool' = False,
    dataset: 'bool' = False,
    filename_prefix: 'str | None' = None,
    partition_cols: 'list[str] | None' = None,
    bucketing_info: 'BucketingInfoTuple | None' = None,
    concurrent_partitioning: 'bool' = False,
    mode: "Literal['append', 'overwrite', 'overwrite_partitions'] | None" = None,
    catalog_versioning: 'bool' = False,
    schema_evolution: 'bool' = True,
    dtype: 'dict[str, str] | None' = None,
    database: 'str | None' = None,
    table: 'str | None' = None,
    glue_table_settings: 'GlueTableSettings | None' = None,
    athena_partition_projection_settings: 'typing.AthenaPartitionProjectionSettings | None' = None,
    catalog_id: 'str | None' = None,
    **pandas_kwargs: 'Any'
) -> '_S3WriteDataReturnValue'
```

Write JSON file on Amazon S3.

:::note
In case of `use_threads=True` the number of threads
that will be spawned will be gotten from os.cpu_count().
:::

:::note
Following arguments are not supported in distributed mode with engine `EngineEnum.RAY`:

- boto3_session
:::



:::note
This function has arguments which can be configured globally through *wr.config* or environment variables:

- catalog_id

- concurrent_partitioning

- database

Check out the `Global Configurations Tutorial <https://github.com/aws/aws-sdk-pandas/blob/main/tutorials/021%20-%20Global%20Configurations.ipynb>`_ for details.
:::

**Parameters**

- **`df`** — Pandas DataFrame https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html
- **`path`** — Amazon S3 path (e.g. s3://bucket/filename.json).
- **`index`** — Write row names (index).
- **`columns`** — Columns to write.
- **`use_threads`** — True to enable concurrent requests, False to disable multiple threads. If enabled os.cpu_count() will be used as the max number of threads. If integer is provided, specified number is used.
- **`boto3_session`** — Boto3 Session. The default boto3 Session will be used if boto3_session receive None.
- **`s3_additional_kwarg`** — Forwarded to botocore requests. e.g. s3_additional_kwargs={'ServerSideEncryption': 'aws:kms', 'SSEKMSKeyId': 'YOUR_KMS_KEY_ARN'}
- **`sanitize_columns`** — True to sanitize columns names or False to keep it as is. True value is forced if `dataset=True`.
- **`dataset`** — If True store as a dataset instead of ordinary file(s) If True, enable all follow arguments: partition_cols, mode, database, table, description, parameters, columns_comments, concurrent_partitioning, catalog_versioning, projection_params, catalog_id, schema_evolution.
- **`filename_prefix`** — If dataset=True, add a filename prefix to the output files.
- **`partition_cols`** — List of column names that will be used to create partitions. Only takes effect if dataset=True.
- **`bucketing_info`** — Tuple consisting of the column names used for bucketing as the first element and the number of buckets as the second element. Only `str`, `int` and `bool` are supported as column data types for bucketing.
- **`concurrent_partitioning`** — If True will increase the parallelism level during the partitions writing. It will decrease the writing time and increase the memory usage. https://aws-sdk-pandas.readthedocs.io/en/3.17.1/tutorials/022%20-%20Writing%20Partitions%20Concurrently.html
- **`mode`** — `append` (Default), `overwrite`, `overwrite_partitions`. Only takes effect if dataset=True. For details check the related tutorial: https://aws-sdk-pandas.readthedocs.io/en/3.17.1/stubs/awswrangler.s3.to_parquet.html#awswrangler.s3.to_parquet
- **`catalog_versioning`** — If True and `mode="overwrite"`, creates an archived version of the table catalog before updating it.
- **`schema_evolution`** — If True allows schema evolution (new or missing columns), otherwise a exception will be raised. (Only considered if dataset=True and mode in ("append", "overwrite_partitions")) Related tutorial: https://aws-sdk-pandas.readthedocs.io/en/3.17.1/tutorials/014%20-%20Schema%20Evolution.html
- **`database`** — Glue/Athena catalog: Database name.
- **`table`** — Glue/Athena catalog: Table name.
- **`glue_table_settings`** — Settings for writing to the Glue table.
- **`dtype`** — Dictionary of columns names and Athena/Glue types to be casted. Useful when you have columns with undetermined or mixed data types. (e.g. {'col name': 'bigint', 'col2 name': 'int'})
- **`athena_partition_projection_settings`** — Parameters of the Athena Partition Projection (https://docs.aws.amazon.com/athena/latest/ug/partition-projection.html). AthenaPartitionProjectionSettings is a `TypedDict`, meaning the passed parameter can be instantiated either as an instance of AthenaPartitionProjectionSettings or as a regular Python dict. Following projection parameters are supported: .. list-table:: Projection Parameters :header-rows: 1 * - Name - Type - Description * - projection_types - Optional[Dict[str, str]] - Dictionary of partitions names and Athena projections types. Valid types: "enum", "integer", "date", "injected" https://docs.aws.amazon.com/athena/latest/ug/partition-projection-supported-types.html (e.g. {'col_name': 'enum', 'col2_name': 'integer'}) * - projection_ranges - Optional[Dict[str, str]] - Dictionary of partitions names and Athena projections ranges. https://docs.aws.amazon.com/athena/latest/ug/partition-projection-supported-types.html (e.g. {'col_name': '0,10', 'col2_name': '-1,8675309'}) * - projection_values - Optional[Dict[str, str]] - Dictionary of partitions names and Athena projections values. https://docs.aws.amazon.com/athena/latest/ug/partition-projection-supported-types.html (e.g. {'col_name': 'A,B,Unknown', 'col2_name': 'foo,boo,bar'}) * - projection_intervals - Optional[Dict[str, str]] - Dictionary of partitions names and Athena projections intervals. https://docs.aws.amazon.com/athena/latest/ug/partition-projection-supported-types.html (e.g. {'col_name': '1', 'col2_name': '5'}) * - projection_digits - Optional[Dict[str, str]] - Dictionary of partitions names and Athena projections digits. https://docs.aws.amazon.com/athena/latest/ug/partition-projection-supported-types.html (e.g. {'col_name': '1', 'col2_name': '2'}) * - projection_formats - Optional[Dict[str, str]] - Dictionary of partitions names and Athena projections formats. https://docs.aws.amazon.com/athena/latest/ug/partition-projection-supported-types.html (e.g. {'col_date': 'yyyy-MM-dd', 'col2_timestamp': 'yyyy-MM-dd HH:mm:ss'}) * - projection_storage_location_template - Optional[str] - Value which is allows Athena to properly map partition values if the S3 file locations do not follow a typical `.../column=value/...` pattern. https://docs.aws.amazon.com/athena/latest/ug/partition-projection-setting-up.html (e.g. s3://bucket/table_root/a=${a}/${b}/some_static_subdirectory/${c}/)
- **`catalog_id`** — The ID of the Data Catalog from which to retrieve Databases. If none is provided, the AWS account ID is used by default.
- **`pandas_kwargs`** — KEYWORD arguments forwarded to pandas.DataFrame.to_json(). You can NOT pass `pandas_kwargs` explicit, just add valid Pandas arguments in the function call and awswrangler will accept it. e.g. wr.s3.to_json(df, path, lines=True, date_format='iso') https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.to_json.html

**Returns**

- Dictionary with: * 'paths': List of all stored files paths on S3. * 'partitions_values': Dictionary of partitions added with keys as S3 path locations and values as a list of partitions values as str.

**Examples**

Writing JSON file

```python
>>> import awswrangler as wr
>>> import pandas as pd
>>> wr.s3.to_json(
...     df=pd.DataFrame({'col': [1, 2, 3]}),
...     path='s3://bucket/filename.json',
... )
```

Writing JSON file using pandas_kwargs

```python
>>> import awswrangler as wr
>>> import pandas as pd
>>> wr.s3.to_json(
...     df=pd.DataFrame({'col': [1, 2, 3]}),
...     path='s3://bucket/filename.json',
...     lines=True,
...     date_format='iso'
... )
```

Writing CSV file encrypted with a KMS key

```python
>>> import awswrangler as wr
>>> import pandas as pd
>>> wr.s3.to_json(
...     df=pd.DataFrame({'col': [1, 2, 3]}),
...     path='s3://bucket/filename.json',
...     s3_additional_kwargs={
...         'ServerSideEncryption': 'aws:kms',
...         'SSEKMSKeyId': 'YOUR_KMS_KEY_ARN'
...     }
... )
```

Writing partitioned dataset with partition projection

```python
>>> import awswrangler as wr
>>> import pandas as pd
>>> from datetime import datetime
>>> dt = lambda x: datetime.strptime(x, "%Y-%m-%d").date()
>>> wr.s3.to_json(
...     df=pd.DataFrame({
...         "id": [1, 2, 3],
...         "value": [1000, 1001, 1002],
...         "category": ['A', 'B', 'C'],
...     }),
...     path='s3://bucket/prefix',
...     dataset=True,
...     partition_cols=['value', 'category'],
...     athena_partition_projection_settings={
...        "projection_types": {
...             "value": "integer",
...             "category": "enum",
...         },
...         "projection_ranges": {
...             "value": "1000,2000",
...             "category": "A,B,C",
...         },
...     },
... )
{
'paths': [
's3://.../value=1000/category=A/x.json', ...
],
'partitions_values': {
's3://.../value=1000/category=A/': [
'1000',
'A',
], ...
}
}
```

---

### to_parquet

```python
wr.s3.to_parquet(
    df: 'pd.DataFrame',
    path: 'str | None' = None,
    index: 'bool' = False,
    compression: 'str | None' = 'snappy',
    pyarrow_additional_kwargs: 'dict[str, Any] | None' = None,
    max_rows_by_file: 'int | None' = None,
    use_threads: 'bool | int' = True,
    boto3_session: 'boto3.Session | None' = None,
    s3_additional_kwargs: 'dict[str, Any] | None' = None,
    sanitize_columns: 'bool' = False,
    dataset: 'bool' = False,
    filename_prefix: 'str | None' = None,
    partition_cols: 'list[str] | None' = None,
    bucketing_info: 'BucketingInfoTuple | None' = None,
    concurrent_partitioning: 'bool' = False,
    mode: "Literal['append', 'overwrite', 'overwrite_partitions'] | None" = None,
    catalog_versioning: 'bool' = False,
    schema_evolution: 'bool' = True,
    database: 'str | None' = None,
    table: 'str | None' = None,
    glue_table_settings: 'GlueTableSettings | None' = None,
    dtype: 'dict[str, str] | None' = None,
    athena_partition_projection_settings: 'typing.AthenaPartitionProjectionSettings | None' = None,
    catalog_id: 'str | None' = None,
    encryption_configuration: 'ArrowEncryptionConfiguration | None' = None
) -> '_S3WriteDataReturnValue'
```

Write Parquet file or dataset on Amazon S3.

The concept of Dataset goes beyond the simple idea of ordinary files and enable more
complex features like partitioning and catalog integration (Amazon Athena/AWS Glue Catalog).

:::note
This operation may mutate the original pandas DataFrame in-place. To avoid this behaviour
please pass in a deep copy instead (i.e. `df.copy()`)
:::
:::note
If `database` and `table` arguments are passed, the table name and all column names
will be automatically sanitized using `wr.catalog.sanitize_table_name` and `wr.catalog.sanitize_column_name`.
Please, pass `sanitize_columns=True` to enforce this behaviour always.
:::
:::note
In case of `use_threads=True` the number of threads
that will be spawned will be gotten from os.cpu_count().
:::

:::note
Following arguments are not supported in distributed mode with engine `EngineEnum.RAY`:

- boto3_session

- s3_additional_kwargs
:::



:::note
This function has arguments which can be configured globally through *wr.config* or environment variables:

- catalog_id

- concurrent_partitioning

- database

Check out the `Global Configurations Tutorial <https://github.com/aws/aws-sdk-pandas/blob/main/tutorials/021%20-%20Global%20Configurations.ipynb>`_ for details.
:::

**Parameters**

- **`df`** — Pandas DataFrame https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html
- **`path`** — S3 path (for file e.g. `s3://bucket/prefix/filename.parquet`) (for dataset e.g. `s3://bucket/prefix`). Required if dataset=False or when dataset=True and creating a new dataset
- **`index`** — True to store the DataFrame index in file, otherwise False to ignore it. Is not supported in conjunction with `max_rows_by_file` when running the library with Ray/Modin.
- **`compression`** — Compression style (`None`, `snappy`, `gzip`, `zstd`).
- **`pyarrow_additional_kwargs`** — Additional parameters forwarded to pyarrow. e.g. pyarrow_additional_kwargs={'coerce_timestamps': 'ns', 'use_deprecated_int96_timestamps': False, 'allow_truncated_timestamps'=False}
- **`max_rows_by_file`** — Max number of rows in each file. Default is None i.e. don't split the files. (e.g. 33554432, 268435456) Is not supported in conjunction with `index=True` when running the library with Ray/Modin.
- **`use_threads`** — True to enable concurrent requests, False to disable multiple threads. If enabled os.cpu_count() will be used as the max number of threads. If integer is provided, specified number is used.
- **`boto3_session`** — Boto3 Session. The default boto3 session will be used if boto3_session receive None.
- **`s3_additional_kwargs`** — Forwarded to botocore requests. e.g. s3_additional_kwargs={'ServerSideEncryption': 'aws:kms', 'SSEKMSKeyId': 'YOUR_KMS_KEY_ARN'}
- **`sanitize_columns`** — True to sanitize columns names (using `wr.catalog.sanitize_table_name` and `wr.catalog.sanitize_column_name`) or False to keep it as is. True value behaviour is enforced if `database` and `table` arguments are passed.
- **`dataset`** — If True store a parquet dataset instead of a ordinary file(s) If True, enable all follow arguments: partition_cols, mode, database, table, description, parameters, columns_comments, concurrent_partitioning, catalog_versioning, projection_params, catalog_id, schema_evolution.
- **`filename_prefix`** — If dataset=True, add a filename prefix to the output files.
- **`partition_cols`** — List of column names that will be used to create partitions. Only takes effect if dataset=True.
- **`bucketing_info`** — Tuple consisting of the column names used for bucketing as the first element and the number of buckets as the second element. Only `str`, `int` and `bool` are supported as column data types for bucketing.
- **`concurrent_partitioning`** — If True will increase the parallelism level during the partitions writing. It will decrease the writing time and increase the memory usage. https://aws-sdk-pandas.readthedocs.io/en/3.17.1/tutorials/022%20-%20Writing%20Partitions%20Concurrently.html
- **`mode`** — `append` (Default), `overwrite`, `overwrite_partitions`. Only takes effect if dataset=True. For details check the related tutorial: https://aws-sdk-pandas.readthedocs.io/en/3.17.1/tutorials/004%20-%20Parquet%20Datasets.html
- **`catalog_versioning`** — If True and `mode="overwrite"`, creates an archived version of the table catalog before updating it.
- **`schema_evolution`** — If True allows schema evolution (new or missing columns), otherwise a exception will be raised. True by default. (Only considered if dataset=True and mode in ("append", "overwrite_partitions")) Related tutorial: https://aws-sdk-pandas.readthedocs.io/en/3.17.1/tutorials/014%20-%20Schema%20Evolution.html
- **`database`** — Glue/Athena catalog: Database name.
- **`table`** — Glue/Athena catalog: Table name.
- **`glue_table_settings`** — Settings for writing to the Glue table.
- **`dtype`** — Dictionary of columns names and Athena/Glue types to be casted. Useful when you have columns with undetermined or mixed data types. (e.g. {'col name': 'bigint', 'col2 name': 'int'})
- **`athena_partition_projection_settings`** — Parameters of the Athena Partition Projection (https://docs.aws.amazon.com/athena/latest/ug/partition-projection.html). AthenaPartitionProjectionSettings is a `TypedDict`, meaning the passed parameter can be instantiated either as an instance of AthenaPartitionProjectionSettings or as a regular Python dict. Following projection parameters are supported: .. list-table:: Projection Parameters :header-rows: 1 * - Name - Type - Description * - projection_types - Optional[Dict[str, str]] - Dictionary of partitions names and Athena projections types. Valid types: "enum", "integer", "date", "injected" https://docs.aws.amazon.com/athena/latest/ug/partition-projection-supported-types.html (e.g. {'col_name': 'enum', 'col2_name': 'integer'}) * - projection_ranges - Optional[Dict[str, str]] - Dictionary of partitions names and Athena projections ranges. https://docs.aws.amazon.com/athena/latest/ug/partition-projection-supported-types.html (e.g. {'col_name': '0,10', 'col2_name': '-1,8675309'}) * - projection_values - Optional[Dict[str, str]] - Dictionary of partitions names and Athena projections values. https://docs.aws.amazon.com/athena/latest/ug/partition-projection-supported-types.html (e.g. {'col_name': 'A,B,Unknown', 'col2_name': 'foo,boo,bar'}) * - projection_intervals - Optional[Dict[str, str]] - Dictionary of partitions names and Athena projections intervals. https://docs.aws.amazon.com/athena/latest/ug/partition-projection-supported-types.html (e.g. {'col_name': '1', 'col2_name': '5'}) * - projection_digits - Optional[Dict[str, str]] - Dictionary of partitions names and Athena projections digits. https://docs.aws.amazon.com/athena/latest/ug/partition-projection-supported-types.html (e.g. {'col_name': '1', 'col2_name': '2'}) * - projection_formats - Optional[Dict[str, str]] - Dictionary of partitions names and Athena projections formats. https://docs.aws.amazon.com/athena/latest/ug/partition-projection-supported-types.html (e.g. {'col_date': 'yyyy-MM-dd', 'col2_timestamp': 'yyyy-MM-dd HH:mm:ss'}) * - projection_storage_location_template - Optional[str] - Value which is allows Athena to properly map partition values if the S3 file locations do not follow a typical `.../column=value/...` pattern. https://docs.aws.amazon.com/athena/latest/ug/partition-projection-setting-up.html (e.g. s3://bucket/table_root/a=${a}/${b}/some_static_subdirectory/${c}/)
- **`catalog_id`** — The ID of the Data Catalog from which to retrieve Databases. If none is provided, the AWS account ID is used by default.
- **`encryption_configuration`** — For Arrow client-side encryption provide materials as follows {'crypto_factory': pyarrow.parquet.encryption.CryptoFactory, 'kms_connection_config': pyarrow.parquet.encryption.KmsConnectionConfig, 'encryption_config': pyarrow.parquet.encryption.EncryptionConfiguration} see: https://arrow.apache.org/docs/python/parquet.html#parquet-modular-encryption-columnar-encryption Client Encryption is not supported in distributed mode.

**Returns**

- Dictionary with: * 'paths': List of all stored files paths on S3. * 'partitions_values': Dictionary of partitions added with keys as S3 path locations and values as a list of partitions values as str.

**Examples**

Writing single file

```python
>>> import awswrangler as wr
>>> import pandas as pd
>>> wr.s3.to_parquet(
...     df=pd.DataFrame({'col': [1, 2, 3]}),
...     path='s3://bucket/prefix/my_file.parquet',
... )
{
'paths': ['s3://bucket/prefix/my_file.parquet'],
'partitions_values': {}
}
```

Writing single file encrypted with a KMS key

```python
>>> import awswrangler as wr
>>> import pandas as pd
>>> wr.s3.to_parquet(
...     df=pd.DataFrame({'col': [1, 2, 3]}),
...     path='s3://bucket/prefix/my_file.parquet',
...     s3_additional_kwargs={
...         'ServerSideEncryption': 'aws:kms',
...         'SSEKMSKeyId': 'YOUR_KMS_KEY_ARN'
...     }
... )
{
'paths': ['s3://bucket/prefix/my_file.parquet'],
'partitions_values': {}
}
```

Writing partitioned dataset

```python
>>> import awswrangler as wr
>>> import pandas as pd
>>> wr.s3.to_parquet(
...     df=pd.DataFrame({
...         'col': [1, 2, 3],
...         'col2': ['A', 'A', 'B']
...     }),
...     path='s3://bucket/prefix',
...     dataset=True,
...     partition_cols=['col2']
... )
{
'paths': ['s3://.../col2=A/x.parquet', 's3://.../col2=B/y.parquet'],
'partitions_values: {
's3://.../col2=A/': ['A'],
's3://.../col2=B/': ['B']
}
}
```

Writing partitioned dataset with partition projection

```python
>>> import awswrangler as wr
>>> import pandas as pd
>>> from datetime import datetime
>>> dt = lambda x: datetime.strptime(x, "%Y-%m-%d").date()
>>> wr.s3.to_parquet(
...     df=pd.DataFrame({
...         "id": [1, 2, 3],
...         "value": [1000, 1001, 1002],
...         "category": ['A', 'B', 'C'],
...     }),
...     path='s3://bucket/prefix',
...     dataset=True,
...     partition_cols=['value', 'category'],
...     athena_partition_projection_settings={
...        "projection_types": {
...             "value": "integer",
...             "category": "enum",
...         },
...         "projection_ranges": {
...             "value": "1000,2000",
...             "category": "A,B,C",
...         },
...     },
... )
{
'paths': [
's3://.../value=1000/category=A/x.snappy.parquet', ...
],
'partitions_values': {
's3://.../value=1000/category=A/': [
'1000',
'A',
], ...
}
}
```

Writing bucketed dataset

```python
>>> import awswrangler as wr
>>> import pandas as pd
>>> wr.s3.to_parquet(
...     df=pd.DataFrame({
...         'col': [1, 2, 3],
...         'col2': ['A', 'A', 'B']
...     }),
...     path='s3://bucket/prefix',
...     dataset=True,
...     bucketing_info=(["col2"], 2)
... )
{
'paths': ['s3://.../x_bucket-00000.csv', 's3://.../col2=B/x_bucket-00001.csv'],
'partitions_values: {}
}
```

Writing dataset to S3 with metadata on Athena/Glue Catalog.

```python
>>> import awswrangler as wr
>>> import pandas as pd
>>> wr.s3.to_parquet(
...     df=pd.DataFrame({
...         'col': [1, 2, 3],
...         'col2': ['A', 'A', 'B']
...     }),
...     path='s3://bucket/prefix',
...     dataset=True,
...     partition_cols=['col2'],
...     database='default',  # Athena/Glue database
...     table='my_table'  # Athena/Glue table
... )
{
'paths': ['s3://.../col2=A/x.parquet', 's3://.../col2=B/y.parquet'],
'partitions_values: {
's3://.../col2=A/': ['A'],
's3://.../col2=B/': ['B']
}
}
```

Writing dataset casting empty column data type

```python
>>> import awswrangler as wr
>>> import pandas as pd
>>> wr.s3.to_parquet(
...     df=pd.DataFrame({
...         'col': [1, 2, 3],
...         'col2': ['A', 'A', 'B'],
...         'col3': [None, None, None]
...     }),
...     path='s3://bucket/prefix',
...     dataset=True,
...     database='default',  # Athena/Glue database
...     table='my_table'  # Athena/Glue table
...     dtype={'col3': 'date'}
... )
{
'paths': ['s3://.../x.parquet'],
'partitions_values: {}
}
```

---

### to_orc

```python
wr.s3.to_orc(
    df: 'pd.DataFrame',
    path: 'str | None' = None,
    index: 'bool' = False,
    compression: 'str | None' = None,
    pyarrow_additional_kwargs: 'dict[str, Any] | None' = None,
    max_rows_by_file: 'int | None' = None,
    use_threads: 'bool | int' = True,
    boto3_session: 'boto3.Session | None' = None,
    s3_additional_kwargs: 'dict[str, Any] | None' = None,
    sanitize_columns: 'bool' = False,
    dataset: 'bool' = False,
    filename_prefix: 'str | None' = None,
    partition_cols: 'list[str] | None' = None,
    bucketing_info: 'BucketingInfoTuple | None' = None,
    concurrent_partitioning: 'bool' = False,
    mode: "Literal['append', 'overwrite', 'overwrite_partitions'] | None" = None,
    catalog_versioning: 'bool' = False,
    schema_evolution: 'bool' = True,
    database: 'str | None' = None,
    table: 'str | None' = None,
    glue_table_settings: 'GlueTableSettings | None' = None,
    dtype: 'dict[str, str] | None' = None,
    athena_partition_projection_settings: 'typing.AthenaPartitionProjectionSettings | None' = None,
    catalog_id: 'str | None' = None
) -> '_S3WriteDataReturnValue'
```

Write ORC file or dataset on Amazon S3.

The concept of Dataset goes beyond the simple idea of ordinary files and enable more
complex features like partitioning and catalog integration (Amazon Athena/AWS Glue Catalog).

:::note
This operation may mutate the original pandas DataFrame in-place. To avoid this behaviour
please pass in a deep copy instead (i.e. `df.copy()`)
:::
:::note
If `database` and `table` arguments are passed, the table name and all column names
will be automatically sanitized using `wr.catalog.sanitize_table_name` and `wr.catalog.sanitize_column_name`.
Please, pass `sanitize_columns=True` to enforce this behaviour always.
:::
:::note
In case of `use_threads=True` the number of threads
that will be spawned will be gotten from os.cpu_count().
:::

:::note
This function has arguments which can be configured globally through *wr.config* or environment variables:

- catalog_id

- concurrent_partitioning

- database

Check out the `Global Configurations Tutorial <https://github.com/aws/aws-sdk-pandas/blob/main/tutorials/021%20-%20Global%20Configurations.ipynb>`_ for details.
:::



:::note
Following arguments are not supported in distributed mode with engine `EngineEnum.RAY`:

- boto3_session

- s3_additional_kwargs
:::

**Parameters**

- **`df`** — Pandas DataFrame https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html
- **`path`** — S3 path (for file e.g. `s3://bucket/prefix/filename.orc`) (for dataset e.g. `s3://bucket/prefix`). Required if dataset=False or when dataset=True and creating a new dataset
- **`index`** — True to store the DataFrame index in file, otherwise False to ignore it. Is not supported in conjunction with `max_rows_by_file` when running the library with Ray/Modin.
- **`compression`** — Compression style (`None`, `snappy`, `gzip`, `zstd`).
- **`pyarrow_additional_kwargs`** — Additional parameters forwarded to pyarrow. e.g. pyarrow_additional_kwargs={'coerce_timestamps': 'ns', 'use_deprecated_int96_timestamps': False, 'allow_truncated_timestamps'=False}
- **`max_rows_by_file`** — Max number of rows in each file. Default is None i.e. don't split the files. (e.g. 33554432, 268435456) Is not supported in conjunction with `index=True` when running the library with Ray/Modin.
- **`use_threads`** — True to enable concurrent requests, False to disable multiple threads. If enabled os.cpu_count() will be used as the max number of threads. If integer is provided, specified number is used.
- **`boto3_session`** — Boto3 Session. The default boto3 session will be used if boto3_session receive None.
- **`s3_additional_kwargs: dict[str, Any], optional`** — Forwarded to botocore requests. e.g. s3_additional_kwargs={'ServerSideEncryption': 'aws:kms', 'SSEKMSKeyId': 'YOUR_KMS_KEY_ARN'}
- **`sanitize_columns`** — True to sanitize columns names (using `wr.catalog.sanitize_table_name` and `wr.catalog.sanitize_column_name`) or False to keep it as is. True value behaviour is enforced if `database` and `table` arguments are passed.
- **`dataset`** — If True store a orc dataset instead of a ordinary file(s) If True, enable all follow arguments: partition_cols, mode, database, table, description, parameters, columns_comments, concurrent_partitioning, catalog_versioning, projection_params, catalog_id, schema_evolution.
- **`filename_prefix`** — If dataset=True, add a filename prefix to the output files.
- **`partition_cols`** — List of column names that will be used to create partitions. Only takes effect if dataset=True.
- **`bucketing_info`** — Tuple consisting of the column names used for bucketing as the first element and the number of buckets as the second element. Only `str`, `int` and `bool` are supported as column data types for bucketing.
- **`concurrent_partitioning`** — If True will increase the parallelism level during the partitions writing. It will decrease the writing time and increase the memory usage. https://aws-sdk-pandas.readthedocs.io/en/3.17.1/tutorials/022%20-%20Writing%20Partitions%20Concurrently.html
- **`mode`** — `append` (Default), `overwrite`, `overwrite_partitions`. Only takes effect if dataset=True.
- **`catalog_versioning`** — If True and `mode="overwrite"`, creates an archived version of the table catalog before updating it.
- **`schema_evolution`** — If True allows schema evolution (new or missing columns), otherwise a exception will be raised. True by default. (Only considered if dataset=True and mode in ("append", "overwrite_partitions")) Related tutorial: https://aws-sdk-pandas.readthedocs.io/en/3.17.1/tutorials/014%20-%20Schema%20Evolution.html
- **`database`** — Glue/Athena catalog: Database name.
- **`table`** — Glue/Athena catalog: Table name.
- **`glue_table_settings`** — Settings for writing to the Glue table.
- **`dtype`** — Dictionary of columns names and Athena/Glue types to be casted. Useful when you have columns with undetermined or mixed data types. (e.g. {'col name': 'bigint', 'col2 name': 'int'})
- **`athena_partition_projection_settings`** — Parameters of the Athena Partition Projection (https://docs.aws.amazon.com/athena/latest/ug/partition-projection.html). AthenaPartitionProjectionSettings is a `TypedDict`, meaning the passed parameter can be instantiated either as an instance of AthenaPartitionProjectionSettings or as a regular Python dict. Following projection parameters are supported: .. list-table:: Projection Parameters :header-rows: 1 * - Name - Type - Description * - projection_types - Optional[Dict[str, str]] - Dictionary of partitions names and Athena projections types. Valid types: "enum", "integer", "date", "injected" https://docs.aws.amazon.com/athena/latest/ug/partition-projection-supported-types.html (e.g. {'col_name': 'enum', 'col2_name': 'integer'}) * - projection_ranges - Optional[Dict[str, str]] - Dictionary of partitions names and Athena projections ranges. https://docs.aws.amazon.com/athena/latest/ug/partition-projection-supported-types.html (e.g. {'col_name': '0,10', 'col2_name': '-1,8675309'}) * - projection_values - Optional[Dict[str, str]] - Dictionary of partitions names and Athena projections values. https://docs.aws.amazon.com/athena/latest/ug/partition-projection-supported-types.html (e.g. {'col_name': 'A,B,Unknown', 'col2_name': 'foo,boo,bar'}) * - projection_intervals - Optional[Dict[str, str]] - Dictionary of partitions names and Athena projections intervals. https://docs.aws.amazon.com/athena/latest/ug/partition-projection-supported-types.html (e.g. {'col_name': '1', 'col2_name': '5'}) * - projection_digits - Optional[Dict[str, str]] - Dictionary of partitions names and Athena projections digits. https://docs.aws.amazon.com/athena/latest/ug/partition-projection-supported-types.html (e.g. {'col_name': '1', 'col2_name': '2'}) * - projection_formats - Optional[Dict[str, str]] - Dictionary of partitions names and Athena projections formats. https://docs.aws.amazon.com/athena/latest/ug/partition-projection-supported-types.html (e.g. {'col_date': 'yyyy-MM-dd', 'col2_timestamp': 'yyyy-MM-dd HH:mm:ss'}) * - projection_storage_location_template - Optional[str] - Value which is allows Athena to properly map partition values if the S3 file locations do not follow a typical `.../column=value/...` pattern. https://docs.aws.amazon.com/athena/latest/ug/partition-projection-setting-up.html (e.g. s3://bucket/table_root/a=${a}/${b}/some_static_subdirectory/${c}/)
- **`catalog_id`** — The ID of the Data Catalog from which to retrieve Databases. If none is provided, the AWS account ID is used by default.

**Returns**

- Dictionary with: * 'paths': List of all stored files paths on S3. * 'partitions_values': Dictionary of partitions added with keys as S3 path locations and values as a list of partitions values as str.

**Examples**

Writing single file

```python
>>> import awswrangler as wr
>>> import pandas as pd
>>> wr.s3.to_orc(
...     df=pd.DataFrame({'col': [1, 2, 3]}),
...     path='s3://bucket/prefix/my_file.orc',
... )
{
'paths': ['s3://bucket/prefix/my_file.orc'],
'partitions_values': {}
}
```

Writing single file encrypted with a KMS key

```python
>>> import awswrangler as wr
>>> import pandas as pd
>>> wr.s3.to_orc(
...     df=pd.DataFrame({'col': [1, 2, 3]}),
...     path='s3://bucket/prefix/my_file.orc',
...     s3_additional_kwargs={
...         'ServerSideEncryption': 'aws:kms',
...         'SSEKMSKeyId': 'YOUR_KMS_KEY_ARN'
...     }
... )
{
'paths': ['s3://bucket/prefix/my_file.orc'],
'partitions_values': {}
}
```

Writing partitioned dataset

```python
>>> import awswrangler as wr
>>> import pandas as pd
>>> wr.s3.to_orc(
...     df=pd.DataFrame({
...         'col': [1, 2, 3],
...         'col2': ['A', 'A', 'B']
...     }),
...     path='s3://bucket/prefix',
...     dataset=True,
...     partition_cols=['col2']
... )
{
'paths': ['s3://.../col2=A/x.orc', 's3://.../col2=B/y.orc'],
'partitions_values: {
's3://.../col2=A/': ['A'],
's3://.../col2=B/': ['B']
}
}
```

Writing partitioned dataset with partition projection

```python
>>> import awswrangler as wr
>>> import pandas as pd
>>> from datetime import datetime
>>> dt = lambda x: datetime.strptime(x, "%Y-%m-%d").date()
>>> wr.s3.to_orc(
...     df=pd.DataFrame({
...         "id": [1, 2, 3],
...         "value": [1000, 1001, 1002],
...         "category": ['A', 'B', 'C'],
...     }),
...     path='s3://bucket/prefix',
...     dataset=True,
...     partition_cols=['value', 'category'],
...     athena_partition_projection_settings={
...        "projection_types": {
...             "value": "integer",
...             "category": "enum",
...         },
...         "projection_ranges": {
...             "value": "1000,2000",
...             "category": "A,B,C",
...         },
...     },
... )
{
'paths': [
's3://.../value=1000/category=A/x.snappy.orc', ...
],
'partitions_values': {
's3://.../value=1000/category=A/': [
'1000',
'A',
], ...
}
}
```

Writing bucketed dataset

```python
>>> import awswrangler as wr
>>> import pandas as pd
>>> wr.s3.to_orc(
...     df=pd.DataFrame({
...         'col': [1, 2, 3],
...         'col2': ['A', 'A', 'B']
...     }),
...     path='s3://bucket/prefix',
...     dataset=True,
...     bucketing_info=(["col2"], 2)
... )
{
'paths': ['s3://.../x_bucket-00000.csv', 's3://.../col2=B/x_bucket-00001.csv'],
'partitions_values: {}
}
```

Writing dataset to S3 with metadata on Athena/Glue Catalog.

```python
>>> import awswrangler as wr
>>> import pandas as pd
>>> wr.s3.to_orc(
...     df=pd.DataFrame({
...         'col': [1, 2, 3],
...         'col2': ['A', 'A', 'B']
...     }),
...     path='s3://bucket/prefix',
...     dataset=True,
...     partition_cols=['col2'],
...     database='default',  # Athena/Glue database
...     table='my_table'  # Athena/Glue table
... )
{
'paths': ['s3://.../col2=A/x.orc', 's3://.../col2=B/y.orc'],
'partitions_values: {
's3://.../col2=A/': ['A'],
's3://.../col2=B/': ['B']
}
}
```

---

### to_deltalake

```python
wr.s3.to_deltalake(
    df: 'pd.DataFrame',
    path: 'str',
    index: 'bool' = False,
    mode: "Literal['error', 'append', 'overwrite', 'ignore']" = 'append',
    dtype: 'dict[str, str] | None' = None,
    partition_cols: 'list[str] | None' = None,
    schema_mode: "Literal['overwrite'] | None" = None,
    lock_dynamodb_table: 'str | None' = None,
    s3_allow_unsafe_rename: 'bool' = False,
    boto3_session: 'boto3.Session | None' = None,
    s3_additional_kwargs: 'dict[str, str] | None' = None
) -> 'None'
```

Write a DataFrame to S3 as a DeltaLake table.

This function requires the `deltalake package
<https://delta-io.github.io/delta-rs/python>`__.


:::warning
This API is experimental and may change in future AWS SDK for Pandas releases.
:::

**Parameters**

- **`df`** — `Pandas DataFrame <https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html>`_
- **`path`** — S3 path for a directory where the DeltaLake table will be stored.
- **`index`** — True to store the DataFrame index in file, otherwise False to ignore it.
- **`mode`** — `append` (Default), `overwrite`, `ignore`, `error`
- **`dtype`** — Dictionary of columns names and Athena/Glue types to be casted. Useful when you have columns with undetermined or mixed data types. (e.g. `{'col name':'bigint', 'col2 name': 'int'})`
- **`partition_cols`** — List of columns to partition the table by. Only required when creating a new table.
- **`schema_mode`** — If set to "overwrite", allows replacing the schema of the table. Set to "merge" to merge with existing schema.
- **`lock_dynamodb_table`** — DynamoDB table to use as a locking provider. A locking mechanism is needed to prevent unsafe concurrent writes to a delta lake directory when writing to S3. If you don't want to use a locking mechanism, you can choose to set `s3_allow_unsafe_rename` to True. For information on how to set up the lock table, please check `this page <https://delta-io.github.io/delta-rs/usage/writing/writing-to-s3-with-locking-provider/#dynamodb>`_.
- **`s3_allow_unsafe_rename`** — Allows using the default S3 backend without support for concurrent writers.
- **`boto3_session`** — If None, the default boto3 session is used.
- **`pyarrow_additional_kwargs`** — Forwarded to the Delta Table class for the storage options of the S3 backend.

**Examples**

Writing a Pandas DataFrame into a DeltaLake table in S3.

```python
>>> import awswrangler as wr
>>> import pandas as pd
>>> wr.s3.to_deltalake(
...     df=pd.DataFrame({"col": [1, 2, 3]}),
...     path="s3://bucket/prefix/",
...     lock_dynamodb_table="my-lock-table",
... )
```

**See Also**

deltalake.DeltaTable: Create a DeltaTable instance with the deltalake library.
deltalake.write_deltalake: Write to a DeltaLake table.

---

### upload

```python
wr.s3.upload(
    local_file: 'str | Any',
    path: 'str',
    use_threads: 'bool | int' = True,
    boto3_session: 'boto3.Session | None' = None,
    s3_additional_kwargs: 'dict[str, Any] | None' = None
) -> 'None'
```

Upload file from a local file to received S3 path.

:::note
In case of `use_threads=True` the number of threads
that will be spawned will be gotten from os.cpu_count().
:::

**Parameters**

- **`local_file`** — A file-like object in binary mode or a path to local file (e.g. `./local/path/to/key0`).
- **`path`** — S3 path (e.g. `s3://bucket/key0`).
- **`use_threads`** — True to enable concurrent requests, False to disable multiple threads. If enabled os.cpu_count() will be used as the max number of threads. If integer is provided, specified number is used.
- **`boto3_session`** — The default boto3 session will be used if boto3_session receive None.
- **`pyarrow_additional_kwargs`** — Forward to botocore requests, only "SSECustomerAlgorithm" and "SSECustomerKey" arguments will be considered.

**Returns**

- None

**Examples**

Uploading a file using a path to local file

```python
>>> import awswrangler as wr
>>> wr.s3.upload(local_file='./key', path='s3://bucket/key')
```

Uploading a file using a file-like object

```python
>>> import awswrangler as wr
>>> with open(file='./key', mode='wb') as local_f:
>>>     wr.s3.upload(local_file=local_f, path='s3://bucket/key')
```

---

### wait_objects_exist

```python
wr.s3.wait_objects_exist(
    paths: 'list[str]',
    delay: 'float | None' = None,
    max_attempts: 'int | None' = None,
    use_threads: 'bool | int' = True,
    boto3_session: 'boto3.Session | None' = None
) -> 'None'
```

Wait Amazon S3 objects exist.

Polls S3.Client.head_object() every 5 seconds (default) until a successful
state is reached. An error is returned after 20 (default) failed checks.
https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Waiter.ObjectExists

:::note
In case of `use_threads=True` the number of threads
that will be spawned will be gotten from os.cpu_count().
:::

:::note
Following arguments are not supported in distributed mode with engine `EngineEnum.RAY`:

- boto3_session
:::

**Parameters**

- **`paths`** — List of S3 objects paths (e.g. [s3://bucket/key0, s3://bucket/key1]).
- **`delay`** — The amount of time in seconds to wait between attempts. Default: 5
- **`max_attempts`** — The maximum number of attempts to be made. Default: 20
- **`use_threads`** — True to enable concurrent requests, False to disable multiple threads. If enabled os.cpu_count() will be used as the max number of threads. If integer is provided, specified number is used.
- **`boto3_session`** — The default boto3 session will be used if boto3_session receive None.

**Returns**

- None

**Examples**

```python
>>> import awswrangler as wr
>>> wr.s3.wait_objects_exist(['s3://bucket/key0', 's3://bucket/key1'])  # wait both objects
```

---

### wait_objects_not_exist

```python
wr.s3.wait_objects_not_exist(
    paths: 'list[str]',
    delay: 'float | None' = None,
    max_attempts: 'int | None' = None,
    use_threads: 'bool | int' = True,
    boto3_session: 'boto3.Session | None' = None
) -> 'None'
```

Wait Amazon S3 objects not exist.

Polls S3.Client.head_object() every 5 seconds (default) until a successful
state is reached. An error is returned after 20 (default) failed checks.
https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Waiter.ObjectNotExists

:::note
In case of `use_threads=True` the number of threads
that will be spawned will be gotten from os.cpu_count().
:::

:::note
Following arguments are not supported in distributed mode with engine `EngineEnum.RAY`:

- boto3_session
:::

**Parameters**

- **`paths`** — List of S3 objects paths (e.g. [s3://bucket/key0, s3://bucket/key1]).
- **`delay`** — The amount of time in seconds to wait between attempts. Default: 5
- **`max_attempts`** — The maximum number of attempts to be made. Default: 20
- **`use_threads`** — True to enable concurrent requests, False to disable multiple threads. If enabled os.cpu_count() will be used as the max number of threads. If integer is provided, specified number is used.
- **`boto3_session`** — The default boto3 session will be used if boto3_session receive None.

**Returns**

- None

**Examples**

```python
>>> import awswrangler as wr
>>> wr.s3.wait_objects_not_exist(['s3://bucket/key0', 's3://bucket/key1'])  # wait both objects not exist
```

---
