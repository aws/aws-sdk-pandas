---
id: amazon-s3-tables
title: "Amazon S3 Tables"
sidebar_position: 13
---

# Amazon S3 Tables

Module: `wr.s3`

### create_table_bucket

```python
wr.s3.create_table_bucket(name: 'str', boto3_session: 'boto3.Session | None' = None) -> 'str'
```

Create an S3 Table Bucket.

**Parameters**

- **`name : str`** — The name of the table bucket to create.
- **`boto3_session : boto3.Session, optional`** — Boto3 Session. If None, the default boto3 session is used.

**Returns**

- str The ARN of the created table bucket.

**Examples**

```python
>>> import awswrangler as wr
>>> arn = wr.s3.create_table_bucket(name="my-table-bucket")
```

---

### create_namespace

```python
wr.s3.create_namespace(
    table_bucket_arn: 'str',
    namespace: 'str',
    boto3_session: 'boto3.Session | None' = None
) -> 'str'
```

Create a namespace in an S3 Table Bucket.

**Parameters**

- **`table_bucket_arn : str`** — The ARN of the table bucket.
- **`namespace : str`** — The name of the namespace to create.
- **`boto3_session : boto3.Session, optional`** — Boto3 Session. If None, the default boto3 session is used.

**Returns**

- str The namespace name.

**Examples**

```python
>>> import awswrangler as wr
>>> ns = wr.s3.create_namespace(
...     table_bucket_arn="arn:aws:s3tables:us-east-1:123456789012:bucket/my-bucket",
...     namespace="my_namespace",
... )
```

---

### create_table

```python
wr.s3.create_table(
    table_bucket_arn: 'str',
    namespace: 'str',
    table_name: 'str',
    format: 'str' = 'ICEBERG',
    boto3_session: 'boto3.Session | None' = None
) -> 'str'
```

Create a table in an S3 Table Bucket namespace.

**Parameters**

- **`table_bucket_arn : str`** — The ARN of the table bucket.
- **`namespace : str`** — The namespace in which to create the table.
- **`table_name : str`** — The name of the table to create.
- **`format : str, optional`** — The table format. Default is `"ICEBERG"`.
- **`boto3_session : boto3.Session, optional`** — Boto3 Session. If None, the default boto3 session is used.

**Returns**

- str The ARN of the created table.

**Examples**

```python
>>> import awswrangler as wr
>>> table_arn = wr.s3.create_table(
...     table_bucket_arn="arn:aws:s3tables:us-east-1:123456789012:bucket/my-bucket",
...     namespace="my_namespace",
...     table_name="my_table",
... )
```

---

### delete_table_bucket

```python
wr.s3.delete_table_bucket(
    table_bucket_arn: 'str',
    boto3_session: 'boto3.Session | None' = None
) -> 'None'
```

Delete an S3 Table Bucket.

**Parameters**

- **`table_bucket_arn : str`** — The ARN of the table bucket to delete.
- **`boto3_session : boto3.Session, optional`** — Boto3 Session. If None, the default boto3 session is used.

**Examples**

```python
>>> import awswrangler as wr
>>> wr.s3.delete_table_bucket(
...     table_bucket_arn="arn:aws:s3tables:us-east-1:123456789012:bucket/my-bucket",
... )
```

---

### delete_namespace

```python
wr.s3.delete_namespace(
    table_bucket_arn: 'str',
    namespace: 'str',
    boto3_session: 'boto3.Session | None' = None
) -> 'None'
```

Delete a namespace from an S3 Table Bucket.

**Parameters**

- **`table_bucket_arn : str`** — The ARN of the table bucket.
- **`namespace : str`** — The name of the namespace to delete.
- **`boto3_session : boto3.Session, optional`** — Boto3 Session. If None, the default boto3 session is used.

**Examples**

```python
>>> import awswrangler as wr
>>> wr.s3.delete_namespace(
...     table_bucket_arn="arn:aws:s3tables:us-east-1:123456789012:bucket/my-bucket",
...     namespace="my_namespace",
... )
```

---

### delete_table

```python
wr.s3.delete_table(
    table_bucket_arn: 'str',
    namespace: 'str',
    table_name: 'str',
    version_token: 'str | None' = None,
    boto3_session: 'boto3.Session | None' = None
) -> 'None'
```

Delete a table from an S3 Table Bucket namespace.

**Parameters**

- **`table_bucket_arn : str`** — The ARN of the table bucket.
- **`namespace : str`** — The namespace of the table.
- **`table_name : str`** — The name of the table to delete.
- **`version_token : str, optional`** — The version token of the table. If not provided, the current version is deleted.
- **`boto3_session : boto3.Session, optional`** — Boto3 Session. If None, the default boto3 session is used.

**Examples**

```python
>>> import awswrangler as wr
>>> wr.s3.delete_table(
...     table_bucket_arn="arn:aws:s3tables:us-east-1:123456789012:bucket/my-bucket",
...     namespace="my_namespace",
...     table_name="my_table",
... )
```

---

### from_iceberg

```python
wr.s3.from_iceberg(
    table_bucket_arn: 'str',
    namespace: 'str',
    table_name: 'str',
    columns: 'list[str] | None' = None,
    row_filter: 'str | None' = None,
    snapshot_id: 'int | None' = None,
    limit: 'int | None' = None,
    dtype_backend: "Literal['numpy_nullable', 'pyarrow']" = 'numpy_nullable',
    boto3_session: 'boto3.Session | None' = None,
    pyarrow_additional_kwargs: 'dict[str, Any] | None' = None
) -> 'pd.DataFrame'
```

Read an S3 Table into a Pandas DataFrame via PyIceberg.

This function requires the `pyiceberg` package.
Install it with `pip install awswrangler[pyiceberg]`.

By default, the S3 Tables REST endpoint is used. To use the AWS Glue
Iceberg REST endpoint instead, set
`wr.config.s3tables_catalog_endpoint_url`
(e.g. `"https://glue.<region>.amazonaws.com/iceberg"`).
See `Integrating S3 Tables with AWS analytics services
<https://docs.aws.amazon.com/AmazonS3/latest/userguide/s3-tables-integrating-aws.html>`_
for the required Glue Data Catalog and Lake Formation setup.


:::note
This function has arguments which can be configured globally through *wr.config* or environment variables:

- dtype_backend

Check out the `Global Configurations Tutorial <https://github.com/aws/aws-sdk-pandas/blob/main/tutorials/021%20-%20Global%20Configurations.ipynb>`_ for details.
:::

**Parameters**

- **`table_bucket_arn : str`** — The ARN of the S3 table bucket.
- **`namespace : str`** — The namespace of the table.
- **`table_name : str`** — The name of the table to read.
- **`columns : list[str], optional`** — List of column names to read. If None, all columns are read.
- **`row_filter : str, optional`** — A row filter expression (e.g. `"col > 5"`). If None, all rows are read.
- **`snapshot_id : int, optional`** — A specific snapshot ID to read. If None, the latest snapshot is read.
- **`limit : int, optional`** — Maximum number of rows to return. If None, all rows are returned.
- **`dtype_backend : str, optional`** — Which dtype_backend to use. `"numpy_nullable"` or `"pyarrow"`.
- **`boto3_session : boto3.Session, optional`** — Boto3 Session. If None, the default boto3 session is used.
- **`pyarrow_additional_kwargs : dict[str, Any], optional`** — Additional keyword arguments forwarded to PyArrow's `to_pandas()` method.

**Returns**

- pd.DataFrame DataFrame with the table data.

**Examples**

Reading an entire table:

```python
>>> import awswrangler as wr
>>> df = wr.s3.from_iceberg(
...     table_bucket_arn="arn:aws:s3tables:us-east-1:123456789012:bucket/my-bucket",
...     namespace="my_namespace",
...     table_name="my_table",
... )
```

Reading with row filtering and limit:

```python
>>> df = wr.s3.from_iceberg(
...     table_bucket_arn="arn:aws:s3tables:us-east-1:123456789012:bucket/my-bucket",
...     namespace="my_namespace",
...     table_name="my_table",
...     row_filter="amount > 50.0",
...     limit=100,
... )
```

Reading via the Glue Iceberg REST endpoint:

```python
>>> wr.config.s3tables_catalog_endpoint_url = "https://glue.us-east-1.amazonaws.com/iceberg"
>>> df = wr.s3.from_iceberg(
...     table_bucket_arn="arn:aws:s3tables:us-east-1:123456789012:bucket/my-bucket",
...     namespace="my_namespace",
...     table_name="my_table",
... )
```

---

### to_iceberg

```python
wr.s3.to_iceberg(
    df: 'pd.DataFrame',
    table_bucket_arn: 'str',
    namespace: 'str',
    table_name: 'str',
    mode: "Literal['append', 'overwrite']" = 'append',
    index: 'bool' = False,
    dtype: 'dict[str, str] | None' = None,
    boto3_session: 'boto3.Session | None' = None
) -> 'None'
```

Write a Pandas DataFrame to an S3 Table via PyIceberg.

If the table does not exist, it is automatically created with a schema
inferred from the DataFrame.

This function requires the `pyiceberg` package.
Install it with `pip install awswrangler[pyiceberg]`.

By default, the S3 Tables REST endpoint is used. To use the AWS Glue
Iceberg REST endpoint instead, set
`wr.config.s3tables_catalog_endpoint_url`
(e.g. `"https://glue.<region>.amazonaws.com/iceberg"`).
See `Integrating S3 Tables with AWS analytics services
<https://docs.aws.amazon.com/AmazonS3/latest/userguide/s3-tables-integrating-aws.html>`_
for the required Glue Data Catalog and Lake Formation setup.

**Parameters**

- **`df : pd.DataFrame`** — Pandas DataFrame to write.
- **`table_bucket_arn : str`** — The ARN of the S3 table bucket.
- **`namespace : str`** — The namespace of the table.
- **`table_name : str`** — The name of the table to write to.
- **`mode : str, optional`** — Write mode. `"append"` (default) adds rows to the table. `"overwrite"` replaces all existing data.
- **`index : bool, optional`** — If True, include the DataFrame index as a column. Default is False.
- **`dtype : dict[str, str], optional`** — Dictionary of column names and Athena/Glue types to cast. (e.g. `{"col_name": "bigint", "col2_name": "int"}`).
- **`boto3_session : boto3.Session, optional`** — Boto3 Session. If None, the default boto3 session is used.

**Examples**

```python
>>> import awswrangler as wr
>>> import pandas as pd
>>> wr.s3.to_iceberg(
...     df=pd.DataFrame({"col": [1, 2, 3]}),
...     table_bucket_arn="arn:aws:s3tables:us-east-1:123456789012:bucket/my-bucket",
...     namespace="my_namespace",
...     table_name="my_table",
... )
```

---
