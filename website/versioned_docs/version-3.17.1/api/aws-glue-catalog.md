---
id: aws-glue-catalog
title: "AWS Glue Catalog"
sidebar_position: 2
---

# AWS Glue Catalog

Module: `wr.catalog`

### add_column

```python
wr.catalog.add_column(
    database: 'str',
    table: 'str',
    column_name: 'str',
    column_type: 'str' = 'string',
    column_comment: 'str | None' = None,
    boto3_session: 'boto3.Session | None' = None,
    catalog_id: 'str | None' = None
) -> 'None'
```

Add a column in a AWS Glue Catalog table.


:::note
This function has arguments which can be configured globally through *wr.config* or environment variables:

- catalog_id

- database

Check out the `Global Configurations Tutorial <https://github.com/aws/aws-sdk-pandas/blob/main/tutorials/021%20-%20Global%20Configurations.ipynb>`_ for details.
:::

**Parameters**

- **`database`** — Database name.
- **`table`** — Table name.
- **`column_name`** — Column name
- **`column_type`** — Column type.
- **`column_comment`** — Column Comment
- **`boto3_session`** — The default boto3 session will be used if **boto3_session** is `None`.
- **`catalog_id`** — The ID of the Data Catalog from which to retrieve Databases. If none is provided, the AWS account ID is used by default.

**Examples**

```python
>>> import awswrangler as wr
>>> wr.catalog.add_column(
...     database='my_db',
...     table='my_table',
...     column_name='my_col',
...     column_type='int'
... )
```

---

### add_csv_partitions

```python
wr.catalog.add_csv_partitions(
    database: 'str',
    table: 'str',
    partitions_values: 'dict[str,
    list[str]]',
    bucketing_info: 'typing.BucketingInfoTuple | None' = None,
    catalog_id: 'str | None' = None,
    compression: 'str | None' = None,
    sep: 'str' = ',',
    serde_library: 'str | None' = None,
    serde_parameters: 'dict[str, str] | None' = None,
    boto3_session: 'boto3.Session | None' = None,
    columns_types: 'dict[str, str] | None' = None,
    partitions_parameters: 'dict[str, str] | None' = None
) -> 'None'
```

Add partitions (metadata) to a CSV Table in the AWS Glue Catalog.


:::note
This function has arguments which can be configured globally through *wr.config* or environment variables:

- catalog_id

- database

Check out the `Global Configurations Tutorial <https://github.com/aws/aws-sdk-pandas/blob/main/tutorials/021%20-%20Global%20Configurations.ipynb>`_ for details.
:::

**Parameters**

- **`database`** — Database name.
- **`table`** — Table name.
- **`partitions_values`** — Dictionary with keys as S3 path locations and values as a list of partitions values as str (e.g. {'s3://bucket/prefix/y=2020/m=10/': ['2020', '10']}).
- **`bucketing_info`** — Tuple consisting of the column names used for bucketing as the first element and the number of buckets as the second element. Only `str`, `int` and `bool` are supported as column data types for bucketing.
- **`catalog_id`** — The ID of the Data Catalog from which to retrieve Databases. If none is provided, the AWS account ID is used by default.
- **`compression`** — Compression style (`None`, `gzip`, etc).
- **`sep`** — String of length 1. Field delimiter for the output file.
- **`serde_library`** — Specifies the SerDe Serialization library which will be used. You need to provide the Class library name as a string. If no library is provided the default is `org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe`.
- **`serde_parameters`** — Dictionary of initialization parameters for the SerDe. The default is `{"field.delim": sep, "escape.delim": "\\"}`.
- **`boto3_session`** — The default boto3 session will be used if boto3_session receive None.
- **`columns_types`** — Only required for Hive compability. Dictionary with keys as column names and values as data types (e.g. {'col0': 'bigint', 'col1': 'double'}). P.S. Only materialized columns please, not partition columns.
- **`partitions_parameters`** — Dictionary with key-value pairs defining partition parameters.

**Examples**

```python
>>> import awswrangler as wr
>>> wr.catalog.add_csv_partitions(
...     database='default',
...     table='my_table',
...     partitions_values={
...         's3://bucket/prefix/y=2020/m=10/': ['2020', '10'],
...         's3://bucket/prefix/y=2020/m=11/': ['2020', '11'],
...         's3://bucket/prefix/y=2020/m=12/': ['2020', '12']
...     }
... )
```

---

### add_parquet_partitions

```python
wr.catalog.add_parquet_partitions(
    database: 'str',
    table: 'str',
    partitions_values: 'dict[str,
    list[str]]',
    bucketing_info: 'typing.BucketingInfoTuple | None' = None,
    catalog_id: 'str | None' = None,
    compression: 'str | None' = None,
    boto3_session: 'boto3.Session | None' = None,
    columns_types: 'dict[str, str] | None' = None,
    partitions_parameters: 'dict[str, str] | None' = None
) -> 'None'
```

Add partitions (metadata) to a Parquet Table in the AWS Glue Catalog.


:::note
This function has arguments which can be configured globally through *wr.config* or environment variables:

- catalog_id

- database

Check out the `Global Configurations Tutorial <https://github.com/aws/aws-sdk-pandas/blob/main/tutorials/021%20-%20Global%20Configurations.ipynb>`_ for details.
:::

**Parameters**

- **`database`** — Database name.
- **`table`** — Table name.
- **`partitions_values`** — Dictionary with keys as S3 path locations and values as a list of partitions values as str (e.g. {'s3://bucket/prefix/y=2020/m=10/': ['2020', '10']}).
- **`bucketing_info`** — Tuple consisting of the column names used for bucketing as the first element and the number of buckets as the second element. Only `str`, `int` and `bool` are supported as column data types for bucketing.
- **`catalog_id`** — The ID of the Data Catalog from which to retrieve Databases. If none is provided, the AWS account ID is used by default.
- **`compression`** — Compression style (`None`, `snappy`, `gzip`, etc).
- **`boto3_session`** — Boto3 Session. The default boto3 session will be used if boto3_session receive None.
- **`columns_types`** — Only required for Hive compability. Dictionary with keys as column names and values as data types (e.g. {'col0': 'bigint', 'col1': 'double'}). P.S. Only materialized columns please, not partition columns.
- **`partitions_parameters`** — Dictionary with key-value pairs defining partition parameters.

**Examples**

```python
>>> import awswrangler as wr
>>> wr.catalog.add_parquet_partitions(
...     database='default',
...     table='my_table',
...     partitions_values={
...         's3://bucket/prefix/y=2020/m=10/': ['2020', '10'],
...         's3://bucket/prefix/y=2020/m=11/': ['2020', '11'],
...         's3://bucket/prefix/y=2020/m=12/': ['2020', '12']
...     }
... )
```

---

### create_csv_table

```python
wr.catalog.create_csv_table(
    database: 'str',
    table: 'str',
    path: 'str',
    columns_types: 'dict[str, str]',
    table_type: 'str | None' = None,
    partitions_types: 'dict[str, str] | None' = None,
    bucketing_info: 'typing.BucketingInfoTuple | None' = None,
    compression: 'str | None' = None,
    description: 'str | None' = None,
    parameters: 'dict[str, str] | None' = None,
    columns_comments: 'dict[str, str] | None' = None,
    columns_parameters: 'dict[str,
    dict[str, str]] | None' = None,
    mode: "Literal['overwrite', 'append']" = 'overwrite',
    catalog_versioning: 'bool' = False,
    schema_evolution: 'bool' = False,
    sep: 'str' = ',',
    skip_header_line_count: 'int | None' = None,
    serde_library: 'str | None' = None,
    serde_parameters: 'dict[str, str] | None' = None,
    boto3_session: 'boto3.Session | None' = None,
    athena_partition_projection_settings: 'typing.AthenaPartitionProjectionSettings | None' = None,
    catalog_id: 'str | None' = None
) -> 'None'
```

Create a CSV Table (Metadata Only) in the AWS Glue Catalog.

'https://docs.aws.amazon.com/athena/latest/ug/data-types.html'

:::note
Athena requires the columns in the underlying CSV files in S3 to be in the same order
as the columns in the Glue data catalog.
:::

:::note
This function has arguments which can be configured globally through *wr.config* or environment variables:

- catalog_id

- database

Check out the `Global Configurations Tutorial <https://github.com/aws/aws-sdk-pandas/blob/main/tutorials/021%20-%20Global%20Configurations.ipynb>`_ for details.
:::

**Parameters**

- **`database`** — Database name.
- **`table`** — Table name.
- **`path`** — Amazon S3 path (e.g. s3://bucket/prefix/).
- **`columns_types`** — Dictionary with keys as column names and values as data types (e.g. {'col0': 'bigint', 'col1': 'double'}).
- **`table_type`** — The type of the Glue Table. Set to EXTERNAL_TABLE if None.
- **`partitions_types`** — Dictionary with keys as partition names and values as data types (e.g. {'col2': 'date'}).
- **`bucketing_info`** — Tuple consisting of the column names used for bucketing as the first element and the number of buckets as the second element. Only `str`, `int` and `bool` are supported as column data types for bucketing.
- **`compression`** — Compression style (`None`, `gzip`, etc).
- **`description`** — Table description
- **`parameters`** — Key/value pairs to tag the table.
- **`columns_comments`** — Columns names and the related comments (e.g. {'col0': 'Column 0.', 'col1': 'Column 1.', 'col2': 'Partition.'}).
- **`columns_parameters`** — Columns names and the related parameters (e.g. {'col0': {'par0': 'Param 0', 'par1': 'Param 1'}}).
- **`mode`** — 'overwrite' to recreate any possible existing table or 'append' to keep any possible existing table.
- **`catalog_versioning`** — If True and `mode="overwrite"`, creates an archived version of the table catalog before updating it.
- **`schema_evolution`** — If True allows schema evolution (new or missing columns), otherwise a exception will be raised. (Only considered if dataset=True and mode in ("append", "overwrite_partitions")) Related tutorial: https://aws-sdk-pandas.readthedocs.io/en/3.17.1/tutorials/014%20-%20Schema%20Evolution.html
- **`sep`** — String of length 1. Field delimiter for the output file.
- **`skip_header_line_count`** — Number of Lines to skip regarding to the header.
- **`serde_library`** — Specifies the SerDe Serialization library which will be used. You need to provide the Class library name as a string. If no library is provided the default is `org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe`.
- **`serde_parameters`** — Dictionary of initialization parameters for the SerDe. The default is `{"field.delim": sep, "escape.delim": "\\"}`.
- **`athena_partition_projection_settings`** — Parameters of the Athena Partition Projection (https://docs.aws.amazon.com/athena/latest/ug/partition-projection.html). AthenaPartitionProjectionSettings is a `TypedDict`, meaning the passed parameter can be instantiated either as an instance of AthenaPartitionProjectionSettings or as a regular Python dict. Following projection parameters are supported: .. list-table:: Projection Parameters :header-rows: 1 * - Name - Type - Description * - projection_types - Optional[Dict[str, str]] - Dictionary of partitions names and Athena projections types. Valid types: "enum", "integer", "date", "injected" https://docs.aws.amazon.com/athena/latest/ug/partition-projection-supported-types.html (e.g. {'col_name': 'enum', 'col2_name': 'integer'}) * - projection_ranges - Optional[Dict[str, str]] - Dictionary of partitions names and Athena projections ranges. https://docs.aws.amazon.com/athena/latest/ug/partition-projection-supported-types.html (e.g. {'col_name': '0,10', 'col2_name': '-1,8675309'}) * - projection_values - Optional[Dict[str, str]] - Dictionary of partitions names and Athena projections values. https://docs.aws.amazon.com/athena/latest/ug/partition-projection-supported-types.html (e.g. {'col_name': 'A,B,Unknown', 'col2_name': 'foo,boo,bar'}) * - projection_intervals - Optional[Dict[str, str]] - Dictionary of partitions names and Athena projections intervals. https://docs.aws.amazon.com/athena/latest/ug/partition-projection-supported-types.html (e.g. {'col_name': '1', 'col2_name': '5'}) * - projection_digits - Optional[Dict[str, str]] - Dictionary of partitions names and Athena projections digits. https://docs.aws.amazon.com/athena/latest/ug/partition-projection-supported-types.html (e.g. {'col_name': '1', 'col2_name': '2'}) * - projection_formats - Optional[Dict[str, str]] - Dictionary of partitions names and Athena projections formats. https://docs.aws.amazon.com/athena/latest/ug/partition-projection-supported-types.html (e.g. {'col_date': 'yyyy-MM-dd', 'col2_timestamp': 'yyyy-MM-dd HH:mm:ss'}) * - projection_storage_location_template - Optional[str] - Value which is allows Athena to properly map partition values if the S3 file locations do not follow a typical `.../column=value/...` pattern. https://docs.aws.amazon.com/athena/latest/ug/partition-projection-setting-up.html (e.g. s3://bucket/table_root/a=${a}/${b}/some_static_subdirectory/${c}/)
- **`boto3_session`** — The default boto3 session will be used if **boto3_session** receive `None`.
- **`catalog_id`** — The ID of the Data Catalog from which to retrieve Databases. If `None` is provided, the AWS account ID is used by default.

**Examples**

```python
>>> import awswrangler as wr
>>> wr.catalog.create_csv_table(
...     database='default',
...     table='my_table',
...     path='s3://bucket/prefix/',
...     columns_types={'col0': 'bigint', 'col1': 'double'},
...     partitions_types={'col2': 'date'},
...     compression='gzip',
...     description='My own table!',
...     parameters={'source': 'postgresql'},
...     columns_comments={'col0': 'Column 0.', 'col1': 'Column 1.', 'col2': 'Partition.'}
... )
```

---

### create_database

```python
wr.catalog.create_database(
    name: 'str',
    description: 'str | None' = None,
    catalog_id: 'str | None' = None,
    exist_ok: 'bool' = False,
    database_input_args: 'dict[str, Any] | None' = None,
    boto3_session: 'boto3.Session | None' = None
) -> 'None'
```

Create a database in AWS Glue Catalog.


:::note
This function has arguments which can be configured globally through *wr.config* or environment variables:

- catalog_id

Check out the `Global Configurations Tutorial <https://github.com/aws/aws-sdk-pandas/blob/main/tutorials/021%20-%20Global%20Configurations.ipynb>`_ for details.
:::

**Parameters**

- **`name`** — Database name.
- **`description`** — A description for the Database.
- **`catalog_id`** — The ID of the Data Catalog from which to retrieve Databases. If `None` is provided, the AWS account ID is used by default.
- **`exist_ok`** — If set to `True` will not raise an Exception if a Database with the same already exists. In this case the description will be updated if it is different from the current one.
- **`database_input_args`** — Additional metadata to pass to database creation. Supported arguments listed here: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/glue.html#Glue.Client.create_database
- **`boto3_session`** — The default boto3 session will be used if **boto3_session** receive `None`.

**Examples**

```python
>>> import awswrangler as wr
>>> wr.catalog.create_database(
...     name='awswrangler_test'
... )
```

---

### create_json_table

```python
wr.catalog.create_json_table(
    database: 'str',
    table: 'str',
    path: 'str',
    columns_types: 'dict[str, str]',
    table_type: 'str | None' = None,
    partitions_types: 'dict[str, str] | None' = None,
    bucketing_info: 'typing.BucketingInfoTuple | None' = None,
    compression: 'str | None' = None,
    description: 'str | None' = None,
    parameters: 'dict[str, str] | None' = None,
    columns_comments: 'dict[str, str] | None' = None,
    columns_parameters: 'dict[str,
    dict[str, str]] | None' = None,
    mode: "Literal['overwrite', 'append']" = 'overwrite',
    catalog_versioning: 'bool' = False,
    schema_evolution: 'bool' = False,
    serde_library: 'str | None' = None,
    serde_parameters: 'dict[str, str] | None' = None,
    boto3_session: 'boto3.Session | None' = None,
    athena_partition_projection_settings: 'typing.AthenaPartitionProjectionSettings | None' = None,
    catalog_id: 'str | None' = None
) -> 'None'
```

Create a JSON Table (Metadata Only) in the AWS Glue Catalog.

'https://docs.aws.amazon.com/athena/latest/ug/data-types.html'


:::note
This function has arguments which can be configured globally through *wr.config* or environment variables:

- catalog_id

- database

Check out the `Global Configurations Tutorial <https://github.com/aws/aws-sdk-pandas/blob/main/tutorials/021%20-%20Global%20Configurations.ipynb>`_ for details.
:::

**Parameters**

- **`database`** — Database name.
- **`table`** — Table name.
- **`path`** — Amazon S3 path (e.g. s3://bucket/prefix/).
- **`columns_types`** — Dictionary with keys as column names and values as data types (e.g. {'col0': 'bigint', 'col1': 'double'}).
- **`table_type`** — The type of the Glue Table. Set to EXTERNAL_TABLE if None.
- **`partitions_types`** — Dictionary with keys as partition names and values as data types (e.g. {'col2': 'date'}).
- **`bucketing_info`** — Tuple consisting of the column names used for bucketing as the first element and the number of buckets as the second element. Only `str`, `int` and `bool` are supported as column data types for bucketing.
- **`compression`** — Compression style (`None`, `gzip`, etc).
- **`description`** — Table description
- **`parameters`** — Key/value pairs to tag the table.
- **`columns_comments`** — Columns names and the related comments (e.g. {'col0': 'Column 0.', 'col1': 'Column 1.', 'col2': 'Partition.'}).
- **`columns_parameters`** — Columns names and the related parameters (e.g. {'col0': {'par0': 'Param 0', 'par1': 'Param 1'}}).
- **`mode`** — 'overwrite' to recreate any possible existing table or 'append' to keep any possible existing table.
- **`catalog_versioning`** — If True and `mode="overwrite"`, creates an archived version of the table catalog before updating it.
- **`schema_evolution`** — If True allows schema evolution (new or missing columns), otherwise a exception will be raised. (Only considered if dataset=True and mode in ("append", "overwrite_partitions")) Related tutorial: https://aws-sdk-pandas.readthedocs.io/en/3.17.1/tutorials/014%20-%20Schema%20Evolution.html
- **`serde_library`** — Specifies the SerDe Serialization library which will be used. You need to provide the Class library name as a string. If no library is provided the default is `org.openx.data.jsonserde.JsonSerDe`.
- **`serde_parameters`** — Dictionary of initialization parameters for the SerDe. The default is `{"field.delim": sep, "escape.delim": "\\"}`.
- **`athena_partition_projection_settings`** — Parameters of the Athena Partition Projection (https://docs.aws.amazon.com/athena/latest/ug/partition-projection.html). AthenaPartitionProjectionSettings is a `TypedDict`, meaning the passed parameter can be instantiated either as an instance of AthenaPartitionProjectionSettings or as a regular Python dict. Following projection parameters are supported: .. list-table:: Projection Parameters :header-rows: 1 * - Name - Type - Description * - projection_types - Optional[Dict[str, str]] - Dictionary of partitions names and Athena projections types. Valid types: "enum", "integer", "date", "injected" https://docs.aws.amazon.com/athena/latest/ug/partition-projection-supported-types.html (e.g. {'col_name': 'enum', 'col2_name': 'integer'}) * - projection_ranges - Optional[Dict[str, str]] - Dictionary of partitions names and Athena projections ranges. https://docs.aws.amazon.com/athena/latest/ug/partition-projection-supported-types.html (e.g. {'col_name': '0,10', 'col2_name': '-1,8675309'}) * - projection_values - Optional[Dict[str, str]] - Dictionary of partitions names and Athena projections values. https://docs.aws.amazon.com/athena/latest/ug/partition-projection-supported-types.html (e.g. {'col_name': 'A,B,Unknown', 'col2_name': 'foo,boo,bar'}) * - projection_intervals - Optional[Dict[str, str]] - Dictionary of partitions names and Athena projections intervals. https://docs.aws.amazon.com/athena/latest/ug/partition-projection-supported-types.html (e.g. {'col_name': '1', 'col2_name': '5'}) * - projection_digits - Optional[Dict[str, str]] - Dictionary of partitions names and Athena projections digits. https://docs.aws.amazon.com/athena/latest/ug/partition-projection-supported-types.html (e.g. {'col_name': '1', 'col2_name': '2'}) * - projection_formats - Optional[Dict[str, str]] - Dictionary of partitions names and Athena projections formats. https://docs.aws.amazon.com/athena/latest/ug/partition-projection-supported-types.html (e.g. {'col_date': 'yyyy-MM-dd', 'col2_timestamp': 'yyyy-MM-dd HH:mm:ss'}) * - projection_storage_location_template - Optional[str] - Value which is allows Athena to properly map partition values if the S3 file locations do not follow a typical `.../column=value/...` pattern. https://docs.aws.amazon.com/athena/latest/ug/partition-projection-setting-up.html (e.g. s3://bucket/table_root/a=${a}/${b}/some_static_subdirectory/${c}/)
- **`boto3_session`** — The default boto3 session will be used if **boto3_session** receive `None`.
- **`catalog_id`** — The ID of the Data Catalog from which to retrieve Databases. If `None` is provided, the AWS account ID is used by default.

**Examples**

```python
>>> import awswrangler as wr
>>> wr.catalog.create_json_table(
...     database='default',
...     table='my_table',
...     path='s3://bucket/prefix/',
...     columns_types={'col0': 'bigint', 'col1': 'double'},
...     partitions_types={'col2': 'date'},
...     description='My very own JSON table!',
...     parameters={'source': 'postgresql'},
...     columns_comments={'col0': 'Column 0.', 'col1': 'Column 1.', 'col2': 'Partition.'}
... )
```

---

### create_parquet_table

```python
wr.catalog.create_parquet_table(
    database: 'str',
    table: 'str',
    path: 'str',
    columns_types: 'dict[str, str]',
    table_type: 'str | None' = None,
    partitions_types: 'dict[str, str] | None' = None,
    bucketing_info: 'typing.BucketingInfoTuple | None' = None,
    catalog_id: 'str | None' = None,
    compression: 'str | None' = None,
    description: 'str | None' = None,
    parameters: 'dict[str, str] | None' = None,
    columns_comments: 'dict[str, str] | None' = None,
    columns_parameters: 'dict[str,
    dict[str, str]] | None' = None,
    mode: "Literal['overwrite', 'append']" = 'overwrite',
    catalog_versioning: 'bool' = False,
    athena_partition_projection_settings: 'typing.AthenaPartitionProjectionSettings | None' = None,
    boto3_session: 'boto3.Session | None' = None
) -> 'None'
```

Create a Parquet Table (Metadata Only) in the AWS Glue Catalog.

'https://docs.aws.amazon.com/athena/latest/ug/data-types.html'


:::note
This function has arguments which can be configured globally through *wr.config* or environment variables:

- catalog_id

- database

Check out the `Global Configurations Tutorial <https://github.com/aws/aws-sdk-pandas/blob/main/tutorials/021%20-%20Global%20Configurations.ipynb>`_ for details.
:::

**Parameters**

- **`database`** — Database name.
- **`table`** — Table name.
- **`path`** — Amazon S3 path (e.g. s3://bucket/prefix/).
- **`columns_types`** — Dictionary with keys as column names and values as data types (e.g. {'col0': 'bigint', 'col1': 'double'}).
- **`table_type`** — The type of the Glue Table. Set to `EXTERNAL_TABLE` if `None`.
- **`partitions_types`** — Dictionary with keys as partition names and values as data types (e.g. {'col2': 'date'}).
- **`bucketing_info`** — Tuple consisting of the column names used for bucketing as the first element and the number of buckets as the second element. Only `str`, `int` and `bool` are supported as column data types for bucketing.
- **`catalog_id`** — The ID of the Data Catalog from which to retrieve Databases. If none is provided, the AWS account ID is used by default.
- **`compression`** — Compression style (`None`, `snappy`, `gzip`, etc).
- **`description`** — Table description
- **`parameters`** — Key/value pairs to tag the table.
- **`columns_comments`** — Columns names and the related comments (e.g. {'col0': 'Column 0.', 'col1': 'Column 1.', 'col2': 'Partition.'}).
- **`columns_parameters`** — Columns names and the related parameters (e.g. {'col0': {'par0': 'Param 0', 'par1': 'Param 1'}}).
- **`mode`** — 'overwrite' to recreate any possible existing table or 'append' to keep any possible existing table.
- **`catalog_versioning`** — If True and `mode="overwrite"`, creates an archived version of the table catalog before updating it.
- **`athena_partition_projection_settings`** — Parameters of the Athena Partition Projection (https://docs.aws.amazon.com/athena/latest/ug/partition-projection.html). AthenaPartitionProjectionSettings is a `TypedDict`, meaning the passed parameter can be instantiated either as an instance of AthenaPartitionProjectionSettings or as a regular Python dict. Following projection parameters are supported: .. list-table:: Projection Parameters :header-rows: 1 * - Name - Type - Description * - projection_types - Optional[Dict[str, str]] - Dictionary of partitions names and Athena projections types. Valid types: "enum", "integer", "date", "injected" https://docs.aws.amazon.com/athena/latest/ug/partition-projection-supported-types.html (e.g. {'col_name': 'enum', 'col2_name': 'integer'}) * - projection_ranges - Optional[Dict[str, str]] - Dictionary of partitions names and Athena projections ranges. https://docs.aws.amazon.com/athena/latest/ug/partition-projection-supported-types.html (e.g. {'col_name': '0,10', 'col2_name': '-1,8675309'}) * - projection_values - Optional[Dict[str, str]] - Dictionary of partitions names and Athena projections values. https://docs.aws.amazon.com/athena/latest/ug/partition-projection-supported-types.html (e.g. {'col_name': 'A,B,Unknown', 'col2_name': 'foo,boo,bar'}) * - projection_intervals - Optional[Dict[str, str]] - Dictionary of partitions names and Athena projections intervals. https://docs.aws.amazon.com/athena/latest/ug/partition-projection-supported-types.html (e.g. {'col_name': '1', 'col2_name': '5'}) * - projection_digits - Optional[Dict[str, str]] - Dictionary of partitions names and Athena projections digits. https://docs.aws.amazon.com/athena/latest/ug/partition-projection-supported-types.html (e.g. {'col_name': '1', 'col2_name': '2'}) * - projection_formats - Optional[Dict[str, str]] - Dictionary of partitions names and Athena projections formats. https://docs.aws.amazon.com/athena/latest/ug/partition-projection-supported-types.html (e.g. {'col_date': 'yyyy-MM-dd', 'col2_timestamp': 'yyyy-MM-dd HH:mm:ss'}) * - projection_storage_location_template - Optional[str] - Value which is allows Athena to properly map partition values if the S3 file locations do not follow a typical `.../column=value/...` pattern. https://docs.aws.amazon.com/athena/latest/ug/partition-projection-setting-up.html (e.g. s3://bucket/table_root/a=${a}/${b}/some_static_subdirectory/${c}/)
- **`boto3_session`** — The default boto3 session will be used if **boto3_session** receive `None`.

**Examples**

```python
>>> import awswrangler as wr
>>> wr.catalog.create_parquet_table(
...     database='default',
...     table='my_table',
...     path='s3://bucket/prefix/',
...     columns_types={'col0': 'bigint', 'col1': 'double'},
...     partitions_types={'col2': 'date'},
...     compression='snappy',
...     description='My own table!',
...     parameters={'source': 'postgresql'},
...     columns_comments={'col0': 'Column 0.', 'col1': 'Column 1.', 'col2': 'Partition.'}
... )
```

---

### databases

```python
wr.catalog.databases(
    limit: 'int' = 100,
    catalog_id: 'str | None' = None,
    boto3_session: 'boto3.Session | None' = None
) -> 'pd.DataFrame'
```

Get a Pandas DataFrame with all listed databases.


:::note
This function has arguments which can be configured globally through *wr.config* or environment variables:

- catalog_id

Check out the `Global Configurations Tutorial <https://github.com/aws/aws-sdk-pandas/blob/main/tutorials/021%20-%20Global%20Configurations.ipynb>`_ for details.
:::

**Parameters**

- **`limit`** — Max number of tables to be returned.
- **`catalog_id`** — The ID of the Data Catalog from which to retrieve Databases. If `None` is provided, the AWS account ID is used by default.
- **`boto3_session`** — The default boto3 session will be used if **boto3_session** receive `None`.

**Returns**

- Pandas DataFrame filled by formatted table information.

**Examples**

```python
>>> import awswrangler as wr
>>> df_dbs = wr.catalog.databases()
```

---

### delete_column

```python
wr.catalog.delete_column(
    database: 'str',
    table: 'str',
    column_name: 'str',
    boto3_session: 'boto3.Session | None' = None,
    catalog_id: 'str | None' = None
) -> 'None'
```

Delete a column in a AWS Glue Catalog table.


:::note
This function has arguments which can be configured globally through *wr.config* or environment variables:

- catalog_id

- database

Check out the `Global Configurations Tutorial <https://github.com/aws/aws-sdk-pandas/blob/main/tutorials/021%20-%20Global%20Configurations.ipynb>`_ for details.
:::

**Parameters**

- **`database`** — Database name.
- **`table`** — Table name.
- **`column_name`** — Column name
- **`boto3_session`** — The default boto3 session will be used if **boto3_session** receive `None`.
- **`catalog_id`** — The ID of the Data Catalog from which to retrieve Databases. If `None` is provided, the AWS account ID is used by default.

**Examples**

```python
>>> import awswrangler as wr
>>> wr.catalog.delete_column(
...     database='my_db',
...     table='my_table',
...     column_name='my_col',
... )
```

---

### delete_database

```python
wr.catalog.delete_database(
    name: 'str',
    catalog_id: 'str | None' = None,
    boto3_session: 'boto3.Session | None' = None
) -> 'None'
```

Delete a database in AWS Glue Catalog.


:::note
This function has arguments which can be configured globally through *wr.config* or environment variables:

- catalog_id

Check out the `Global Configurations Tutorial <https://github.com/aws/aws-sdk-pandas/blob/main/tutorials/021%20-%20Global%20Configurations.ipynb>`_ for details.
:::

**Parameters**

- **`name`** — Database name.
- **`catalog_id`** — The ID of the Data Catalog from which to retrieve Databases. If none is provided, the AWS account ID is used by default.
- **`boto3_session`** — The default boto3 session will be used if **boto3_session** receive `None`.

**Examples**

```python
>>> import awswrangler as wr
>>> wr.catalog.delete_database(
...     name='awswrangler_test'
... )
```

---

### delete_partitions

```python
wr.catalog.delete_partitions(
    table: 'str',
    database: 'str',
    partitions_values: 'list[list[str]]',
    catalog_id: 'str | None' = None,
    boto3_session: 'boto3.Session | None' = None
) -> 'None'
```

Delete specified partitions in a AWS Glue Catalog table.


:::note
This function has arguments which can be configured globally through *wr.config* or environment variables:

- catalog_id

- database

Check out the `Global Configurations Tutorial <https://github.com/aws/aws-sdk-pandas/blob/main/tutorials/021%20-%20Global%20Configurations.ipynb>`_ for details.
:::

**Parameters**

- **`table`** — Table name.
- **`database`** — Table name.
- **`catalog_id`** — The ID of the Data Catalog from which to retrieve Databases. If none is provided, the AWS account ID is used by default.
- **`partitions_values`** — List of lists of partitions values as strings. (e.g. [['2020', '10', '25'], ['2020', '11', '16'], ['2020', '12', '19']]).
- **`boto3_session`** — The default boto3 session will be used if **boto3_session** receive `None`.

**Examples**

```python
>>> import awswrangler as wr
>>> wr.catalog.delete_partitions(
...     table='my_table',
...     database='awswrangler_test',
...     partitions_values=[['2020', '10', '25'], ['2020', '11', '16'], ['2020', '12', '19']]
... )
```

---

### delete_all_partitions

```python
wr.catalog.delete_all_partitions(
    table: 'str',
    database: 'str',
    catalog_id: 'str | None' = None,
    boto3_session: 'boto3.Session | None' = None
) -> 'list[list[str]]'
```

Delete all partitions in a AWS Glue Catalog table.


:::note
This function has arguments which can be configured globally through *wr.config* or environment variables:

- catalog_id

- database

Check out the `Global Configurations Tutorial <https://github.com/aws/aws-sdk-pandas/blob/main/tutorials/021%20-%20Global%20Configurations.ipynb>`_ for details.
:::

**Parameters**

- **`table`** — Table name.
- **`database`** — Table name.
- **`catalog_id`** — The ID of the Data Catalog from which to retrieve Databases. If none is provided, the AWS account ID is used by default.
- **`boto3_session`** — The default boto3 session will be used if **boto3_session** receive `None`.

**Returns**

- Partitions values.

**Examples**

```python
>>> import awswrangler as wr
>>> partitions = wr.catalog.delete_all_partitions(
...     table='my_table',
...     database='awswrangler_test',
... )
```

---

### delete_table_if_exists

```python
wr.catalog.delete_table_if_exists(
    database: 'str',
    table: 'str',
    catalog_id: 'str | None' = None,
    boto3_session: 'boto3.Session | None' = None
) -> 'bool'
```

Delete Glue table if exists.


:::note
This function has arguments which can be configured globally through *wr.config* or environment variables:

- catalog_id

- database

Check out the `Global Configurations Tutorial <https://github.com/aws/aws-sdk-pandas/blob/main/tutorials/021%20-%20Global%20Configurations.ipynb>`_ for details.
:::

**Parameters**

- **`database`** — Database name.
- **`table`** — Table name.
- **`catalog_id`** — The ID of the Data Catalog from which to retrieve Databases. If none is provided, the AWS account ID is used by default.
- **`boto3_session`** — The default boto3 session will be used if **boto3_session** receive `None`.

**Returns**

- `True` if deleted, otherwise `False`.

**Examples**

```python
>>> import awswrangler as wr
>>> wr.catalog.delete_table_if_exists(database='default', table='my_table')  # deleted
True
>>> wr.catalog.delete_table_if_exists(database='default', table='my_table')  # Nothing to be deleted
False
```

---

### does_table_exist

```python
wr.catalog.does_table_exist(
    database: 'str',
    table: 'str',
    boto3_session: 'boto3.Session | None' = None,
    catalog_id: 'str | None' = None
) -> 'bool'
```

Check if the table exists.


:::note
This function has arguments which can be configured globally through *wr.config* or environment variables:

- catalog_id

- database

Check out the `Global Configurations Tutorial <https://github.com/aws/aws-sdk-pandas/blob/main/tutorials/021%20-%20Global%20Configurations.ipynb>`_ for details.
:::

**Parameters**

- **`database`** — Database name.
- **`table`** — Table name.
- **`boto3_session`** — The default boto3 session will be used if **boto3_session** receive `None`.
- **`catalog_id`** — The ID of the Data Catalog from which to retrieve Databases. If `None` is provided, the AWS account ID is used by default.

**Returns**

- `True` if exists, otherwise `False`.

**Examples**

```python
>>> import awswrangler as wr
>>> wr.catalog.does_table_exist(database='default', table='my_table')
```

---

### drop_duplicated_columns

```python
wr.catalog.drop_duplicated_columns(df: 'pd.DataFrame') -> 'pd.DataFrame'
```

Drop all repeated columns (duplicated names).

:::note
This transformation will run `inplace` and will make changes in the original DataFrame.
:::
:::note
It is different from Panda's drop_duplicates() function which considers the column values.
wr.catalog.drop_duplicated_columns() will deduplicate by column name.
:::

**Parameters**

- **`df`** — Original Pandas DataFrame.

**Returns**

- Pandas DataFrame without duplicated columns.

**Examples**

```python
>>> import awswrangler as wr
>>> df = pd.DataFrame({"A": [1, 2], "B": [3, 4]})
>>> df.columns = ["A", "A"]
>>> wr.catalog.drop_duplicated_columns(df=df)
A
0  1
1  2
```

---

### extract_athena_types

```python
wr.catalog.extract_athena_types(
    df: 'pd.DataFrame',
    index: 'bool' = False,
    partition_cols: 'list[str] | None' = None,
    dtype: 'dict[str, str] | None' = None,
    file_format: 'str' = 'parquet'
) -> 'tuple[dict[str, str], dict[str, str]]'
```

Extract columns and partitions types (Amazon Athena) from Pandas DataFrame.

https://docs.aws.amazon.com/athena/latest/ug/data-types.html

**Parameters**

- **`df`** — Pandas DataFrame.
- **`index`** — Should consider the DataFrame index as a column?.
- **`partition_cols`** — List of partitions names.
- **`dtype`** — Dictionary of columns names and Athena/Glue types to be casted. Useful when you have columns with undetermined or mixed data types. (e.g. {'col name': 'bigint', 'col2 name': 'int'})
- **`file_format`** — File format to be considered to place the index column: "parquet" | "csv".

**Returns**

- columns_types: Dictionary with keys as column names and values as data types (e.g. {'col0': 'bigint', 'col1': 'double'}). / partitions_types: Dictionary with keys as partition names and values as data types (e.g. {'col2': 'date'}).

**Examples**

```python
>>> import awswrangler as wr
>>> columns_types, partitions_types = wr.catalog.extract_athena_types(
...     df=df, index=False, partition_cols=["par0", "par1"], file_format="csv"
... )
```

---

### get_columns_comments

```python
wr.catalog.get_columns_comments(
    database: 'str',
    table: 'str',
    catalog_id: 'str | None' = None,
    boto3_session: 'boto3.Session | None' = None
) -> 'dict[str, str | None]'
```

Get all columns comments.


:::note
This function has arguments which can be configured globally through *wr.config* or environment variables:

- catalog_id

- database

Check out the `Global Configurations Tutorial <https://github.com/aws/aws-sdk-pandas/blob/main/tutorials/021%20-%20Global%20Configurations.ipynb>`_ for details.
:::

**Parameters**

- **`database`** — Database name.
- **`table`** — Table name.
- **`catalog_id`** — The ID of the Data Catalog from which to retrieve Databases. If `None` is provided, the AWS account ID is used by default.
- **`boto3_session`** — The default boto3 session will be used if **boto3_session** receive `None`.

**Returns**

- Columns comments. e.g. {"col1": "foo boo bar", "col2": None}.

**Examples**

```python
>>> import awswrangler as wr
>>> pars = wr.catalog.get_columns_comments(database="...", table="...")
```

---

### get_columns_parameters

```python
wr.catalog.get_columns_parameters(
    database: 'str',
    table: 'str',
    catalog_id: 'str | None' = None,
    boto3_session: 'boto3.Session | None' = None
) -> 'dict[str, dict[str, str] | None]'
```

Get all columns parameters.


:::note
This function has arguments which can be configured globally through *wr.config* or environment variables:

- catalog_id

- database

Check out the `Global Configurations Tutorial <https://github.com/aws/aws-sdk-pandas/blob/main/tutorials/021%20-%20Global%20Configurations.ipynb>`_ for details.
:::

**Parameters**

- **`database`** — Database name.
- **`table`** — Table name.
- **`catalog_id`** — The ID of the Data Catalog from which to retrieve Databases. If `None` is provided, the AWS account ID is used by default.
- **`boto3_session`** — The default boto3 session will be used if **boto3_session** receive `None`.

**Returns**

- Columns parameters.

**Examples**

```python
>>> import awswrangler as wr
>>> pars = wr.catalog.get_columns_parameters(database="...", table="...")
```

---

### get_csv_partitions

```python
wr.catalog.get_csv_partitions(
    database: 'str',
    table: 'str',
    expression: 'str | None' = None,
    catalog_id: 'str | None' = None,
    boto3_session: 'boto3.Session | None' = None
) -> 'dict[str, list[str]]'
```

Get all partitions from a Table in the AWS Glue Catalog.

Expression argument instructions:
https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/glue.html#Glue.Client.get_partitions


:::note
This function has arguments which can be configured globally through *wr.config* or environment variables:

- catalog_id

- database

Check out the `Global Configurations Tutorial <https://github.com/aws/aws-sdk-pandas/blob/main/tutorials/021%20-%20Global%20Configurations.ipynb>`_ for details.
:::

**Parameters**

- **`database`** — Database name.
- **`table`** — Table name.
- **`expression`** — An expression that filters the partitions to be returned.
- **`catalog_id`** — The ID of the Data Catalog from which to retrieve Databases. If `None` is provided, the AWS account ID is used by default.
- **`boto3_session`** — The default boto3 session will be used if **boto3_session** receive `None`.

**Returns**

- partitions_values: Dictionary with keys as S3 path locations and values as a list of partitions values as str (e.g. {'s3://bucket/prefix/y=2020/m=10/': ['2020', '10']}).

**Examples**

Fetch all partitions

```python
>>> import awswrangler as wr
>>> wr.catalog.get_csv_partitions(
...     database='default',
...     table='my_table',
... )
{
's3://bucket/prefix/y=2020/m=10/': ['2020', '10'],
's3://bucket/prefix/y=2020/m=11/': ['2020', '11'],
's3://bucket/prefix/y=2020/m=12/': ['2020', '12']
}
```

Filtering partitions

```python
>>> import awswrangler as wr
>>> wr.catalog.get_csv_partitions(
...     database='default',
...     table='my_table',
...     expression='m=10'
... )
{
's3://bucket/prefix/y=2020/m=10/': ['2020', '10']
}
```

---

### get_databases

```python
wr.catalog.get_databases(
    catalog_id: 'str | None' = None,
    boto3_session: 'boto3.Session | None' = None
) -> 'Iterator[dict[str, Any]]'
```

Get an iterator of databases.

**Parameters**

- **`catalog_id`** — The ID of the Data Catalog from which to retrieve Databases. If `None` is provided, the AWS account ID is used by default.
- **`boto3_session`** — The default boto3 session will be used if **boto3_session** receive `None`.

**Returns**

- Iterator of Databases.

**Examples**

```python
>>> import awswrangler as wr
>>> dbs = wr.catalog.get_databases()
```

---

### get_parquet_partitions

```python
wr.catalog.get_parquet_partitions(
    database: 'str',
    table: 'str',
    expression: 'str | None' = None,
    catalog_id: 'str | None' = None,
    boto3_session: 'boto3.Session | None' = None
) -> 'dict[str, list[str]]'
```

Get all partitions from a Table in the AWS Glue Catalog.

Expression argument instructions:
https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/glue.html#Glue.Client.get_partitions


:::note
This function has arguments which can be configured globally through *wr.config* or environment variables:

- catalog_id

- database

Check out the `Global Configurations Tutorial <https://github.com/aws/aws-sdk-pandas/blob/main/tutorials/021%20-%20Global%20Configurations.ipynb>`_ for details.
:::

**Parameters**

- **`database`** — Database name.
- **`table`** — Table name.
- **`expression`** — An expression that filters the partitions to be returned.
- **`catalog_id`** — The ID of the Data Catalog from which to retrieve Databases. If none is provided, the AWS account ID is used by default.
- **`boto3_session`** — The default boto3 session will be used if **boto3_session** receive `None`.

**Returns**

- partitions_values: Dictionary with keys as S3 path locations and values as a list of partitions values as str (e.g. {'s3://bucket/prefix/y=2020/m=10/': ['2020', '10']}).

**Examples**

Fetch all partitions

```python
>>> import awswrangler as wr
>>> wr.catalog.get_parquet_partitions(
...     database='default',
...     table='my_table',
... )
{
's3://bucket/prefix/y=2020/m=10/': ['2020', '10'],
's3://bucket/prefix/y=2020/m=11/': ['2020', '11'],
's3://bucket/prefix/y=2020/m=12/': ['2020', '12']
}
```

Filtering partitions

```python
>>> import awswrangler as wr
>>> wr.catalog.get_parquet_partitions(
...     database='default',
...     table='my_table',
...     expression='m=10'
... )
{
's3://bucket/prefix/y=2020/m=10/': ['2020', '10']
}
```

---

### get_partitions

```python
wr.catalog.get_partitions(
    database: 'str',
    table: 'str',
    expression: 'str | None' = None,
    catalog_id: 'str | None' = None,
    boto3_session: 'boto3.Session | None' = None
) -> 'dict[str, list[str]]'
```

Get all partitions from a Table in the AWS Glue Catalog.

Expression argument instructions:
https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/glue.html#Glue.Client.get_partitions


:::note
This function has arguments which can be configured globally through *wr.config* or environment variables:

- catalog_id

- database

Check out the `Global Configurations Tutorial <https://github.com/aws/aws-sdk-pandas/blob/main/tutorials/021%20-%20Global%20Configurations.ipynb>`_ for details.
:::

**Parameters**

- **`database`** — Database name.
- **`table`** — Table name.
- **`expression`** — An expression that filters the partitions to be returned.
- **`catalog_id`** — The ID of the Data Catalog from which to retrieve Databases. If `None` is provided, the AWS account ID is used by default.
- **`boto3_session`** — The default boto3 session will be used if **boto3_session** receive `None`.

**Returns**

- partitions_values: Dictionary with keys as S3 path locations and values as a list of partitions values as str (e.g. {'s3://bucket/prefix/y=2020/m=10/': ['2020', '10']}).

**Examples**

Fetch all partitions

```python
>>> import awswrangler as wr
>>> wr.catalog.get_partitions(
...     database='default',
...     table='my_table',
... )
{
's3://bucket/prefix/y=2020/m=10/': ['2020', '10'],
's3://bucket/prefix/y=2020/m=11/': ['2020', '11'],
's3://bucket/prefix/y=2020/m=12/': ['2020', '12']
}
```

Filtering partitions

```python
>>> import awswrangler as wr
>>> wr.catalog.get_partitions(
...     database='default',
...     table='my_table',
...     expression='m=10'
... )
{
's3://bucket/prefix/y=2020/m=10/': ['2020', '10']
}
```

---

### get_table_description

```python
wr.catalog.get_table_description(
    database: 'str',
    table: 'str',
    catalog_id: 'str | None' = None,
    boto3_session: 'boto3.Session | None' = None
) -> 'str | None'
```

Get table description.

**Parameters**

- **`database`** — Database name.
- **`table`** — Table name.
- **`catalog_id`** — The ID of the Data Catalog from which to retrieve Databases. If none is provided, the AWS account ID is used by default.
- **`boto3_session`** — The default boto3 session will be used if **boto3_session** receive `None`.

**Returns**

- Description if exists.

**Examples**

```python
>>> import awswrangler as wr
>>> desc = wr.catalog.get_table_description(database="...", table="...")
```

---

### get_table_location

```python
wr.catalog.get_table_location(
    database: 'str',
    table: 'str',
    catalog_id: 'str | None' = None,
    boto3_session: 'boto3.Session | None' = None
) -> 'str'
```

Get table's location on Glue catalog.


:::note
This function has arguments which can be configured globally through *wr.config* or environment variables:

- catalog_id

- database

Check out the `Global Configurations Tutorial <https://github.com/aws/aws-sdk-pandas/blob/main/tutorials/021%20-%20Global%20Configurations.ipynb>`_ for details.
:::

**Parameters**

- **`database`** — Database name.
- **`table`** — Table name.
- **`catalog_id`** — The ID of the Data Catalog from which to retrieve Databases. If none is provided, the AWS account ID is used by default.
- **`boto3_session`** — The default boto3 session will be used if **boto3_session** receive `None`.

**Returns**

- Table's location.

**Examples**

```python
>>> import awswrangler as wr
>>> wr.catalog.get_table_location(database='default', table='my_table')
's3://bucket/prefix/'
```

---

### get_table_number_of_versions

```python
wr.catalog.get_table_number_of_versions(
    database: 'str',
    table: 'str',
    catalog_id: 'str | None' = None,
    boto3_session: 'boto3.Session | None' = None
) -> 'int'
```

Get total number of versions.


:::note
This function has arguments which can be configured globally through *wr.config* or environment variables:

- catalog_id

- database

Check out the `Global Configurations Tutorial <https://github.com/aws/aws-sdk-pandas/blob/main/tutorials/021%20-%20Global%20Configurations.ipynb>`_ for details.
:::

**Parameters**

- **`database`** — Database name.
- **`table`** — Table name.
- **`catalog_id`** — The ID of the Data Catalog from which to retrieve Databases. If `None` is provided, the AWS account ID is used by default.
- **`boto3_session`** — The default boto3 session will be used if **boto3_session** receive `None`.

**Returns**

- Total number of versions.

**Examples**

```python
>>> import awswrangler as wr
>>> num = wr.catalog.get_table_number_of_versions(database="...", table="...")
```

---

### get_table_parameters

```python
wr.catalog.get_table_parameters(
    database: 'str',
    table: 'str',
    catalog_id: 'str | None' = None,
    boto3_session: 'boto3.Session | None' = None
) -> 'dict[str, str]'
```

Get all parameters.

**Parameters**

- **`database`** — Database name.
- **`table`** — Table name.
- **`catalog_id`** — The ID of the Data Catalog from which to retrieve Databases. If `None` is provided, the AWS account ID is used by default.
- **`boto3_session`** — The default boto3 session will be used if **boto3_session** receive `None`.

**Returns**

- Dictionary of parameters.

**Examples**

```python
>>> import awswrangler as wr
>>> pars = wr.catalog.get_table_parameters(database="...", table="...")
```

---

### get_table_types

```python
wr.catalog.get_table_types(
    database: 'str',
    table: 'str',
    catalog_id: 'str | None' = None,
    filter_iceberg_current: 'bool' = False,
    boto3_session: 'boto3.Session | None' = None
) -> 'dict[str, str] | None'
```

Get all columns and types from a table.


:::note
This function has arguments which can be configured globally through *wr.config* or environment variables:

- catalog_id

- database

Check out the `Global Configurations Tutorial <https://github.com/aws/aws-sdk-pandas/blob/main/tutorials/021%20-%20Global%20Configurations.ipynb>`_ for details.
:::

**Parameters**

- **`database`** — Database name.
- **`table`** — Table name.
- **`catalog_id`** — The ID of the Data Catalog from which to retrieve Databases. If `None` is provided, the AWS account ID is used by default.
- **`filter_iceberg_current`** — If True, returns only current iceberg fields (fields marked with iceberg.field.current: true). Otherwise, returns the all fields. False by default (return all fields).
- **`boto3_session`** — The default boto3 session will be used if **boto3_session** receive `None`.

**Returns**

- If table exists, a dictionary like {'col name': 'col data type'}. Otherwise None.

**Examples**

```python
>>> import awswrangler as wr
>>> wr.catalog.get_table_types(database='default', table='my_table')
{'col0': 'int', 'col1': double}
```

---

### get_table_versions

```python
wr.catalog.get_table_versions(
    database: 'str',
    table: 'str',
    catalog_id: 'str | None' = None,
    boto3_session: 'boto3.Session | None' = None
) -> 'list[dict[str, Any]]'
```

Get all versions.


:::note
This function has arguments which can be configured globally through *wr.config* or environment variables:

- catalog_id

- database

Check out the `Global Configurations Tutorial <https://github.com/aws/aws-sdk-pandas/blob/main/tutorials/021%20-%20Global%20Configurations.ipynb>`_ for details.
:::

**Parameters**

- **`database`** — Database name.
- **`table`** — Table name.
- **`catalog_id`** — The ID of the Data Catalog from which to retrieve Databases. If `None` is provided, the AWS account ID is used by default.
- **`boto3_session`** — The default boto3 session will be used if **boto3_session** receive `None`.

**Returns**

- List of table inputs: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/glue.html#Glue.Client.get_table_versions

**Examples**

```python
>>> import awswrangler as wr
>>> tables_versions = wr.catalog.get_table_versions(database="...", table="...")
```

---

### get_tables

```python
wr.catalog.get_tables(
    catalog_id: 'str | None' = None,
    database: 'str | None' = None,
    name_contains: 'str | None' = None,
    name_prefix: 'str | None' = None,
    name_suffix: 'str | None' = None,
    boto3_session: 'boto3.Session | None' = None
) -> 'Iterator[dict[str, Any]]'
```

Get an iterator of tables.

:::note
Please, do not filter using name_contains and name_prefix/name_suffix at the same time.
Only name_prefix and name_suffix can be combined together.
:::

:::note
This function has arguments which can be configured globally through *wr.config* or environment variables:

- catalog_id

- database

Check out the `Global Configurations Tutorial <https://github.com/aws/aws-sdk-pandas/blob/main/tutorials/021%20-%20Global%20Configurations.ipynb>`_ for details.
:::

**Parameters**

- **`catalog_id`** — The ID of the Data Catalog from which to retrieve Databases. If `None` is provided, the AWS account ID is used by default.
- **`database`** — Database name.
- **`name_contains`** — Select by a specific string on table name
- **`name_prefix`** — Select by a specific prefix on table name
- **`name_suffix`** — Select by a specific suffix on table name
- **`boto3_session`** — The default boto3 session will be used if **boto3_session** receive `None`.

**Returns**

- Iterator of tables.

**Examples**

```python
>>> import awswrangler as wr
>>> tables = wr.catalog.get_tables()
```

---

### overwrite_table_parameters

```python
wr.catalog.overwrite_table_parameters(
    parameters: 'dict[str, str]',
    database: 'str',
    table: 'str',
    catalog_versioning: 'bool' = False,
    catalog_id: 'str | None' = None,
    boto3_session: 'boto3.Session | None' = None
) -> 'dict[str, str]'
```

Overwrite all existing parameters.


:::note
This function has arguments which can be configured globally through *wr.config* or environment variables:

- catalog_id

- database

Check out the `Global Configurations Tutorial <https://github.com/aws/aws-sdk-pandas/blob/main/tutorials/021%20-%20Global%20Configurations.ipynb>`_ for details.
:::

**Parameters**

- **`parameters`** — e.g. {"source": "mysql", "destination":  "datalake"}
- **`database`** — Database name.
- **`table`** — Table name.
- **`catalog_versioning`** — If True and `mode="overwrite"`, creates an archived version of the table catalog before updating it.
- **`catalog_id`** — The ID of the Data Catalog from which to retrieve Databases. If none is provided, the AWS account ID is used by default.
- **`boto3_session`** — The default boto3 session will be used if **boto3_session** is `None`.

**Returns**

- All parameters after the overwrite (The same received).

**Examples**

```python
>>> import awswrangler as wr
>>> pars = wr.catalog.overwrite_table_parameters(
...     parameters={"source": "mysql", "destination":  "datalake"},
...     database="...",
...     table="...",
... )
```

---

### sanitize_column_name

```python
wr.catalog.sanitize_column_name(column: 'str') -> 'str'
```

Convert the column name to be compatible with Amazon Athena and the AWS Glue Catalog.

https://docs.aws.amazon.com/athena/latest/ug/tables-databases-columns-names.html

Possible transformations:
- Strip accents
- Remove non alphanumeric characters

**Parameters**

- **`column`** — Column name.

**Returns**

- Normalized column name.

**Examples**

```python
>>> import awswrangler as wr
>>> wr.catalog.sanitize_column_name('MyNewColumn')
'mynewcolumn'
```

---

### sanitize_dataframe_columns_names

```python
wr.catalog.sanitize_dataframe_columns_names(
    df: 'pd.DataFrame',
    handle_duplicate_columns: 'str | None' = 'warn'
) -> 'pd.DataFrame'
```

Normalize all columns names to be compatible with Amazon Athena.

https://docs.aws.amazon.com/athena/latest/ug/tables-databases-columns-names.html

Possible transformations:
- Strip accents
- Remove non alphanumeric characters

:::note
After transformation, some column names might not be unique anymore.
Example: the columns ["A", "a"] will be sanitized to ["a", "a"]
:::

**Parameters**

- **`df`** — Original Pandas DataFrame.
- **`handle_duplicate_columns`** — How to handle duplicate columns. Can be "warn" or "drop" or "rename". "drop" will drop all but the first duplicated column. "rename" will rename all duplicated columns with an incremental number. Defaults to "warn".

**Returns**

- Original Pandas DataFrame with columns names normalized.

**Examples**

```python
>>> import awswrangler as wr
>>> df_normalized = wr.catalog.sanitize_dataframe_columns_names(df=pd.DataFrame({"A": [1, 2]}))
>>> df_normalized_drop = wr.catalog.sanitize_dataframe_columns_names(
df=pd.DataFrame({"A": [1, 2], "a": [3, 4]}), handle_duplicate_columns="drop"
)
>>> df_normalized_rename = wr.catalog.sanitize_dataframe_columns_names(
df=pd.DataFrame({"A": [1, 2], "a": [3, 4], "a_1": [4, 6]}), handle_duplicate_columns="rename"
)
```

---

### sanitize_table_name

```python
wr.catalog.sanitize_table_name(table: 'str') -> 'str'
```

Convert the table name to be compatible with Amazon Athena and the AWS Glue Catalog.

https://docs.aws.amazon.com/athena/latest/ug/tables-databases-columns-names.html

Possible transformations:
- Strip accents
- Remove non alphanumeric characters

**Parameters**

- **`table`** — Table name.

**Returns**

- Normalized table name.

**Examples**

```python
>>> import awswrangler as wr
>>> wr.catalog.sanitize_table_name('MyNewTable')
'mynewtable'
```

---

### search_tables

```python
wr.catalog.search_tables(
    text: 'str',
    catalog_id: 'str | None' = None,
    boto3_session: 'boto3.Session | None' = None
) -> 'Iterator[dict[str, Any]]'
```

Get Pandas DataFrame of tables filtered by a search string.

**Parameters**

- **`text`** — Select only tables with the given string in table's properties.
- **`catalog_id`** — The ID of the Data Catalog from which to retrieve Databases. If `None` is provided, the AWS account ID is used by default.
- **`boto3_session`** — The default boto3 session will be used if **boto3_session** receive `None`.

**Returns**

- Iterator of tables.

**Examples**

```python
>>> import awswrangler as wr
>>> df_tables = wr.catalog.search_tables(text='my_property')
```

---

### table

```python
wr.catalog.table(
    database: 'str',
    table: 'str',
    catalog_id: 'str | None' = None,
    boto3_session: 'boto3.Session | None' = None
) -> 'pd.DataFrame'
```

Get table details as Pandas DataFrame.


:::note
This function has arguments which can be configured globally through *wr.config* or environment variables:

- catalog_id

- database

Check out the `Global Configurations Tutorial <https://github.com/aws/aws-sdk-pandas/blob/main/tutorials/021%20-%20Global%20Configurations.ipynb>`_ for details.
:::

**Parameters**

- **`database`** — Database name.
- **`table`** — Table name.
- **`catalog_id`** — The ID of the Data Catalog from which to retrieve Databases. If `None` is provided, the AWS account ID is used by default.
- **`boto3_session`** — The default boto3 session will be used if **boto3_session** receive `None`.

**Returns**

- Pandas DataFrame filled by formatted table information.

**Examples**

```python
>>> import awswrangler as wr
>>> df_table = wr.catalog.table(database='default', table='my_table')
```

---

### tables

```python
wr.catalog.tables(
    limit: 'int' = 100,
    catalog_id: 'str | None' = None,
    database: 'str | None' = None,
    search_text: 'str | None' = None,
    name_contains: 'str | None' = None,
    name_prefix: 'str | None' = None,
    name_suffix: 'str | None' = None,
    boto3_session: 'boto3.Session | None' = None
) -> 'pd.DataFrame'
```

Get a DataFrame with tables filtered by a search term, prefix, suffix.


:::note
This function has arguments which can be configured globally through *wr.config* or environment variables:

- catalog_id

- database

Check out the `Global Configurations Tutorial <https://github.com/aws/aws-sdk-pandas/blob/main/tutorials/021%20-%20Global%20Configurations.ipynb>`_ for details.
:::

**Parameters**

- **`limit`** — Max number of tables to be returned.
- **`catalog_id`** — The ID of the Data Catalog from which to retrieve Databases. If none is provided, the AWS account ID is used by default.
- **`database`** — Database name.
- **`search_text`** — Select only tables with the given string in table's properties.
- **`name_contains`** — Select by a specific string on table name
- **`name_prefix`** — Select by a specific prefix on table name
- **`name_suffix`** — Select by a specific suffix on table name
- **`boto3_session`** — The default boto3 session will be used if **boto3_session** receive `None`.

**Returns**

- Pandas DataFrame filled by formatted table information.

**Examples**

```python
>>> import awswrangler as wr
>>> df_tables = wr.catalog.tables()
```

---

### upsert_table_parameters

```python
wr.catalog.upsert_table_parameters(
    parameters: 'dict[str, str]',
    database: 'str',
    table: 'str',
    catalog_versioning: 'bool' = False,
    catalog_id: 'str | None' = None,
    boto3_session: 'boto3.Session | None' = None
) -> 'dict[str, str]'
```

Insert or Update the received parameters.


:::note
This function has arguments which can be configured globally through *wr.config* or environment variables:

- catalog_id

- database

Check out the `Global Configurations Tutorial <https://github.com/aws/aws-sdk-pandas/blob/main/tutorials/021%20-%20Global%20Configurations.ipynb>`_ for details.
:::

**Parameters**

- **`parameters`** — e.g. {"source": "mysql", "destination":  "datalake"}
- **`database`** — Database name.
- **`table`** — Table name.
- **`catalog_versioning`** — If True and `mode="overwrite"`, creates an archived version of the table catalog before updating it.
- **`catalog_id`** — The ID of the Data Catalog from which to retrieve Databases. If none is provided, the AWS account ID is used by default.
- **`boto3_session`** — Boto3 Session. The default boto3 session will be used if boto3_session receive None.

**Returns**

- All parameters after the upsert.

**Examples**

```python
>>> import awswrangler as wr
>>> pars = wr.catalog.upsert_table_parameters(
...     parameters={"source": "mysql", "destination":  "datalake"},
...     database="...",
...     table="...",
... )
```

---
