---
id: 006-amazon-athena
title: "Amazon Athena"
sidebar_position: 6
sidebar_label: "6 - Amazon Athena"
custom_edit_url: https://github.com/aws/aws-sdk-pandas/blob/main/tutorials/006%20-%20Amazon%20Athena.ipynb
---

# Amazon Athena

> This page is generated from [`tutorials/006 - Amazon Athena.ipynb`](https://github.com/aws/aws-sdk-pandas/blob/main/tutorials/006%20-%20Amazon%20Athena.ipynb). Open it in Jupyter to run it yourself.
[awswrangler](https://github.com/aws/aws-sdk-pandas) has three ways to run queries on Athena and fetch the result as a DataFrame:

- **ctas_approach=True** (Default)

    Wraps the query with a CTAS and then reads the table data as parquet directly from s3.
    
    * `PROS`:
        - Faster for mid and big result sizes.
        - Can handle some level of nested types.
    * `CONS`:
         - Requires create/delete table permissions on Glue.
         - Does not support timestamp with time zone
         - Does not support columns with repeated names.
         - Does not support columns with undefined data types.
         - A temporary table will be created and then deleted immediately.
         - Does not support custom data_source/catalog_id.

- **unload_approach=True and ctas_approach=False**

    Does an UNLOAD query on Athena and parse the Parquet result on s3.

    * `PROS`:
        - Faster for mid and big result sizes.
        - Can handle some level of nested types.
        - Does not modify Glue Data Catalog.
    * `CONS`:
        - Output S3 path must be empty.
        - Does not support timestamp with time zone
        - Does not support columns with repeated names.
        - Does not support columns with undefined data types.

- **ctas_approach=False**

    Does a regular query on Athena and parse the regular CSV result on s3.
    
    * `PROS`:
        - Faster for small result sizes (less latency).
        - Does not require create/delete table permissions on Glue
        - Supports timestamp with time zone.
        - Support custom data_source/catalog_id.
    * `CONS`:
        - Slower (But stills faster than other libraries that uses the regular Athena API)
        - Does not handle nested types at all.

```python
import awswrangler as wr
```

## Enter your bucket name:

```python
import getpass

bucket = getpass.getpass()
path = f"s3://{bucket}/data/"
```

## Checking/Creating Glue Catalog Databases

```python
if "awswrangler_test" not in wr.catalog.databases().values:
    wr.catalog.create_database("awswrangler_test")
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

```python
wr.s3.to_parquet(df=df, path=path, dataset=True, mode="overwrite", database="awswrangler_test", table="noaa")
```

```python
wr.catalog.table(database="awswrangler_test", table="noaa")
```

## Reading with ctas_approach=False

```python
%%time

wr.athena.read_sql_query("SELECT * FROM noaa", database="awswrangler_test", ctas_approach=False)
```

## Default with ctas_approach=True - 13x faster (default)

```python
%%time

wr.athena.read_sql_query("SELECT * FROM noaa", database="awswrangler_test")
```

## Using categories to speed up and save memory - 24x faster

```python
%%time

wr.athena.read_sql_query(
    "SELECT * FROM noaa",
    database="awswrangler_test",
    categories=["id", "dt", "element", "value", "m_flag", "q_flag", "s_flag", "obs_time"],
)
```

## Reading with unload_approach=True

```python
%%time

wr.athena.read_sql_query(
    "SELECT * FROM noaa",
    database="awswrangler_test",
    ctas_approach=False,
    unload_approach=True,
    s3_output=f"s3://{bucket}/unload/",
)
```

## Batching (Good for restricted memory environments)

```python
%%time

dfs = wr.athena.read_sql_query(
    "SELECT * FROM noaa",
    database="awswrangler_test",
    chunksize=True,  # Chunksize calculated automatically for ctas_approach.
)

for df in dfs:  # Batching
    print(len(df.index))
```

```python
%%time

dfs = wr.athena.read_sql_query("SELECT * FROM noaa", database="awswrangler_test", chunksize=100_000_000)

for df in dfs:  # Batching
    print(len(df.index))
```

## Parameterized queries

### Client-side parameter resolution

The `params` parameter allows client-side resolution of parameters, which are specified with `:col_name`, when `paramstyle` is set to `named`.
Additionally, Python types will map to the appropriate Athena definitions.
For example, the value `dt.date(2023, 1, 1)` will resolve to `DATE '2023-01-01`.

For the example below, the following query will be sent to Athena:
```sql
SELECT * FROM noaa WHERE S_FLAG = 'E'
```

```python
%%time

wr.athena.read_sql_query(
    "SELECT * FROM noaa WHERE S_FLAG = :flag_value",
    database="awswrangler_test",
    params={
        "flag_value": "E",
    },
)
```

### Server-side parameter resolution

Alternatively, Athena supports server-side parameter resolution when `paramstyle` is defined as `qmark`.
The SQL statement sent to Athena will not contain the values passed in `params`.
Instead, they will be passed as part of a separate `params` parameter in `boto3`.

The downside of using this approach is that types aren't automatically resolved.
The values sent to `params` must be strings.
Therefore, if one of the values is a date, the value passed in `params` has to be `DATE 'XXXX-XX-XX'`.

The upside, however, is that these parameters can be used with prepared statements.

For more information, see "[Using parameterized queries](https://docs.aws.amazon.com/athena/latest/ug/querying-with-prepared-statements.html)".

```python
%%time

wr.athena.read_sql_query(
    "SELECT * FROM noaa WHERE S_FLAG = ?",
    database="awswrangler_test",
    params=["E"],
    paramstyle="qmark",
)
```

## Prepared statements

```python
wr.athena.create_prepared_statement(
    sql="SELECT * FROM noaa WHERE S_FLAG = ?",
    statement_name="statement",
)

# Resolve parameter using Athena execution parameters
wr.athena.read_sql_query(
    sql="EXECUTE statement",
    database="awswrangler_test",
    params=["E"],
    paramstyle="qmark",
)

# Resolve parameter using Athena execution parameters (same effect as above)
wr.athena.read_sql_query(
    sql="EXECUTE statement USING ?",
    database="awswrangler_test",
    params=["E"],
    paramstyle="qmark",
)

# Resolve parameter using client-side formatter
wr.athena.read_sql_query(
    sql="EXECUTE statement USING :flag_value",
    database="awswrangler_test",
    params={
        "flag_value": "E",
    },
    paramstyle="named",
)
```

```python
# Clean up prepared statement
wr.athena.delete_prepared_statement(statement_name="statement")
```

## Cleaning Up S3

```python
wr.s3.delete_objects(path)
```

## Delete table

```python
wr.catalog.delete_table_if_exists(database="awswrangler_test", table="noaa")
```

## Delete Database

```python
wr.catalog.delete_database("awswrangler_test")
```
