---
id: 005-glue-catalog
title: "Glue Catalog"
sidebar_position: 5
sidebar_label: "5 - Glue Catalog"
custom_edit_url: https://github.com/aws/aws-sdk-pandas/blob/main/tutorials/005%20-%20Glue%20Catalog.ipynb
---

# Glue Catalog

> This page is generated from [`tutorials/005 - Glue Catalog.ipynb`](https://github.com/aws/aws-sdk-pandas/blob/main/tutorials/005%20-%20Glue%20Catalog.ipynb). Open it in Jupyter to run it yourself.
[awswrangler](https://github.com/aws/aws-sdk-pandas) makes heavy use of [Glue Catalog](https://aws.amazon.com/glue/) to store metadata of tables and connections.

```python
import pandas as pd

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

### Creating a Pandas DataFrame

```python
df = pd.DataFrame(
    {"id": [1, 2, 3], "name": ["shoes", "tshirt", "ball"], "price": [50.3, 10.5, 20.0], "in_stock": [True, True, False]}
)
df
```

```text
   id    name  price  in_stock
0   1   shoes   50.3      True
1   2  tshirt   10.5      True
2   3    ball   20.0     False
```

## Checking Glue Catalog Databases

```python
databases = wr.catalog.databases()
print(databases)
```

```text
            Database                                   Description
0  aws_sdk_pandas  AWS SDK for pandas Test Arena - Glue Database
1            default                         Default Hive database
```

### Create the database awswrangler_test if not exists

```python
if "awswrangler_test" not in databases.values:
    wr.catalog.create_database("awswrangler_test")
    print(wr.catalog.databases())
else:
    print("Database awswrangler_test already exists")
```

```text
            Database                                   Description
0  aws_sdk_pandas  AWS SDK for pandas Test Arena - Glue Database
1   awswrangler_test                                              
2            default                         Default Hive database
```

## Checking the empty database

```python
wr.catalog.tables(database="awswrangler_test")
```

```text
Empty DataFrame
Columns: [Database, Table, Description, Columns, Partitions]
Index: []
```

### Writing DataFrames to Data Lake (S3 + Parquet + Glue Catalog)

```python
desc = "This is my product table."

param = {"source": "Product Web Service", "class": "e-commerce"}

comments = {
    "id": "Unique product ID.",
    "name": "Product name",
    "price": "Product price (dollar)",
    "in_stock": "Is this product availaible in the stock?",
}

res = wr.s3.to_parquet(
    df=df,
    path=f"s3://{bucket}/products/",
    dataset=True,
    database="awswrangler_test",
    table="products",
    mode="overwrite",
    glue_table_settings=wr.typing.GlueTableSettings(description=desc, parameters=param, columns_comments=comments),
)
```

### Checking Glue Catalog (AWS Console)

![Glue Console](/img/tutorials/glue_catalog_table_products.png "Glue Console")

### Looking Up for the new table!

```python
wr.catalog.tables(name_contains="roduc")
```

```text
           Database     Table                Description  \
0  awswrangler_test  products  This is my product table.   

                     Columns Partitions  
0  id, name, price, in_stock
```

```python
wr.catalog.tables(name_prefix="pro")
```

```text
           Database     Table                Description  \
0  awswrangler_test  products  This is my product table.   

                     Columns Partitions  
0  id, name, price, in_stock
```

```python
wr.catalog.tables(name_suffix="ts")
```

```text
           Database     Table                Description  \
0  awswrangler_test  products  This is my product table.   

                     Columns Partitions  
0  id, name, price, in_stock
```

```python
wr.catalog.tables(search_text="This is my")
```

```text
           Database     Table                Description  \
0  awswrangler_test  products  This is my product table.   

                     Columns Partitions  
0  id, name, price, in_stock
```

### Getting tables details

```python
wr.catalog.table(database="awswrangler_test", table="products")
```

```text
  Column Name     Type  Partition                                   Comment
0          id   bigint      False                        Unique product ID.
1        name   string      False                              Product name
2       price   double      False                    Product price (dollar)
3    in_stock  boolean      False  Is this product availaible in the stock?
```

## Cleaning Up the Database

```python
for table in wr.catalog.get_tables(database="awswrangler_test"):
    wr.catalog.delete_table_if_exists(database="awswrangler_test", table=table["Name"])
```

### Delete Database

```python
wr.catalog.delete_database("awswrangler_test")
```
