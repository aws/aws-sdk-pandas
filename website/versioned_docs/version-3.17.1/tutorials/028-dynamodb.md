---
id: 028-dynamodb
title: "DynamoDB"
sidebar_position: 28
sidebar_label: "28 - DynamoDB"
custom_edit_url: https://github.com/aws/aws-sdk-pandas/blob/main/tutorials/028%20-%20DynamoDB.ipynb
---

# DynamoDB

> This page is generated from [`tutorials/028 - DynamoDB.ipynb`](https://github.com/aws/aws-sdk-pandas/blob/main/tutorials/028%20-%20DynamoDB.ipynb). Open it in Jupyter to run it yourself.
## Writing Data

```python
from datetime import datetime
from decimal import Decimal
from pathlib import Path

import pandas as pd
from boto3.dynamodb.conditions import Attr, Key

import awswrangler as wr
```

### Writing DataFrame

```python
table_name = "movies"

df = pd.DataFrame(
    {
        "title": ["Titanic", "Snatch", "The Godfather"],
        "year": [1997, 2000, 1972],
        "genre": ["drama", "caper story", "crime"],
    }
)
wr.dynamodb.put_df(df=df, table_name=table_name)
```

### Writing CSV file

```python
filepath = Path("items.csv")
df.to_csv(filepath, index=False)
wr.dynamodb.put_csv(path=filepath, table_name=table_name)
filepath.unlink()
```

### Writing JSON files

```python
filepath = Path("items.json")
df.to_json(filepath, orient="records")
wr.dynamodb.put_json(path="items.json", table_name=table_name)
filepath.unlink()
```

### Writing list of items

```python
items = df.to_dict(orient="records")
wr.dynamodb.put_items(items=items, table_name=table_name)
```

## Reading Data

### Read Items

```python
# Limit Read to 5 items
wr.dynamodb.read_items(table_name=table_name, max_items_evaluated=5)

# Limit Read to Key expression
wr.dynamodb.read_items(
    table_name=table_name, key_condition_expression=(Key("title").eq("Snatch") & Key("year").eq(2000))
)
```

### Read PartiQL

```python
wr.dynamodb.read_partiql_query(
    query=f"SELECT * FROM {table_name} WHERE title=? AND year=?",
    parameters=["Snatch", 2000],
)
```

```text
   year        genre   title
0  2000  caper story  Snatch
```

## Executing statements

```python
title = "The Lord of the Rings: The Fellowship of the Ring"
year = datetime.now().year
genre = "epic"
rating = Decimal("9.9")
plot = "The fate of Middle-earth hangs in the balance as Frodo and eight companions begin their journey to Mount Doom in the land of Mordor."

# Insert items
wr.dynamodb.execute_statement(
    statement=f"INSERT INTO {table_name} VALUE {{'title': ?, 'year': ?, 'genre': ?, 'info': ?}}",
    parameters=[title, year, genre, {"plot": plot, "rating": rating}],
)

# Select items
wr.dynamodb.execute_statement(
    statement=f'SELECT * FROM "{table_name}" WHERE title=? AND year=?',
    parameters=[title, year],
)

# Update items
wr.dynamodb.execute_statement(
    statement=f'UPDATE "{table_name}" SET info.rating=? WHERE title=? AND year=?',
    parameters=[Decimal(10), title, year],
)

# Delete items
wr.dynamodb.execute_statement(
    statement=f'DELETE FROM "{table_name}" WHERE title=? AND year=?',
    parameters=[title, year],
)
```

```text
[]
```

## Deleting items

```python
wr.dynamodb.delete_items(items=items, table_name="table")
```
