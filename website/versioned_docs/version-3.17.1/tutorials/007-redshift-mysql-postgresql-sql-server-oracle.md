---
id: 007-redshift-mysql-postgresql-sql-server-oracle
title: "Redshift, MySQL, PostgreSQL, SQL Server, Oracle"
sidebar_position: 7
sidebar_label: "7 - Redshift, MySQL, PostgreSQL, SQL Server, Oracle"
custom_edit_url: https://github.com/aws/aws-sdk-pandas/blob/main/tutorials/007%20-%20Redshift%2C%20MySQL%2C%20PostgreSQL%2C%20SQL%20Server%2C%20Oracle.ipynb
---

# Redshift, MySQL, PostgreSQL, SQL Server, Oracle

> This page is generated from [`tutorials/007 - Redshift, MySQL, PostgreSQL, SQL Server, Oracle.ipynb`](https://github.com/aws/aws-sdk-pandas/blob/main/tutorials/007%20-%20Redshift%2C%20MySQL%2C%20PostgreSQL%2C%20SQL%20Server%2C%20Oracle.ipynb). Open it in Jupyter to run it yourself.
[awswrangler](https://github.com/aws/aws-sdk-pandas)'s Redshift, MySQL and PostgreSQL have two basic functions in common that try to follow Pandas conventions, but add more data type consistency.

- [wr.redshift.to_sql()](https://aws-sdk-pandas.readthedocs.io/en/3.17.1/stubs/awswrangler.redshift.to_sql.html)
- [wr.redshift.read_sql_query()](https://aws-sdk-pandas.readthedocs.io/en/3.17.1/stubs/awswrangler.redshift.read_sql_query.html)
- [wr.mysql.to_sql()](https://aws-sdk-pandas.readthedocs.io/en/3.17.1/stubs/awswrangler.mysql.to_sql.html)
- [wr.mysql.read_sql_query()](https://aws-sdk-pandas.readthedocs.io/en/3.17.1/stubs/awswrangler.mysql.read_sql_query.html)
- [wr.postgresql.to_sql()](https://aws-sdk-pandas.readthedocs.io/en/3.17.1/stubs/awswrangler.postgresql.to_sql.html)
- [wr.postgresql.read_sql_query()](https://aws-sdk-pandas.readthedocs.io/en/3.17.1/stubs/awswrangler.postgresql.read_sql_query.html)
- [wr.sqlserver.to_sql()](https://aws-sdk-pandas.readthedocs.io/en/3.17.1/stubs/awswrangler.sqlserver.to_sql.html)
- [wr.sqlserver.read_sql_query()](https://aws-sdk-pandas.readthedocs.io/en/3.17.1/stubs/awswrangler.sqlserver.read_sql_query.html)
- [wr.oracle.to_sql()](https://aws-sdk-pandas.readthedocs.io/en/3.17.1/stubs/awswrangler.oracle.to_sql.html)
- [wr.oracle.read_sql_query()](https://aws-sdk-pandas.readthedocs.io/en/3.17.1/stubs/awswrangler.oracle.read_sql_query.html)

```python
# Install the optional modules first
!pip install 'awswrangler[redshift, postgres, mysql, sqlserver, oracle]'
```

```python
import pandas as pd

import awswrangler as wr

df = pd.DataFrame({"id": [1, 2], "name": ["foo", "boo"]})
```

## Connect using the Glue Catalog Connections

- [wr.redshift.connect()](https://aws-sdk-pandas.readthedocs.io/en/3.17.1/stubs/awswrangler.redshift.connect.html)
- [wr.mysql.connect()](https://aws-sdk-pandas.readthedocs.io/en/3.17.1/stubs/awswrangler.mysql.connect.html)
- [wr.postgresql.connect()](https://aws-sdk-pandas.readthedocs.io/en/3.17.1/stubs/awswrangler.postgresql.connect.html)
- [wr.sqlserver.connect()](https://aws-sdk-pandas.readthedocs.io/en/3.17.1/stubs/awswrangler.sqlserver.connect.html)
- [wr.oracle.connect()](https://aws-sdk-pandas.readthedocs.io/en/3.17.1/stubs/awswrangler.oracle.connect.html)

```python
con_redshift = wr.redshift.connect("aws-sdk-pandas-redshift")
con_mysql = wr.mysql.connect("aws-sdk-pandas-mysql")
con_postgresql = wr.postgresql.connect("aws-sdk-pandas-postgresql")
con_sqlserver = wr.sqlserver.connect("aws-sdk-pandas-sqlserver")
con_oracle = wr.oracle.connect("aws-sdk-pandas-oracle")
```

## Raw SQL queries (No Pandas)

```python
with con_redshift.cursor() as cursor:
    for row in cursor.execute("SELECT 1"):
        print(row)
```

```text
[1]
```

## Loading data to Database

```python
wr.redshift.to_sql(df, con_redshift, schema="public", table="tutorial", mode="overwrite")
wr.mysql.to_sql(df, con_mysql, schema="test", table="tutorial", mode="overwrite")
wr.postgresql.to_sql(df, con_postgresql, schema="public", table="tutorial", mode="overwrite")
wr.sqlserver.to_sql(df, con_sqlserver, schema="dbo", table="tutorial", mode="overwrite")
wr.oracle.to_sql(df, con_oracle, schema="test", table="tutorial", mode="overwrite")
```

## Unloading data from Database

```python
wr.redshift.read_sql_query("SELECT * FROM public.tutorial", con=con_redshift)
wr.mysql.read_sql_query("SELECT * FROM test.tutorial", con=con_mysql)
wr.postgresql.read_sql_query("SELECT * FROM public.tutorial", con=con_postgresql)
wr.sqlserver.read_sql_query("SELECT * FROM dbo.tutorial", con=con_sqlserver)
wr.oracle.read_sql_query("SELECT * FROM test.tutorial", con=con_oracle)
```

```text
   id name
0   1  foo
1   2  boo
```

```python
con_redshift.close()
con_mysql.close()
con_postgresql.close()
con_sqlserver.close()
con_oracle.close()
```
