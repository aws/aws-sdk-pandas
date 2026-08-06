---
id: 020-spark-table-interoperability
title: "Spark Table Interoperability"
sidebar_position: 20
sidebar_label: "20 - Spark Table Interoperability"
custom_edit_url: https://github.com/aws/aws-sdk-pandas/blob/main/tutorials/020%20-%20Spark%20Table%20Interoperability.ipynb
---

# Spark Table Interoperability

> This page is generated from [`tutorials/020 - Spark Table Interoperability.ipynb`](https://github.com/aws/aws-sdk-pandas/blob/main/tutorials/020%20-%20Spark%20Table%20Interoperability.ipynb). Open it in Jupyter to run it yourself.
[awswrangler](https://github.com/aws/aws-sdk-pandas) has no difficulty to insert, overwrite or do any other kind of interaction with a Table created by Apache Spark.

But if you want to do the opposite (Spark interacting with a table created by awswrangler) you should be aware that awswrangler follows the Hive's format and you must be explicit when using the Spark's `saveAsTable` method:

```python
spark_df.write.format("hive").saveAsTable("database.table")
```

Or just move forward using the `insertInto` alternative:

```python
spark_df.write.insertInto("database.table")
```
