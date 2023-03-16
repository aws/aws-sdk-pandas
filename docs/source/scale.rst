At scale
-----------

AWS SDK for pandas supports `Ray <https://www.ray.io/>`_ and `Modin <https://modin.readthedocs.io/en/stable/>`_, enabling you to scale your pandas workflows from a single machine to a multi-node environment, with no code changes.

The simplest way to do this is to use `AWS Glue for Ray <https://aws.amazon.com/blogs/big-data/introducing-aws-glue-for-ray-scaling-your-data-integration-workloads-using-python/>`_, the new serverless option to run distributed Python code announced at AWS re:Invent 2022. AWS SDK for pandas also supports self-managed Ray on `Amazon Elastic Compute Cloud (Amazon EC2) <https://aws.amazon.com/ec2/>`_.

Getting Started
-------------

Install the library with the following optional dependencies:

    >>> pip install "awswrangler[ray,modin]"

Once installed, you can use the library in your code by importing it with the following statement:

    >>> import awswrangler as wr

When you run this code, the SDK for pandas looks for an environmental variable called `WR_ADDRESS`. If it finds it, it uses this value to send the commands to a remote cluster. If it doesn’t find it, it starts a local Ray runtime on your machine.

Finally, let's read Amazon product data in Parquet format from Amazon S3 and load it into a distributed Modin data frame and apply a few transformations:

.. code-block:: python

    # Read Parquet data (1.2 Gb Parquet compressed)
    df = wr.s3.read_parquet(
        path=f"s3://amazon-reviews-pds/parquet/product_category={category.title()}/",
    )

    # Drop the customer_id column
    df.drop("customer_id", axis=1, inplace=True)

    # Filter reviews with 5-star rating
    df5 = df[df["star_rating"] == 5]

Supported APIs
--------------------------

+-----------------+--------------------------+----------------+
| Service         | API                      | Implementation |
+=================+==========================+================+
| `S3`            | `read_parquet`           |       ✅       |
+-----------------+--------------------------+----------------+
|                 | `read_parquet_metadata`  |       ✅       |
+-----------------+--------------------------+----------------+
|                 | `read_parquet_table`     |       ✅       |
+-----------------+--------------------------+----------------+
|                 | `read_csv`               |       ✅       |
+-----------------+--------------------------+----------------+
|                 | `read_json`              |       ✅       |
+-----------------+--------------------------+----------------+
|                 | `read_fwf`               |       ✅       |
+-----------------+--------------------------+----------------+
|                 | `to_parquet`             |       ✅       |
+-----------------+--------------------------+----------------+
|                 | `to_csv`                 |       ✅       |
+-----------------+--------------------------+----------------+
|                 | `to_json`                |       ✅       |
+-----------------+--------------------------+----------------+
|                 | `select_query`           |       ✅       |
+-----------------+--------------------------+----------------+
|                 | `store_parquet_metadata` |       ✅       |
+-----------------+--------------------------+----------------+
|                 | `delete_objects`         |       ✅       |
+-----------------+--------------------------+----------------+
|                 | `describe_objects`       |       ✅       |
+-----------------+--------------------------+----------------+
|                 | `size_objects`           |       ✅       |
+-----------------+--------------------------+----------------+
|                 | `wait_objects_exist`     |       ✅       |
+-----------------+--------------------------+----------------+
|                 | `wait_objects_not_exist` |       ✅       |
+-----------------+--------------------------+----------------+
|                 | `merge_datasets`         |       ✅       |
+-----------------+--------------------------+----------------+
|                 | `copy_objects`           |       ✅       |
+-----------------+--------------------------+----------------+
| `Redshift`      | `copy`                   |       ✅       |
+-----------------+--------------------------+----------------+
|                 | `unload`                 |       ✅       |
+-----------------+--------------------------+----------------+
| `Athena`        | `read_sql_query`         |       ✅       |
+-----------------+--------------------------+----------------+
|                 | `read_sql_table`         |       ✅       |
+-----------------+--------------------------+----------------+
|                 | `describe_table`         |       ✅       |
+-----------------+--------------------------+----------------+
|                 | `get_query_results`      |       ✅       |
+-----------------+--------------------------+----------------+
|                 | `show_create_table`      |       ✅       |
+-----------------+--------------------------+----------------+
| `DynamoDB`      | `read_items`             |       ✅       |
+-----------------+--------------------------+----------------+
| `LakeFormation` | `read_sql_query`         |       ✅       |
+-----------------+--------------------------+----------------+
|                 | `read_sql_table`         |       ✅       |
+-----------------+--------------------------+----------------+
| `Timestream`    | `write`                  |       ✅       |
+-----------------+--------------------------+----------------+

Resources
--------------------------

Read our `blog post <https://aws.amazon.com/blogs/big-data/scale-aws-sdk-for-pandas-workloads-with-aws-glue-for-ray/>`_, then head to our latest `tutorials <https://github.com/aws/aws-sdk-pandas/tree/release-3.0.0/tutorials>`_ to discover even more features.