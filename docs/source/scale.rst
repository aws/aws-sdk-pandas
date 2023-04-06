At scale
---------

AWS SDK for pandas supports `Ray <https://www.ray.io/>`_ and `Modin <https://modin.readthedocs.io/en/stable/>`_, enabling you to scale your pandas workflows from a single machine to a multi-node environment, with no code changes.

The simplest way to try this is with `AWS Glue for Ray <https://aws.amazon.com/blogs/big-data/introducing-aws-glue-for-ray-scaling-your-data-integration-workloads-using-python/>`_, the new serverless option to run distributed Python code announced at AWS re:Invent 2022. AWS SDK for pandas also supports self-managed Ray on `Amazon Elastic Compute Cloud (Amazon EC2) <https://github.com/aws/aws-sdk-pandas/blob/main/tutorials/035%20-%20Distributing%20Calls%20on%20Ray%20Remote%20Cluster.ipynb>`_.

Getting Started
----------------

Install the library with the these two optional dependencies to enable distributed mode:

    >>> pip install "awswrangler[ray,modin]"

Once installed, you can use the library in your code as usual:

    >>> import awswrangler as wr

At import, SDK for pandas looks for an environmental variable called ``WR_ADDRESS``.
If found, it is used to send commands to a remote cluster.
If not found, a local Ray runtime is initialized on your machine instead.

To confirm that you are in distributed mode, run:

    >>> print(f"Execution Engine: {wr.engine.get()}")
    >>> print(f"Memory Format: {wr.memory_format.get()}")

which show that both Ray and Modin are enabled as an execution engine and memory format, respectively.

In distributed mode, the same ``awswrangler`` APIs can now handle much larger datasets:

.. code-block:: python

    # Read Parquet data (1.2 Gb Parquet compressed)
    df = wr.s3.read_parquet(
        path=f"s3://amazon-reviews-pds/parquet/product_category={category.title()}/",
    )

    # Drop the customer_id column
    df.drop("customer_id", axis=1, inplace=True)

    # Filter reviews with 5-star rating
    df5 = df[df["star_rating"] == 5]

In the example above, Amazon product data is read from Amazon S3 into a distributed `Modin data frame <https://modin.readthedocs.io/en/stable/getting_started/why_modin/pandas.html>`_.
Modin is a drop-in replacement for Pandas. It exposes the same APIs but enables you to use all of the cores on your machine, or all of the workers in an entire cluster, leading to improved performance and scale.
To use it, make sure to replace your pandas import statement with modin:

    >>> import modin.pandas as pd  # instead of import pandas as pd

Failing to do so means that all operations run on a single thread instead of leveraging the entire cluster resources.

Note that in distributed mode, all ``awswrangler`` APIs return and accept Modin data frames, not pandas.

Supported APIs
---------------

This table lists the ``awswrangler`` APIs available in distributed mode (i.e. that can run at scale):

+-------------------+------------------------------+------------------+
| Service           | API                          | Implementation   |
+===================+==============================+==================+
| ``S3``            | ``read_parquet``             |       ✅         |
+-------------------+------------------------------+------------------+
|                   | ``read_parquet_metadata``    |       ✅         |
+-------------------+------------------------------+------------------+
|                   | ``read_parquet_table``       |       ✅         |
+-------------------+------------------------------+------------------+
|                   | ``read_csv``                 |       ✅         |
+-------------------+------------------------------+------------------+
|                   | ``read_json``                |       ✅         |
+-------------------+------------------------------+------------------+
|                   | ``read_fwf``                 |       ✅         |
+-------------------+------------------------------+------------------+
|                   | ``to_parquet``               |       ✅         |
+-------------------+------------------------------+------------------+
|                   | ``to_csv``                   |       ✅         |
+-------------------+------------------------------+------------------+
|                   | ``to_json``                  |       ✅         |
+-------------------+------------------------------+------------------+
|                   | ``select_query``             |       ✅         |
+-------------------+------------------------------+------------------+
|                   | ``store_parquet_metadata``   |       ✅         |
+-------------------+------------------------------+------------------+
|                   | ``delete_objects``           |       ✅         |
+-------------------+------------------------------+------------------+
|                   | ``describe_objects``         |       ✅         |
+-------------------+------------------------------+------------------+
|                   | ``size_objects``             |       ✅         |
+-------------------+------------------------------+------------------+
|                   | ``wait_objects_exist``       |       ✅         |
+-------------------+------------------------------+------------------+
|                   | ``wait_objects_not_exist``   |       ✅         |
+-------------------+------------------------------+------------------+
|                   | ``merge_datasets``           |       ✅         |
+-------------------+------------------------------+------------------+
|                   | ``copy_objects``             |       ✅         |
+-------------------+------------------------------+------------------+
| ``Redshift``      | ``copy``                     |       ✅         |
+-------------------+------------------------------+------------------+
|                   | ``unload``                   |       ✅         |
+-------------------+------------------------------+------------------+
| ``Athena``        | ``read_sql_query``           |       ✅         |
+-------------------+------------------------------+------------------+
|                   | ``read_sql_table``           |       ✅         |
+-------------------+------------------------------+------------------+
|                   | ``describe_table``           |       ✅         |
+-------------------+------------------------------+------------------+
|                   | ``get_query_results``        |       ✅         |
+-------------------+------------------------------+------------------+
|                   | ``show_create_table``        |       ✅         |
+-------------------+------------------------------+------------------+
| ``DynamoDB``      | ``read_items``               |       ✅         |
+-------------------+------------------------------+------------------+
|                   | ``put_df``                   |       ✅         |
+-------------------+------------------------------+------------------+
|                   | ``put_csv``                  |       ✅         |
+-------------------+------------------------------+------------------+
|                   | ``put_json``                 |       ✅         |
+-------------------+------------------------------+------------------+
|                   | ``put_items``                |       ✅         |
+-------------------+------------------------------+------------------+
| ``Timestream``    | ``write``                    |       ✅         |
+-------------------+------------------------------+------------------+

Caveats
--------

S3FS Filesystem
^^^^^^^^^^^^^^^^

When Ray is chosen as an engine, `S3Fs <https://s3fs.readthedocs.io/en/latest/>`_ is used instead of boto3 for certain API calls.
These include listing a large number of S3 objects for example.
This choice was made for performance reasons as a boto3 implementation can be much slower in some cases.
As a side effect,
users won't be able to use the ``s3_additional_kwargs`` input parameter as it's currently not supported by S3Fs.

Unsupported kwargs
^^^^^^^^^^^^^^^^^^^

Most AWS SDK for pandas calls support passing the ``boto3_session`` argument.
While this is acceptable for an application running in a single process,
distributed applications require the session to be serialized and passed to the worker nodes in the cluster.
This constitutes a security risk.
As a result, passing ``boto3_session`` when using the Ray runtime is not supported.

To learn more
--------------

Read our `blog post <https://aws.amazon.com/blogs/big-data/scale-aws-sdk-for-pandas-workloads-with-aws-glue-for-ray/>`_, then head to our latest `tutorials <https://github.com/aws/aws-sdk-pandas/tree/release-3.0.0/tutorials>`_ to discover even more features.

A runbook with common errors when running the library with Ray is available `here <https://github.com/aws/aws-sdk-pandas/discussions/1815>`_.
