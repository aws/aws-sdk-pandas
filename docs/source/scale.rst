At scale
=========

AWS SDK for pandas supports `Ray <https://www.ray.io/>`_ and `Modin <https://modin.readthedocs.io/en/stable/>`_, enabling you to scale your pandas workflows from a single machine to a multi-node environment, with no code changes.

The simplest way to try this is with `AWS Glue for Ray <https://aws.amazon.com/blogs/big-data/introducing-aws-glue-for-ray-scaling-your-data-integration-workloads-using-python/>`_, the new serverless option to run distributed Python code announced at AWS re:Invent 2022. AWS SDK for pandas also supports self-managed Ray on `Amazon Elastic Compute Cloud (Amazon EC2) <https://github.com/aws/aws-sdk-pandas/blob/main/tutorials/035%20-%20Distributing%20Calls%20on%20Ray%20Remote%20Cluster.ipynb>`_.

Getting Started
----------------

Install the library with the these two optional dependencies to enable distributed mode:

    >>> pip install "awswrangler[ray,modin]"

Once installed, you can use the library in your code as usual:

    >>> import awswrangler as wr

At import, SDK for pandas checks if ``ray`` and ``modin`` are in the installation path and enables distributed mode.
To confirm that you are in distributed mode, run:

    >>> print(f"Execution Engine: {wr.engine.get()}")
    >>> print(f"Memory Format: {wr.memory_format.get()}")

which show that both Ray and Modin are enabled as an execution engine and memory format, respectively.
You can switch back to non-distributed mode at any point (See `Switching modes <scale.rst#switching-modes>`__ below).

Initialization of the Ray cluster is lazy and only triggered when the first distributed API is executed.
At that point, SDK for pandas looks for an environment variable called ``WR_ADDRESS``.
If found, it is used to send commands to a remote cluster.
If not found, a local Ray runtime is initialized on your machine instead.
Alternatively, you can trigger Ray initialization with:

    >>> wr.engine.initialize()

In distributed mode, the same ``awswrangler`` APIs can now handle much larger datasets:

.. code-block:: python

    # Read 1.6 Gb Parquet data
    df = wr.s3.read_parquet(path="s3://ursa-labs-taxi-data/2017/")

    # Drop vendor_id column
    df.drop("vendor_id", axis=1, inplace=True)

    # Filter trips over 1 mile
    df1 = df[df["trip_distance"] > 1]

In the example above, New York City Taxi data is read from Amazon S3 into a distributed `Modin data frame <https://modin.readthedocs.io/en/stable/getting_started/why_modin/pandas.html>`_.
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
| ``Athena``        | ``describe_table``           |       ✅         |
+-------------------+------------------------------+------------------+
|                   | ``get_query_results``        |       ✅         |
+-------------------+------------------------------+------------------+
|                   | ``read_sql_query``           |       ✅         |
+-------------------+------------------------------+------------------+
|                   | ``read_sql_table``           |       ✅         |
+-------------------+------------------------------+------------------+
|                   | ``show_create_table``        |       ✅         |
+-------------------+------------------------------+------------------+
|                   | ``to_iceberg``               |       ✅         |
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
| ``Lake Formation``| ``read_sql_query``           |       ✅         |
+-------------------+------------------------------+------------------+
|                   | ``read_sql_table``           |       ✅         |
+-------------------+------------------------------+------------------+
| ``Neptune``       | ``bulk_load``                |       ✅         |
+-------------------+------------------------------+------------------+
| ``Timestream``    | ``batch_load``               |       ✅         |
+-------------------+------------------------------+------------------+
|                   | ``write``                    |       ✅         |
+-------------------+------------------------------+------------------+
|                   | ``unload``                   |       ✅         |
+-------------------+------------------------------+------------------+

Switching modes
----------------
The following commands showcase how to switch between distributed and non-distributed modes:

.. code-block:: python

    # Switch to non-distributed
    wr.engine.set("python")
    wr.memory_format.set("pandas")

    # Switch to distributed
    wr.engine.set("ray")
    wr.memory_format.set("modin")

Similarly, you can set the ``WR_ENGINE`` and ``WR_MEMORY_FORMAT`` environment variables
to the desired engine and memory format, respectively.

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

Read our blog posts `(1) <https://aws.amazon.com/blogs/big-data/scale-aws-sdk-for-pandas-workloads-with-aws-glue-for-ray/>`_ and `(2) <https://aws.amazon.com/blogs/big-data/advanced-patterns-with-aws-sdk-for-pandas-on-aws-glue-for-ray/>`_, then head to our latest `tutorials <https://aws-sdk-pandas.readthedocs.io/en/stable/tutorials.html>`_ to discover even more features.

A runbook with common errors when running the library with Ray is available `here <https://github.com/aws/aws-sdk-pandas/discussions/1815>`_.
