.. note:: Due the new major version 1.*.* with breaking changes, please make sure that all your old projects has dependencies frozen on the desired version (e.g. `pip install awswrangler==0.3.2`).

Quick Start
-----------

    >>> pip install awswrangler

.. code-block:: py3

    import awswrangler as wr
    import pandas as pd

    df = pd.DataFrame({"id": [1, 2], "value": ["foo", "boo"]})

    # Storing data on Data Lake
    wr.s3.to_parquet(
        df=df,
        path="s3://bucket/dataset/",
        dataset=True,
        database="my_db",
        table="my_table"
    )

    # Retrieving the data directly from Amazon S3
    df = wr.s3.read_parquet("s3://bucket/dataset/", dataset=True)

    # Retrieving the data from Amazon Athena
    df = wr.athena.read_sql_query("SELECT * FROM my_table", database="my_db")

    # Getting Redshift connection (SQLAlchemy) from Glue Catalog Connections
    engine = wr.catalog.get_engine("my-redshift-connection")

    # Retrieving the data from Amazon Redshift Spectrum
    df = wr.db.read_sql_query("SELECT * FROM external_schema.my_table", con=engine)

Read The Docs
-------------

.. toctree::
   :maxdepth: 2

   what
   install
   Tutorials <https://github.com/awslabs/aws-data-wrangler/tree/master/tutorials>
   api
   License <https://github.com/awslabs/aws-data-wrangler/blob/master/LICENSE>
   Contributing <https://github.com/awslabs/aws-data-wrangler/blob/master/CONTRIBUTING.md>
