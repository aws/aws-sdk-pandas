An `AWS Professional Service <https://aws.amazon.com/professional-services>`_ open source initiative | aws-proserve-opensource@amazon.com

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

    # Get Redshift connection (SQLAlchemy) from Glue and retrieving data from Redshift Spectrum
    engine = wr.catalog.get_engine("my-redshift-connection")
    df = wr.db.read_sql_query("SELECT * FROM external_schema.my_table", con=engine)

    # Get MySQL connection (SQLAlchemy) from Glue Catalog and LOAD the data into MySQL
    engine = wr.catalog.get_engine("my-mysql-connection")
    wr.db.to_sql(df, engine, schema="test", name="my_table")

    # Get PostgreSQL connection (SQLAlchemy) from Glue Catalog and LOAD the data into PostgreSQL
    engine = wr.catalog.get_engine("my-postgresql-connection")
    wr.db.to_sql(df, engine, schema="test", name="my_table")

Read The Docs
-------------

.. toctree::
   :maxdepth: 2

   what
   install
   Tutorials <https://github.com/awslabs/aws-data-wrangler/tree/master/tutorials>
   api
   Community Resources <https://github.com/awslabs/aws-data-wrangler#community-resources>
   Who uses AWS Data Wrangler? <https://github.com/awslabs/aws-data-wrangler#who-uses-aws-data-wrangler>
   License <https://github.com/awslabs/aws-data-wrangler/blob/master/LICENSE.txt>
   Contributing <https://github.com/awslabs/aws-data-wrangler/blob/master/CONTRIBUTING.md>
   Legacy Docs (pre-1.0.0) <https://aws-data-wrangler.readthedocs.io/en/legacy/>
