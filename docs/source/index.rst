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
    # Retrieving the data from Amazon Redshift Spectrum
    engine = wr.catalog.get_engine("my-redshift-connection")
    df = wr.db.read_sql_query("SELECT * FROM external_schema.my_table", con=engine)

    # Creating QuickSight Data Source and Dataset to reflect our new table
    wr.quicksight.create_athena_data_source("athena-source", allowed_to_manage=["username"])
    wr.quicksight.create_athena_dataset(
        name="my-dataset",
        database="my_db",
        table="my_table",
        data_source_name="athena-source",
        allowed_to_manage=["username"]
    )

    # Getting MySQL connection (SQLAlchemy) from Glue Catalog Connections
    # Load the data into MySQL
    engine = wr.catalog.get_engine("my-mysql-connection")
    wr.db.to_sql(df, engine, schema="test", name="my_table")

    # Getting PostgreSQL connection (SQLAlchemy) from Glue Catalog Connections
    # Load the data into PostgreSQL
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
   License <https://github.com/awslabs/aws-data-wrangler/blob/master/LICENSE>
   Contributing <https://github.com/awslabs/aws-data-wrangler/blob/master/CONTRIBUTING.md>
   Legacy Docs (pre-1.0.0) <https://aws-data-wrangler.readthedocs.io/en/legacy/>
