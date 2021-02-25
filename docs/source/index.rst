An `AWS Professional Service <https://aws.amazon.com/professional-services>`_ open source initiative | aws-proserve-opensource@amazon.com

Quick Start
-----------

    >>> pip install awswrangler

.. code-block:: py3

    import awswrangler as wr
    import pandas as pd
    from datetime import datetime

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

    # Get a Redshift connection from Glue Catalog and retrieving data from Redshift Spectrum
    con = wr.redshift.connect("my-glue-connection")
    df = wr.redshift.read_sql_query("SELECT * FROM external_schema.my_table", con=con)
    con.close()

    # Amazon Timestream Write
    df = pd.DataFrame({
        "time": [datetime.now(), datetime.now()],   
        "my_dimension": ["foo", "boo"],
        "measure": [1.0, 1.1],
    })
    rejected_records = wr.timestream.write(df,
        database="sampleDB",
        table="sampleTable",
        time_col="time",
        measure_col="measure",
        dimensions_cols=["my_dimension"],
    )

    # Amazon Timestream Query
    wr.timestream.query("""
    SELECT time, measure_value::double, my_dimension
    FROM "sampleDB"."sampleTable" ORDER BY time DESC LIMIT 3
    """)

Read The Docs
-------------

.. toctree::
   :maxdepth: 2

   what
   install
   tutorials
   api
   Community Resources <https://github.com/awslabs/aws-data-wrangler#community-resources>
   Logging <https://github.com/awslabs/aws-data-wrangler#logging>
   Who uses AWS Data Wrangler? <https://github.com/awslabs/aws-data-wrangler#who-uses-aws-data-wrangler>
   License <https://github.com/awslabs/aws-data-wrangler/blob/main/LICENSE.txt>
   Contributing <https://github.com/awslabs/aws-data-wrangler/blob/main/CONTRIBUTING.md>
   Legacy Docs (pre-1.0.0) <https://aws-data-wrangler.readthedocs.io/en/legacy/>
