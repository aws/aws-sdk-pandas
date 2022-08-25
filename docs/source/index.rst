An `AWS Professional Service <https://aws.amazon.com/professional-services>`_ open source initiative | aws-proserve-opensource@amazon.com

AWS Data Wrangler is now **AWS SDK for pandas (awswrangler)**.  We’re changing the name we use when we talk about the library, but everything else will stay the same.  You’ll still be able to install using :code:`pip install awswrangler` and you won’t need to change any of your code.  As part of this change, we’ve moved the library from AWS Labs to the main AWS GitHub organisation but, thanks to the GitHub’s redirect feature, you’ll still be able to access the project by its old URLs until you update your bookmarks.  Our documentation has also moved to `aws-sdk-pandas.readthedocs.io <https://aws-sdk-pandas.readthedocs.io>`_, but old bookmarks will redirect to the new site.

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
   Community Resources <https://github.com/aws/aws-sdk-pandas#community-resources>
   Logging <https://github.com/aws/aws-sdk-pandas#logging>
   Who uses AWS SDK for pandas? <https://github.com/aws/aws-sdk-pandas#who-uses-aws-sdk-pandas>
   License <https://github.com/aws/aws-sdk-pandas/blob/main/LICENSE.txt>
   Contributing <https://github.com/aws/aws-sdk-pandas/blob/main/CONTRIBUTING.md>

.. image:: https://d3tiqpr4kkkomd.cloudfront.net/img/pixel.png?asset=RIXAH6KDSYAI1HHEBLTY
   :align: left
