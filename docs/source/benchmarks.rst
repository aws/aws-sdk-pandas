.. _doc_benchmarks:

Benchmark
============

**AWS Data Wrangler** and **Apache Spark** can be used to execute data transformations, but they have different targets.

Spark already has a good sinergy with the AWS ecosystem, but is focused on big data workloads and brings unnecessary overhead and complexity for "small" data.

AWS Wrangler aims to fill this gap and help to move small data through the AWS ecosystem with efficiency simplicity.

So this uncompromised orange and apple comparision is only to demonstrate that there are a right tool for each job.

Methodology
-----------

We will compare three different ETL **serverless** approaches.

- AWS Glue (2 DPU) + Pyspark
- AWS Glue (1 DPU) + AWS Wrangler
- AWS Lambda (3 GB) + AWS Wrangler

The tested ETL consist in read a aleatory generated CSV file (With sizes between 16 to 4096 MB) in S3 and then write it back as partitioned Parquet.

`SOURCE <https://github.com/awslabs/aws-data-wrangler/tree/master/benchmarks/serverless_etl>`_

P.S: The AWS Lambda approach can't handle the workloads bigger tem 1 GB.

.. figure:: _static/report_cost.png
    :align: center
    :alt: alternate text
    :figclass: align-center

    Cost (Less is better)

.. figure:: _static/report_execution_time.png
    :align: center
    :alt: alternate text
    :figclass: align-center

    Execution Time (Less is better)

.. figure:: _static/report_total_time.png
    :align: center
    :alt: alternate text
    :figclass: align-center

    Warm Up + Execution Time (Less is better)