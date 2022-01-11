Install
=======

**AWS Data Wrangler** runs with Python ``3.6``, ``3.7``, ``3.8`` and ``3.9``
and on several platforms (AWS Lambda, AWS Glue Python Shell, EMR, EC2,
on-premises, Amazon SageMaker, local, etc).

Some good practices for most of the methods below are:

  - Use new and individual Virtual Environments for each project (`venv <https://docs.python.org/3/library/venv.html>`_).
  - On Notebooks, always restart your kernel after installations.

.. note:: If you want to use ``awswrangler`` for connecting to Microsoft SQL Server, some additional configuration is needed. Please have a look at the corresponding section below.

PyPI (pip)
----------

    >>> pip install awswrangler

Conda
-----

    >>> conda install -c conda-forge awswrangler

AWS Lambda Layer
----------------

Managed Layer
^^^^^^^^^^^^^^

AWS Data Wrangler is available as a Lambda Managed layer in the following regions:

- ap-northeast-1
- ap-southeast-2
- eu-central-1
- eu-west-1
- us-east-1
- us-east-2
- us-west-2

It can be accessed in the AWS Lambda console directly:

.. image:: _static/aws_lambda_managed_layer.png
  :width: 400
  :alt: AWS Managed Lambda Layer

Or via its arn: ``arn:aws:lambda:<region>:336392948345:layer:AWSDataWrangler-Python<version>:<layer-version>``.
For example: ``arn:aws:lambda:us-east-1:336392948345:layer:AWSDataWrangler-Python37:1``.
Both Python 3.7 and 3.8 are supported.

Custom Layer
^^^^^^^^^^^^^^

For AWS regions not in the above list, you can create your own Lambda layer following these instructions:

1 - Go to `GitHub's release section <https://github.com/awslabs/aws-data-wrangler/releases>`_
and download the layer zip related to the desired version. Alternatively, you can download the zip from the `public artifacts bucket <https://aws-data-wrangler.readthedocs.io/en/latest/install.html#public-artifacts>`_.

2 - Go to the AWS Lambda Panel, open the layer section (left side)
and click **create layer**.

3 - Set name and python version, upload your fresh downloaded zip file
and press **create** to create the layer.

4 - Go to your Lambda and select your new layer!

Serverless Application Repository (SAR)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

AWS Data Wrangler layers are also available in the `AWS Serverless Application Repository <https://serverlessrepo.aws.amazon.com/applications>`_ (SAR).

The app deploys the Lambda layer version in your own AWS account and region via a CloudFormation stack.
This option provides the ability to use semantic versions (i.e. library version) instead of Lambda layer versions. 

.. list-table:: AWS Data Wrangler Layer Apps
   :widths: 25 25 50
   :header-rows: 1

   * - App
     - ARN
     - Description
   * - aws-data-wrangler-layer-py3-7
     - arn:aws:serverlessrepo:us-east-1:336392948345:applications/aws-data-wrangler-layer-py3-7
     - Layer for ``Python 3.7.x`` runtimes
   * - aws-data-wrangler-layer-py3-8
     - arn:aws:serverlessrepo:us-east-1:336392948345:applications/aws-data-wrangler-layer-py3-8
     - Layer for ``Python 3.8.x`` runtimes
   * - aws-data-wrangler-layer-py3-9
     - arn:aws:serverlessrepo:us-east-1:336392948345:applications/aws-data-wrangler-layer-py3-9
     - Layer for ``Python 3.9.x`` runtimes     

Here is an example of how to create and use the AWS Data Wrangler Lambda layer in your CDK app:

.. code-block:: python
    
    from aws_cdk import core, aws_sam as sam, aws_lambda

    class DataWranglerApp(core.Construct):
      def __init__(self, scope: core.Construct, id_: str):
        super.__init__(scope,id)

        wrangler_layer = sam.CfnApplication(
          self,
          "wrangler-layer",
          location=CfnApplication.ApplicationLocationProperty(
            application_id="arn:aws:serverlessrepo:us-east-1:336392948345:applications/aws-data-wrangler-layer-py3-8",
            semantic_version="2.13.0",  # Get the latest version from https://github.com/awslabs/aws-data-wrangler/releases
          ),
        )

        wrangler_layer_arn = wrangler_layer.get_att("Outputs.WranglerLayer38Arn").to_string()
        wrangler_layer_version = aws_lambda.LayerVersion.from_layer_version_arn(self, "wrangler-layer-version", wrangler_layer_arn)

        aws_lambda.Function(
          self,
          "wrangler-function",
          runtime=aws_lambda.Runtime.PYTHON_3_8,
          function_name="sample-wrangler-lambda-function",
          code=aws_lambda.Code.asset("./src/wrangler-lambda"),
          handler='lambda_function.lambda_handler',
          layers=[wrangler_layer_version]
        )

AWS Glue Python Shell Jobs
--------------------------

1 - Go to `GitHub's release page <https://github.com/awslabs/aws-data-wrangler/releases>`_ and download the wheel file
(.whl) related to the desired version. Alternatively, you can download the wheel from the `public artifacts bucket <https://aws-data-wrangler.readthedocs.io/en/latest/install.html#public-artifacts>`_.

2 - Upload the wheel file to any Amazon S3 location.

3 - Go to your Glue Python Shell job and point to the wheel file on S3 in
the *Python library path* field.


`Official Glue Python Shell Reference <https://docs.aws.amazon.com/glue/latest/dg/add-job-python.html#create-python-extra-library>`_

AWS Glue PySpark Jobs
---------------------

.. note:: AWS Data Wrangler has compiled dependencies (C/C++) so there is only support for ``Glue PySpark Jobs >= 2.0``.

Go to your Glue PySpark job and create a new *Job parameters* key/value:

* Key: ``--additional-python-modules``
* Value: ``pyarrow==2,awswrangler``

To install a specific version, set the value for above Job parameter as follows:

* Value: ``cython==0.29.21,pg8000==1.21.0,pyarrow==2,pandas==1.3.0,awswrangler==2.13.0``

.. note:: Pyarrow 3 is not currently supported in Glue PySpark Jobs, which is why a previous installation of pyarrow 2 is required.

`Official Glue PySpark Reference <https://docs.aws.amazon.com/glue/latest/dg/reduced-start-times-spark-etl-jobs.html#reduced-start-times-new-features>`_

Public Artifacts
-----------------

Lambda zipped layers and Python wheels are stored in a publicly accessible S3 bucket for all versions.

* Bucket: ``aws-data-wrangler-public-artifacts``

* Prefix: ``releases/<version>/``

  * Lambda layer: ``awswrangler-layer-<version>-py<py-version>.zip``

  * Python wheel: ``awswrangler-<version>-py3-none-any.whl``

For example: ``s3://aws-data-wrangler-public-artifacts/releases/2.13.0/awswrangler-layer-2.13.0-py3.8.zip``

Amazon SageMaker Notebook
-------------------------

Run this command in any Python 3 notebook paragraph and then make sure to
**restart the kernel** before import the **awswrangler** package.

    >>> !pip install awswrangler

Amazon SageMaker Notebook Lifecycle
-----------------------------------

Open SageMaker console, go to the lifecycle section and
use the follow snippet to configure AWS Data Wrangler for all compatible
SageMaker kernels (`Reference <https://github.com/aws-samples/amazon-sagemaker-notebook-instance-lifecycle-config-samples/blob/master/scripts/install-pip-package-all-environments/on-start.sh>`_).

.. code-block:: sh

    #!/bin/bash

    set -e

    # OVERVIEW
    # This script installs a single pip package in all SageMaker conda environments, apart from the JupyterSystemEnv which
    # is a system environment reserved for Jupyter.
    # Note this may timeout if the package installations in all environments take longer than 5 mins, consider using
    # "nohup" to run this as a background process in that case.

    sudo -u ec2-user -i <<'EOF'

    # PARAMETERS
    PACKAGE=awswrangler

    # Note that "base" is special environment name, include it there as well.
    for env in base /home/ec2-user/anaconda3/envs/*; do
        source /home/ec2-user/anaconda3/bin/activate $(basename "$env")
        if [ $env = 'JupyterSystemEnv' ]; then
            continue
        fi
        nohup pip install --upgrade "$PACKAGE" &
        source /home/ec2-user/anaconda3/bin/deactivate
    done
    EOF

EMR Cluster
-----------

Even not being a distributed library,
AWS Data Wrangler could be a good helper to
complement Big Data pipelines.

- Configure Python 3 as the default interpreter for
  PySpark on your cluster configuration [ONLY REQUIRED FOR EMR < 6]

    .. code-block:: json

        [
          {
             "Classification": "spark-env",
             "Configurations": [
               {
                 "Classification": "export",
                 "Properties": {
                    "PYSPARK_PYTHON": "/usr/bin/python3"
                  }
               }
            ]
          }
        ]

- Keep the bootstrap script above on S3 and reference it on your cluster.

  - For EMR Release < 6

    .. code-block:: sh

        #!/usr/bin/env bash
        set -ex

        sudo pip-3.6 install pyarrow==2 awswrangler

  - For EMR Release >= 6

    .. code-block:: sh

        #!/usr/bin/env bash
        set -ex

        sudo pip install pyarrow==2 awswrangler

.. note:: Make sure to freeze the Wrangler version in the bootstrap for productive
          environments (e.g. awswrangler==2.13.0)

.. note:: Pyarrow 3 is not currently supported in the default EMR image, which is why a previous installation of pyarrow 2 is required.

From Source
-----------

    >>> git clone https://github.com/awslabs/aws-data-wrangler.git
    >>> cd aws-data-wrangler
    >>> pip install .


Notes for Microsoft SQL Server
------------------------------

``awswrangler`` is using the `pyodbc <https://github.com/mkleehammer/pyodbc>`_
for interacting with Microsoft SQL Server. For installing this package you need the ODBC header files,
which can be installed, for example, with the following commands:

    >>> sudo apt install unixodbc-dev
    >>> yum install unixODBC-devel

After installing these header files you can either just install ``pyodbc`` or
``awswrangler`` with the ``sqlserver`` extra, which will also install ``pyodbc``:

    >>> pip install pyodbc
    >>> pip install awswrangler[sqlserver]

Finally you also need the correct ODBC Driver for SQL Server. You can have a look at the
`documentation from Microsoft <https://docs.microsoft.com/sql/connect/odbc/
microsoft-odbc-driver-for-sql-server?view=sql-server-ver15>`_
to see how they can be installed in your environment.

If you want to connect to Microsoft SQL Server from AWS Lambda, you can build a separate Layer including the
needed OBDC drivers and `pyobdc`.

If you maintain your own environment, you need to take care of the above steps.
Because of this limitation usage in combination with Glue jobs is limited and you need to rely on the
provided `functionality inside Glue itself <https://docs.aws.amazon.com/glue/latest/dg/
aws-glue-programming-etl-connect.html#aws-glue-programming-etl-connect-jdbc>`_.
