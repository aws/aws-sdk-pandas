.. note:: Due the new major version 1.0.0 with breaking changes, please make sure that all your old projects has dependencies frozen on the desired version (e.g. `pip install awswrangler==0.3.2`). You can always check the legacy docs `here <https://aws-data-wrangler.readthedocs.io/en/legacy/>`_.

Install
=======

**AWS Data Wrangler** runs with Python ``3.6``, ``3.7`` and ``3.8``
and on several platforms (AWS Lambda, AWS Glue Python Shell, EMR, EC2,
on-premises, Amazon SageMaker, local, etc).

Some good practices for most of the methods bellow are:
  - Use new and individual Virtual Environments for each project (`venv <https://docs.python.org/3/library/venv.html>`_).
  - On Notebooks, always restart your kernel after installations.

PyPI (pip)
----------

    >>> pip install awswrangler

Conda
-----

    >>> conda install -c conda-forge awswrangler

AWS Lambda Layer
----------------

1 - Go to `GitHub's release section <https://github.com/awslabs/aws-data-wrangler/releases>`_
and download the layer zip related to the desired version.

2 - Go to the AWS Lambda Panel, open the layer section (left side)
and click **create layer**.

3 - Set name and python version, upload your fresh downloaded zip file
and press **create** to create the layer.

4 - Go to your Lambda and select your new layer!

AWS Glue Wheel
--------------

.. note:: AWS Data Wrangler has compiled dependencies (C/C++) so there is only support for ``Glue Python Shell``, **not** for ``Glue PySpark``.

1 - Go to `GitHub's release page <https://github.com/awslabs/aws-data-wrangler/releases>`_ and download the wheel file (.whl) related to the desired version.

2 - Upload the wheel file to any Amazon S3 location.

3 - Go to your Glue Python Shell job and point to the new file on S3.

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
  PySpark under your cluster configuration

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

    .. code-block:: sh

        #!/usr/bin/env bash
        set -ex

        sudo pip-3.6 install awswrangler

.. note:: Make sure to freeze the Wrangler version in the bootstrap for productive
          environments (e.g. awswrangler==1.0.0)

From Source
-----------

    >>> git clone https://github.com/awslabs/aws-data-wrangler.git
    >>> cd aws-data-wrangler
    >>> pip install .
