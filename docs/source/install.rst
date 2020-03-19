Install
=======

*Runs* with Python ``3.6``, ``3.7`` and ``3.8``.

*Runs* on several platforms (AWS Lambda, AWS Glue Python Shell, EMR, EC2,
on-premises, Amazon SageMaker, local, etc).

PyPI (pip)
----------

    >>> pip install awswrangler

Conda
-----

    >>> conda install -c conda-forge awswrangler

AWS Lambda Layer
----------------

1 - Go to `GitHub's release section <https://github.com/awslabs/aws-data-wrangler/releases>`_ and download the layer zip related to the desired version.

2 - Go to the AWS Lambda Panel, get in the layer's section (left side) and click to create one.

3 - Fill the fields, upload your fresh downloaded zip file and create your layer.

4 - Go to your Lambda and select your new layer!

AWS Glue Wheel
--------------

.. note:: AWS Data Wrangler counts with compiled dependencies (C/C++) so there is only support for ``Glue Python Shell``, **not** for ``Glue PySpark``.

1 - Go to `GitHub's release page <https://github.com/awslabs/aws-data-wrangler/releases>`_ and download the wheel file (.whl) related to the desired version.

2 - Upload the wheel file to any Amazon S3 location.

3 - Got to your Glue Python Shell job and point to the new file on s3.

Amazon SageMaker Notebook Lifecycle
-----------------------------------

Use the follow snippit to configure AWS Data Wrangler for all compatible SageMaker kernels.
