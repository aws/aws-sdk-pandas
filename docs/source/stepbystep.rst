.. _doc_stepbystep:

Step By Step
============

Setting Up Lambda Layer
-----------------------

.. figure:: _static/step-by-step/lambda-layer/download.png
    :align: center
    :alt: alternate text
    :figclass: align-center

    Go to [GitHub's release section](https://github.com/awslabs/aws-data-wrangler/releases) and download the layer bundle related to the desired version. Also select between Python 3.6 or 3.7.

.. figure:: _static/step-by-step/lambda-layer/upload.png
    :align: center
    :alt: alternate text
    :figclass: align-center

    Go to the AWS console and open the S3 panel. Upload the layer bundle to any S3 bucket in the desired AWS region.

.. figure:: _static/step-by-step/lambda-layer/url.png
    :align: center
    :alt: alternate text
    :figclass: align-center

    Copy the S3 object URL.

.. figure:: _static/step-by-step/lambda-layer/create.png
    :align: center
    :alt: alternate text
    :figclass: align-center

    Go to the AWS Lambda Panel, get in the layer's section (left side) and click to create one.

.. figure:: _static/step-by-step/lambda-layer/config.png
    :align: center
    :alt: alternate text
    :figclass: align-center

    Fill the fields (Use the pasted URL) and create your layer.

.. figure:: _static/step-by-step/lambda-layer/use.png
    :align: center
    :alt: alternate text
    :figclass: align-center

    Go to your AWS Lambda and use it!