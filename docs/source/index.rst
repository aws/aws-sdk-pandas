.. role::  raw-html(raw)
    :format: html

.. image:: _static/logo_transparent.png
    :width: 38%
    :alt: AWS Data Wrangler

*Pandas on AWS*
---------------------

.. note:: Collecting feedback for 1.0.0 (Official release on April 10th)

             We already have a full functional 1.0.0 version available for test!

             Any feedback about the docs or the library itself will be very welcome (Through GitHub Issue/PR).

             - Install: `pip install git+https://github.com/awslabs/aws-data-wrangler.git@dev-1.0.0`
             - Docs: https://aws-data-wrangler.readthedocs.io/en/dev-1.0.0/
             - Code: https://github.com/awslabs/aws-data-wrangler/tree/dev-1.0.0

.. warning:: Version 1.0.0 coming soon with several breaking changes. Please, pin the version you are using on your environment.

             AWS Data Wrangler is completing 1 year, and the team is working to collect feedbacks and features requests to put in our 1.0.0 version. By now we have 3 major changes listed:

             - API redesign
             - Nested data types support
             - Deprecation of PySpark support
                 - PySpark support takes considerable part of the development time and it has not been reflected in user adoption. Only 2 of our 70 issues on GitHub are related to Spark.
                 - In addition, the integration between PySpark and PyArrow/Pandas remains in experimental stage and we have been experiencing tough times to keep it stable.

.. toctree::
   :maxdepth: 4

   install
   tutorials
   examples
   divingdeep
   api/awswrangler
   contributing
   license
