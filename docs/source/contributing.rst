.. _doc_contributing:

Contributing
============

For almost all features we need rely on AWS Services that didn't have mock tools in the community yet (AWS Glue, AWS Athena). So we are focusing on integration tests instead unit tests.

So, you will need provide a S3 bucket and a Glue/Athena database through environment variables.

    >>> export AWSWRANGLER_TEST_BUCKET=...

    >>> export AWSWRANGLER_TEST_DATABASE=...

CAUTION: This may this may incur costs in your AWS Account

    >>> make init

Make your changes...

    >>> make format

    >>> make lint

    >>> make test
