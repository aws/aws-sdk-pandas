.. _doc_contributing:

Contributing
============

* AWS Data Wrangler practically only makes integrations. So we prefer to dedicate our energy / time writing integration tests instead of unit tests. We really like an end-to-end approach for all features.

* All integration tests are between a local Docker container and a remote/real AWS service.

* We have a Docker recipe to set up the local end (testing/Dockerfile).

* We have a Cloudformation to set up the AWS end (testing/template.yaml).

Step-by-step
------------

**DISCLAIMER**: Make sure to know what you are doing. This steps will charge some services on your AWS account. And requires a minimum security skills to keep your environment safe.

* Pick up a Linux or MacOS.

* Install Python 3.6+

* Install Docker and configure at least 4 cores and 8 GB of memory

* Fork the AWS Data Wrangler repository and clone that into your development environment

* Go to the project's directory create a Python's virtual environment for the project (**python -m venv venv && source source venv/bin/activate**)

* Run **./install-dev.sh**

* Go to the *testing* directory

* Configure the parameters.json file with your AWS environment infos (Make sure that your Redshift will not be open for the World!)

* Deploy the Cloudformation stack **./deploy-cloudformation.sh**

* Open the docker image **./open-image.sh**