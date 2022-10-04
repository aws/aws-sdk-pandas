# Contributing Guidelines

Thank you for your interest in contributing to our project. Whether it's a bug report, new feature, correction, or additional
documentation, we greatly value feedback and contributions from our community.

Please read through this document before submitting any issues or pull requests to ensure we have all the necessary
information to effectively respond to your bug report or contribution.

## Reporting Bugs/Feature Requests

We welcome you to use the GitHub issue tracker to report bugs or suggest features.

When filing an issue, please check [existing open](https://github.com/aws/aws-sdk-pandas/issues), or [recently closed](https://github.com/aws/aws-sdk-pandas/issues?utf8=%E2%9C%93&q=is%3Aissue%20is%3Aclosed%20), issues to make sure somebody else hasn't already
reported the issue. Please try to include as much information as you can. Details like these are incredibly useful:

* A reproducible test case or series of steps
* The version of our code being used
* Any modifications you've made relevant to the bug
* Anything unusual about your environment or deployment

Here is a list of tags to label issues and help us triage them:
* question: A question on the library. Consider starting a [discussion](https://github.com/aws/aws-sdk-pandas/discussions) instead
* bug: An error encountered when using the library
* feature: A completely new idea not currently covered by the library
* enhancement: A suggestion to enhance an existing feature

## Contributing via Pull Requests

Contributions via pull requests are much appreciated. Before sending us a pull request, please ensure that:

1. You are working against the latest source on the *main* branch.
2. You check existing open, and recently merged, pull requests to make sure someone else hasn't addressed the problem already.
3. You open an issue to discuss any significant work - we would hate for your time to be wasted.

To send us a pull request, please:

1. Fork the repository.
2. Modify the source; please focus on the specific change you are contributing. If you also reformat all the code, it will be hard for us to focus on your change.
3. Ensure local tests pass.
4. Commit to your fork using clear commit messages.
5. Send us a pull request, answering any default questions in the pull request interface.
6. Pay attention to any automated CI failures reported in the pull request, and stay involved in the conversation.

GitHub provides additional document on [forking a repository](https://help.github.com/articles/fork-a-repo/) and
[creating a pull request](https://help.github.com/articles/creating-a-pull-request/).

*Note: An automated Code Build is triggered with every pull request. To skip it, add the prefix `[skip-ci]` to your commit message.*  

## Finding contributions to work on

Looking at the existing issues is a great way to find something to contribute on. As our projects, by default, use the default GitHub issue labels (enhancement/bug/duplicate/help wanted/invalid/question/wontfix), looking at any ['help wanted'](https://github.com/aws/aws-sdk-pandas/labels/help%20wanted) issues is a great place to start.

## Code of Conduct

This project has adopted the [Amazon Open Source Code of Conduct](https://aws.github.io/code-of-conduct).
For more information see the [Code of Conduct FAQ](https://aws.github.io/code-of-conduct-faq) or contact
opensource-codeofconduct@amazon.com with any additional questions or comments.

## Security issue notifications

If you discover a potential security issue in this project we ask that you notify AWS/Amazon Security via our [vulnerability reporting page](http://aws.amazon.com/security/vulnerability-reporting/). Please do **not** create a public github issue.

## Licensing

See the [LICENSE](https://github.com/aws/aws-sdk-pandas/blob/main/LICENSE) file for our project's licensing. We will ask you to confirm the licensing of your contribution.

We may ask you to sign a [Contributor License Agreement (CLA)](http://en.wikipedia.org/wiki/Contributor_License_Agreement) for larger changes.

## Environments

We have hundreds of test functions that runs against several AWS Services. You don't need to test everything to open a Pull Request.
You can choose from three different environments to test your fixes/changes, based on what makes sense for your case.

* [Mocked test environment](#mocked-test-environment)
  * Based on [moto](https://github.com/spulec/moto).
  * Does not require real AWS resources
  * Fastest approach
  * Basically Limited only for Amazon S3 tests

* [Data Lake test environment](#data-lake-test-environment)
  * Requires some AWS services.
  * Amazon S3, Amazon Athena, AWS Glue Catalog, AWS KMS
  * Enable real tests on typical Data Lake cases

* [Full test environment](#full-test-environment)
  * Requires a bunch of real AWS services.
  * Amazon S3, Amazon Athena, AWS Glue Catalog, AWS KMS, Amazon Redshift, Aurora PostgreSQL, Aurora MySQL, Amazon Quicksight, etc
  * Enable real tests on all use cases.

## Step-by-step

### Mocked test environment

* Pick up a Linux or MacOS.
* Install Python 3.7, 3.8 or 3.9 with [poetry](https://github.com/python-poetry/poetry) for package management
* Fork the AWS SDK for pandas repository and clone that into your development environment

* Install dependencies:

``poetry install --extras "sqlserver oracle sparql"``

* Run the validation script:

``./validate.sh``

* To run a specific test function:

``pytest tests/unit/test_moto.py::test_get_bucket_region_succeed``

* To run all mocked test functions (Using 8 parallel processes):

``pytest -n 8 tests/unit/test_moto.py``

### Data Lake test environment

**DISCLAIMER**: Make sure you know what you are doing. These steps will charge some services on your AWS account and require a minimum security skill to keep your environment safe.

* Pick up a Linux or MacOS.
* Install Python 3.7, 3.8 or 3.9 with [poetry](https://github.com/python-poetry/poetry) for package management
* Fork the AWS SDK for pandas repository and clone that into your development environment

* Install dependencies:

``poetry install --extras "sqlserver oracle sparql"``

* Go to the ``test_infra`` directory

``cd test_infra``

* Install CDK dependencies:

``poetry install``

* [OPTIONAL] Set AWS_DEFAULT_REGION to define the region the Data Lake Test environment will deploy into. You may want to choose a region which you don't currently use:

``export AWS_DEFAULT_REGION=ap-northeast-1``

* Go to the ``scripts`` directory

``cd scripts``

* Deploy the `base` CDK stack

``./deploy-stack.sh base``

* Return to the project root directory

``cd ../../``

* Run the validation script:

``./validate.sh``

* To run a specific test function:

``pytest tests/unit/test_athena_parquet.py::test_parquet_catalog``

* To run all data lake test functions (Using 8 parallel processes):

``pytest -n 8 tests/unit/test_athena*``

* [OPTIONAL] To remove the base test environment cloud formation stack post testing:

``./test_infra/scripts/delete-stack.sh base``

### Full test environment

**DISCLAIMER**: Make sure you know what you are doing. These steps will charge some services on your AWS account and require a minimum security skill to keep your environment safe.

**DISCLAIMER**: This environment contains Aurora MySQL, Aurora PostgreSQL and Redshift (single-node) clusters which will incur cost while running.

* Pick up a Linux or MacOS.
* Install Python 3.7, 3.8 or 3.9 with [poetry](https://github.com/python-poetry/poetry) for package management
* Fork the AWS SDK for pandas repository and clone that into your development environment

* Then run the command bellow to install all dependencies:

``poetry install --extras "sqlserver oracle sparql"``

* Go to the ``test_infra`` directory

``cd test_infra``

* Install CDK dependencies:

``poetry install``

* [OPTIONAL] Set AWS_DEFAULT_REGION to define the region the Full Test environment will deploy into. You may want to choose a region which you don't currently use:

``export AWS_DEFAULT_REGION=ap-northeast-1``

* Go to the ``scripts`` directory

``cd scripts``

* Deploy the `base` and `databases` CDK stacks. This step could take about 15 minutes to deploy.

``./deploy-stack.sh base``
``./deploy-stack.sh databases``

* [OPTIONAL] Deploy the `lakeformation` CDK stack (if you need to test against the AWS Lake Formation Service). You must ensure Lake Formation is enabled in the account.

``./deploy-stack.sh lakeformation``

* [OPTIONAL] Deploy the `opensearch` CDK stack (if you need to test against the Amazon OpenSearch Service). This step could take about 15 minutes to deploy.

``./deploy-stack.sh opensearch``

* Go to the `EC2 -> SecurityGroups` console, open the `aws-sdk-pandas-*` security group and configure to accept your IP from any TCP port.
  - Alternatively run:
  
  ``./security-group-databases-add-local-ip.sh``
  
  - Check local IP was applied:
  
  ``./security-group-databases-check.sh``

``P.S Make sure that your security group will not be open to the World! Configure your security group to only give access for your IP.``

* Return to the project root directory

``cd ../../``

* [OPTIONAL] If you intend to run all test, you also need to make sure that you have Amazon QuickSight activated and your AWS user must be register on that.

* Run the validation script:

``./validate.sh``

* To run a specific test function:

``pytest tests/unit/test_mysql.py::test_read_sql_query_simple``

* To run all database test functions for MySQL (Using 8 parallel processes):

``pytest -n 8 tests/unit/test_mysql.py``

* To run all data lake test functions for all python versions (Only if Amazon QuickSight is activated and Amazon OpenSearch template is deployed):

``./test.sh``

* [OPTIONAL] To remove the base test environment cloud formation stack post testing:

``./test_infra/scripts/delete-stack.sh base``

``./test_infra/scripts/delete-stack.sh databases``

## Ray Load Tests Environment 
**DISCLAIMER**: Make sure you know what you are doing. These steps will charge some services on your AWS account and require a minimum security skill to keep your environment safe.

* Pick up a Linux or MacOS.
* Install Python 3.7, 3.8 or 3.9 with [poetry](https://github.com/python-poetry/poetry) for package management
* Fork the AWS SDK for pandas repository and clone that into your development environment

* Then run the command bellow to install all dependencies:

``poetry install``

* Go to the ``test_infra`` directory

``cd test_infra``

* Install CDK dependencies:

``poetry install``

* [OPTIONAL] Set AWS_DEFAULT_REGION to define the region the Ray Test environment will deploy into. You may want to choose a region which you don't currently use:

``export AWS_DEFAULT_REGION=ap-northeast-1``

* Go to the ``scripts`` directory

``cd scripts``

* Deploy the `ray` CDK stack.

``./deploy-stack.sh ray``

* Configure Ray Cluster 

``vi ray-cluster-config.yaml`` 

```
# Update the following file to match your enviroment
# The following is an example
cluster_name: ray-cluster

min_workers: 2
max_workers: 2

provider:
    type: aws
    region: us-east-1 # change region as required
    availability_zone: us-east-1a,us-east-1b,us-east-1c # change azs as required
    security_group:
        GroupName: ray_client_security_group
    cache_stopped_nodes: False

available_node_types:
  ray.head.default:
    node_config:
      InstanceType: r5n.2xlarge # change instance type as required
      IamInstanceProfile:
        Arn: arn:aws:iam::{UPDATE YOUR ACCOUNT ID HERE}:instance-profile/ray-cluster-instance-profile
      ImageId: ami-0ea510fcb67686b48 # latest ray images -> https://github.com/amzn/amazon-ray#amazon-ray-images
      SubnetId: {replace with subnet within above AZs}

  ray.worker.default:
      min_workers: 2
      max_workers: 2
      node_config:
        InstanceType: r5n.2xlarge
        IamInstanceProfile:
          Arn: arn:aws:iam::{UPDATE YOUR ACCOUNT ID HERE}:instance-profile/ray-cluster-instance-profile
        ImageId: ami-0ea510fcb67686b48 # latest ray images -> https://github.com/amzn/amazon-ray#amazon-ray-images
        SubnetId: {replace with subnet within above AZs}

setup_commands:
- pip install "awswrangler[modin, ray]==3.0.0b2"
- pip install pytest

```

* Create Ray Cluster 
``ray up -y ray-cluster-config.yaml``

* Push Load Tests to Ray Cluster
``ray rsync-up ray-cluster-config.yaml tests/load /home/ubuntu/``

* Submit Pytest Run to Ray Cluster
```
echo '''
import os

import pytest

args = "-v load/"

if not os.getenv("AWS_DEFAULT_REGION"):
    os.environ["AWS_DEFAULT_REGION"] = "us-east-1" # Set your region as necessary

result = pytest.main(args.split(" "))

print(f"result: {result}")
''' > handler.py
ray submit ray-cluster-config.yaml handler.py
```

* Teardown Cluster 
``ray down -y ray-cluster-config.yaml``

[More on launching Ray Clusters on AWS](https://docs.ray.io/en/master/cluster/vms/user-guides/launching-clusters/aws.html#)


## Recommended Visual Studio Code Recommended setting

```json
{
  "python.formatting.provider": "black",
  "python.linting.enabled": true,
  "python.linting.flake8Enabled": true,
  "python.linting.mypyEnabled": true,
  "python.linting.pylintEnabled": false
}
```

## Common Errors

Check the file below to check the common errors and solutions
[ERRORS](https://github.com/aws/aws-sdk-pandas/blob/main/CONTRIBUTING_COMMON_ERRORS.md)

## Bumping version
When there is a new release you can use `bump2version` for updating the version number in relevant files.
You can run `bump2version major|minor|patch` in the top directory and the following steps will be executed:
- The version number in all files which are listed in `.bumpversion.cfg` is updated
- A new commit with message `Bump version: {current_version} → {new_version}` is created
- A new Git tag `{new_version}` is created
