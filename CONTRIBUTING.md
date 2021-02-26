# Contributing Guidelines

Thank you for your interest in contributing to our project. Whether it's a bug report, new feature, correction, or additional
documentation, we greatly value feedback and contributions from our community.

Please read through this document before submitting any issues or pull requests to ensure we have all the necessary
information to effectively respond to your bug report or contribution.

## Reporting Bugs/Feature Requests

We welcome you to use the GitHub issue tracker to report bugs or suggest features.

When filing an issue, please check [existing open](https://github.com/awslabs/aws-data-wrangler/issues), or [recently closed](https://github.com/awslabs/aws-data-wrangler/issues?utf8=%E2%9C%93&q=is%3Aissue%20is%3Aclosed%20), issues to make sure somebody else hasn't already
reported the issue. Please try to include as much information as you can. Details like these are incredibly useful:

* A reproducible test case or series of steps
* The version of our code being used
* Any modifications you've made relevant to the bug
* Anything unusual about your environment or deployment

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

## Finding contributions to work on

Looking at the existing issues is a great way to find something to contribute on. As our projects, by default, use the default GitHub issue labels (enhancement/bug/duplicate/help wanted/invalid/question/wontfix), looking at any ['help wanted'](https://github.com/awslabs/aws-data-wrangler/labels/help%20wanted) issues is a great place to start.

## Code of Conduct

This project has adopted the [Amazon Open Source Code of Conduct](https://aws.github.io/code-of-conduct).
For more information see the [Code of Conduct FAQ](https://aws.github.io/code-of-conduct-faq) or contact
opensource-codeofconduct@amazon.com with any additional questions or comments.

## Security issue notifications

If you discover a potential security issue in this project we ask that you notify AWS/Amazon Security via our [vulnerability reporting page](http://aws.amazon.com/security/vulnerability-reporting/). Please do **not** create a public github issue.

## Licensing

See the [LICENSE](https://github.com/awslabs/aws-data-wrangler/blob/main/LICENSE) file for our project's licensing. We will ask you to confirm the licensing of your contribution.

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
* Install Python 3.7, 3.8 or 3.9
* Fork the AWS Data Wrangler repository and clone that into your development environment
* Go to the project's directory create a Python's virtual environment for the project

`python3 -m venv .venv && source .venv/bin/activate`

or

`python -m venv .venv && source .venv/bin/activate`

* Install dependencies:

``pip install -r requirements-dev.txt``

* Run the validation script:

``./validate.sh``

* To run a specific test function:

``pytest tests/test_moto.py::test_get_bucket_region_succeed``

* To run all mocked test functions (Using 8 parallel processes):

``pytest -n 8 tests/test_moto.py``

### Data Lake test environment

**DISCLAIMER**: Make sure you know what you are doing. These steps will charge some services on your AWS account and require a minimum security skill to keep your environment safe.

* Pick up a Linux or MacOS.
* Install Python 3.7, 3.8 or 3.9
* Fork the AWS Data Wrangler repository and clone that into your development environment
* Go to the project's directory create a Python's virtual environment for the project

`python3 -m venv .venv && source .venv/bin/activate`

or

`python -m venv .venv && source .venv/bin/activate`

* Install dependencies:

``pip install -r requirements-dev.txt``

* [OPTIONAL] Set AWS_DEFAULT_REGION to define the region the Data Lake Test envrioment will deploy into. You may want to choose a region which you don't currently use:

``export AWS_DEFAULT_REGION=ap-northeast-1``

* Go to the ``cloudformation`` directory

``cd cloudformation``

* Deploy the Cloudformation template `base.yaml`

``./deploy-base.sh``

* Return to the project root directory

``cd ..``

* Run the validation script:

``./validate.sh``

* To run a specific test function:

``pytest tests/test_athena_parquet.py::test_parquet_catalog``

* To run all data lake test functions (Using 8 parallel processes):

``pytest -n 8 tests/test_athena*``

* [OPTIONAL] To remove the base test environment cloud formation stack post testing:

``./cloudformation/delete-base.sh``

### Full test environment

**DISCLAIMER**: Make sure you know what you are doing. These steps will charge some services on your AWS account and require a minimum security skill to keep your environment safe.

**DISCLAIMER**: This environment contains Aurora MySQL, Aurora PostgreSQL and Redshift (single-node) clusters which will incur cost while running.

* Pick up a Linux or MacOS.
* Install Python 3.7, 3.8 or 3.9
* Fork the AWS Data Wrangler repository and clone that into your development environment
* Go to the project's directory create a Python's virtual environment for the project

`python -m venv .venv && source .venv/bin/activate`

* Then run the command bellow to install all dependencies:

``pip install -r requirements-dev.txt``

* [OPTIONAL] Set AWS_DEFAULT_REGION to define the region the Full Test envrioment will deploy into. You may want to choose a region which you don't currently use:

``export AWS_DEFAULT_REGION=ap-northeast-1``

* Go to the ``cloudformation`` directory

``cd cloudformation``

* Deploy the Cloudformation templates `base.yaml` and `databases.yaml`. This step could take about 15 minutes to deploy.

``./deploy-base.sh``
``./deploy-databases.sh``

* Go to the `EC2 -> SecurityGroups` console, open the `aws-data-wrangler-*` security group and configure to accept your IP from any TCP port.
  - Alternatively run:
  
  ``./security-group-databases-add-local-ip.sh``
  
  - Check local IP was applied:
  
  ``./security-group-databases-check.sh``

``P.S Make sure that your security group will not be open to the World! Configure your security group to only give access for your IP.``

* Return to the project root directory

``cd ..``

* [OPTIONAL] If you intend to run all test, you also need to make sure that you have Amazon QuickSight activated and your AWS user must be register on that.

* Run the validation script:

``./validate.sh``

* To run a specific test function:

``pytest tests/test_db.py::test_sql``

* To run all database test functions (Using 8 parallel processes):

``pytest -n 8 tests/test_db.py``

* To run all data lake test functions for all python versions (Only if Amazon QuickSight is activated):

``./test.sh``

* [OPTIONAL] To remove the base test environment cloud formation stack post testing:

``./cloudformation/delete-base.sh``

``./cloudformation/delete-databases.sh``

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
[ERRORS](https://github.com/awslabs/aws-data-wrangler/blob/main/CONTRIBUTING_COMMON_ERRORS.md)

## Bumping version
When there is a new release you can use `bump2version` for updating the version number in relevant files.
You can run `bump2version major|minor|patch` in the top directory and the following steps will be executed:
- The version number in all files which are listed in `.bumpversion.cfg` is updated
- A new commit with message `Bump version: {current_version} → {new_version}` is created
- A new Git tag `{new_version}` is created
