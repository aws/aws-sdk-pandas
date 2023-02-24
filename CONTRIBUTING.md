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

There are hundreds of tests that run against several AWS Services. You don't need to test everything to open a Pull Request.
You can choose from three environments to test your fixes/changes, based on what makes sense for your use case.

Start at [Step by step](#step-by-step), and then choose from one of these environments:

* [Mocked test environment](#mocked-test-environment)
  * Based on [moto](https://github.com/spulec/moto).
  * Does not require real AWS resources
  * Fastest approach
  * Limited to a few services (S3 tests)

* [Basic test environment](#basic-test-environment)
  * Requires some AWS services
  * Amazon S3, Amazon Athena, AWS Glue Catalog, AWS KMS
  * A cost is incurred

* [Full test environment](#full-test-environment)
  * Requires access to numerous AWS services
  * Amazon S3, Amazon Athena, AWS Glue Catalog, AWS KMS, Amazon Redshift, Aurora PostgreSQL, Aurora MySQL, Amazon Quicksight, etc
  * Full test coverage
  * A cost is incurred

## Step by step

These instructions are for Linux and Mac machines, some steps might not work for Windows.

Fork the AWS SDK for pandas repository and clone it into your development environment.

[poetry](https://python-poetry.org/) is the Python dependency management system used for development. To install it use:

``curl -sSL https://install.python-poetry.org | python3 -``

You can then install required and dev dependencies with:

``poetry install``

If you are testing an optional dependency (e.g. `sparql`), you can add it with:

``poetry install --extras "sparql" -vvv``

To install all extra dependencies (only recommended for advanced usage):

``poetry install --all-extras``

Poetry creates a virtual environment for you. To activate it, use:

``source "$(poetry env info --path )/bin/activate"`` 

A `validate.sh` script is used for linting and typing (black, mypy...):

``./validate.sh``

### Mocked test environment

Some unit tests can be mocked locally, i.e. no AWS account is required:

To run a specific test:

``pytest tests/test_moto.py::test_get_bucket_region_succeed``

To run all mocked tests (Using 8 parallel processes):

``pytest -n 8 tests/test_moto.py``

### Basic test environment

**DISCLAIMER**: You will incur a cost for some of the services used in your AWS account. A basic understanding of AWS security principles is highly recommended.

*OPTIONAL*: Set the `AWS_DEFAULT_REGION` environment variable to define the AWS region where the infrastructure is deployed:

``export AWS_DEFAULT_REGION=ap-northeast-1``

Infrastructure is deployed with the AWS CDK. Follow this [guide](https://docs.aws.amazon.com/cdk/v2/guide/getting_started.html) to install it if it's missing.

Navigate to the ``test_infra`` directory and install CDK dependencies

```
cd test_infra
poetry install
```

Then deploy the `base` CDK stack (i.e. minimum required infrastructure)

``./scripts/deploy-stack.sh base``

Return to the project root directory

``cd ../``

To run a specific test:

``pytest tests/test_athena_parquet.py::test_parquet_catalog``

To run all athena tests (Using 8 parallel processes):

``pytest -n 8 tests/test_athena*``

*OPTIONAL*: To remove the base test environment CloudFormation stack, use:

``./test_infra/scripts/delete-stack.sh base``

### Full test environment

**DISCLAIMER**: You will incur a cost for some of the services used in your AWS account. A basic understanding of AWS security principles is highly recommended.

**DISCLAIMER**: This environment provisions Aurora MySQL, Aurora PostgreSQL, Redshift (single-node) clusters which may incur a significant cost while running.

*OPTIONAL*: Set the `AWS_DEFAULT_REGION` environment variable to define the AWS region where the infrastructure is deployed:

``export AWS_DEFAULT_REGION=ap-northeast-1``

Infrastructure is deployed with the AWS CDK. Follow this [guide](https://docs.aws.amazon.com/cdk/v2/guide/getting_started.html) to install it if it's missing.

Navigate to the ``test_infra`` directory and install CDK dependencies

```
cd test_infra
poetry install
```

Deploy the `base` and `databases` CDK stacks. This step could take 15 minutes to complete.

```
./scripts/deploy-stack.sh base
./scripts/deploy-stack.sh databases
```

*OPTIONAL*: Deploy the `lakeformation` CDK stack (if you need to test against the AWS Lake Formation Service). You must ensure Lake Formation is enabled in the account.

``./scripts/deploy-stack.sh lakeformation``

*OPTIONAL*: Deploy the `opensearch` CDK stack (if you need to test against the Amazon OpenSearch Service). This step could take 15 minutes to complete.

``./scripts/deploy-stack.sh opensearch``

Go to the `EC2 -> SecurityGroups` console, open the `aws-sdk-pandas-*` security group and configure it to accept your IP from any TCP port.
  - Alternatively run:
  
  ``./scripts/security-group-databases-add-local-ip.sh``
  
  - Check local IP was applied:
  
  ``./scripts/security-group-databases-check.sh``

**P.S Make sure that your security group will not be open to the World! Configure your security group to only give access to your IP.**

Return to the project root directory

``cd ../``

*OPTIONAL*: If you intend to run all tests, you must also ensure that Amazon QuickSight is activated and your AWS user/role is registered.

To run a specific test:

``pytest tests/test_mysql.py::test_read_sql_query_simple``

To run all database MySQL tests (Using 8 parallel processes):

``pytest -n 8 tests/test_mysql.py``

To run all tests for all python versions (assuming Amazon QuickSight/Lake Formation are activated and the optional stacks deployed):

``./test.sh``

*OPTIONAL*: To destroy stacks use:

``./test_infra/scripts/delete-stack.sh <name>``

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

## Bumping the version
When there is a new release you can use `bump2version` for updating the version number in relevant files.
You can run `bump2version major|minor|patch` in the top directory and the following steps will be executed:
- The version number in all files which are listed in `.bumpversion.cfg` is updated
- A new commit with message `Bump version: {current_version} → {new_version}` is created
- A new Git tag `{new_version}` is created
