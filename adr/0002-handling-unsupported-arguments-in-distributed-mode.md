# 2. Handling unsupported arguments in distributed mode

Date: 2023-03-09

## Status

Accepted

## Context

Many of the API functions allow the user to pass their own `boto3` session, which will then be used by all the underlying `boto3` calls. With distributed computing, one of the limitations we have is that we cannot pass the `boto3` session to the worker nodes.

Boto3 session are not thread-safe, and therefore cannot be passed to Ray workers. The credentials behind a `boto3` session cannot be sent to Ray workers either, since sending credentials over the network is considered a security risk.

This raises the question of what to do when, in distributed mode, the customer passes arguments that are normally supported, but aren’t supported in distributed mode.

## Decision

When a user passes arguments that are unsupported by distributed mode, the function should fail immediately.

The main alternative to this approach would be if a parameter such as a `boto3` session is passed, we should use it where possible. This could result in a situation where, when reading Parquet files from S3, the process of listing the files uses the `boto3` session whereas the reading of the Parquet files doesn’t. This could result in inconsistent behavior, as part of the function uses the extra parameters while the other part of it doesn’t.

Another alternative would simply be to ignore the unsupported parameters, while potentially outputting a warning. The main issue with this approach is that if a customer tells our API functions to use certain parameters, they expect those parameters to be used. By ignoring them, the the AWS SDK for pandas API would be doing something different from what the customer asked, without properly notifying them, and would thus lose the customer’s trust.

## Consequences

In [PR#2501](https://github.com/aws/aws-sdk-pandas/pull/2051), the `validate_distributed_kwargs` annotation was introduced which can check for the presence of arguments that are unsupported in the distributed mode.

The annotation has also been applied for arguments such as `s3_additional_kwargs` and `version_id` when reading/writing data on S3.

