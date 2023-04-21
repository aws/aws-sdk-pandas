# 5. Move dependencies to optional

Date: 2023-03-15

## Status

Accepted

## Context

AWS SDK for pandas relies on external dependencies in some of its modules. These include `redshift-connector`, `gremlinpython` and `pymysql` to cite a few.

In versions 2.x and below, most of these packages were set as required, meaning they were installed regardless of whether the user actually needed them. This has introduced two major risks and issues as the number of dependencies increased:
1. **Security risk**: Unused dependencies increase the attack surface to manage. Users must scan them and ensure that they are kept up to date even though they don't need them
2. **Dependency hell**: Users must resolve dependencies for packages that they are not using. It can lead to dependency hell and prevent critical updates related to security patches and major bugs

## Decision

A breaking change is introduced in version 3.x where the number of required dependencies is reduced to the most important ones, namely:
* boto3
* pandas
* numpy
* pyarrow
* typing-extensions

## Consequences

All other dependencies are moved to optional and must be installed by the user separately using pip install `awswrangler[dependency]`. For instance, the command to use the redshift APIs is `pip install awswrangler[redshift]`. Failing to do so raises an exception informing the user that the package is missing and how to install it
