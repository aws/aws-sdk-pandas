# 6. Deprecate wr.s3.merge_upsert_table

Date: 2023-03-15

## Status

Accepted

## Context

AWS SDK for pandas `wr.s3.merge_upsert_table` is used to perform upsert (update else insert) onto an existing AWS Glue 
Data Catalog table. It is a much simplified version of upsert functionality that is supported natively by Apache Hudi 
and Athena Iceberg tables, and does not, for example, handle partitioned datasets.

## Decision

To avoid poor user experience `wr.s3.merge_upsert_table` is deprecated and will be removed in 3.0 release.

## Consequences

In [PR#2076](https://github.com/aws/aws-sdk-pandas/pull/2076), `wr.s3.merge_upsert_table` function was removed.
