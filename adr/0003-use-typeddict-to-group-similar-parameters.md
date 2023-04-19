# 3. Use TypedDict to group similar parameters

Date: 2023-03-10

## Status

Accepted

## Context

*AWS SDK for pandas* API methods contain many parameters which are related to a specific behaviour or setting. For example, methods which have an option to update the Glue AWScatalog, such as `to_csv` and `to_parquet`, contain a list of parameters that define the settings for the table in AWS Glue. These settings include the table description, column comments, the table type, etc.

As a consequence, some of our functions have grown to include dozens of parameters. When reading the function signatures, it can be unclear which parameters are related to which functionality. For example, it's not immediately obvious that the parameter `column_comments` in `s3.to_parquet` only writes the column comments into the AWS Glue catalog, and not to S3.

## Decision

Parameters that are related to similar functionality will be replaced by a single parameter of type [TypedDict](https://peps.python.org/pep-0589/). This will allow us to reduce the amount of parameters for our API functions, and also make it clearer that certain parameters are only related to specific functionalities.

For example, parameters related to Athena cache settings will be extracted into a parameter of type `AthenaCacheSettings`, parameters related to Ray settings will be extracted into `RayReadParquetSettings`, etc.

The usage of `TypedDict` allows the user to define the parameters as regular dictionaries with string keys, while empowering type checkers such as `mypy`. Alternately, implementations such as `AthenaCacheSettings` can be instantiated as classes.

### Alternatives

The main alternative that was considered was the idea of using `dataclass` instead of `TypedDict`. The advantage of this alternative would be that default values for parameters could be defined directly in the class signature, rather than needing to be defined in the function which uses the parameter.

On the other hand, the main issue with using `dataclass` is that it would require the customer figure out which class needs to be imported. With `TypedDict`, this is just one of the options; the parameters can simply be passed as a typical Python dictionary.

This alternative was discussed in more detail as part of [PR#1855](https://github.com/aws/aws-sdk-pandas/pull/1855#issuecomment-1353618099).

## Consequences

Subclasses of `TypedDict` such as `GlueCatalogParameters`, `AthenaCacheSettings`, `AthenaUNLOADSettings`, `AthenaCTASSettings` and `RaySettings` have been created. They are defined in the `wrangler.typing` module.

These parameters grouping can used in either of the following two ways:
```python
wr.athena.read_sql_query(
    "SELECT * FROM ...",,
    ctas_approach=True,
    athena_cache_settings={"max_cache_seconds": 900},
)

wr.athena.read_sql_query(
    "SELECT * FROM ...",,
    ctas_approach=True,
    athena_cache_settings=wr.typing.AthenaCacheSettings(
        max_cache_seconds=900,
    ),
)
```

Many of our functions signatures have been changes to take advantage of this refactor. Many of these are breaking changes which will be released as part of the next major version: `3.0.0`.
