# 8. Switching between PyArrow and Pandas based datasources for CSV/JSON I/O

Date: 2023-03-16

## Status

Accepted

## Context

The reading and writing operations for CSV/JSON data in *AWS SDK for pandas* make use of the underlying functions in Pandas. For example, `wr.s3.read_csv` will open a stream of data from S3 and then invoke `pandas.read_csv`. This allows the library to fully support all the arguments which are supported by the underlying Pandas functions. Functions such as `wr.s3.read_csv` or `wr.s3.to_json` accept a `**kwargs` parameter which forwards all parameters to `pandas.read_csv` and `pandas.to_json` automatically.

From version 3.0.0 onward, *AWS SDK for pandas* supports Ray and Modin. When those two libraries are installed, all aforementioned I/O functions will be distributed on a Ray cluster. In the background, this means that all the I/O functions for S3 are running as part of a [custom Ray data source](https://docs.ray.io/en/latest/_modules/ray/data/datasource/datasource.html). Data is then returned in blocks, which form the Modin DataFrame.

The issue is that the Pandas I/O functions work very slowly in the Ray datasource compared with the equivalent I/O functions in PyArrow. Therefore, calling `pyarrow.csv.read_csv` is significantly faster than calling `pandas.read_csv` in the background.

However, the PyArrow I/O functions do not support the same set of parameters as the ones in Pandas. As a consequence, whereas the PyArrow functions offer greater performance, they come at the cost of feature parity between the non-distributed mode and the distributed mode.

For reference, loading 5 GiB of CSV data with the PyArrow functions took around 30 seconds, compared to 120 seconds with the Pandas functions in the same scenario.
For writing back to S3, the speed-up is around 2x.

## Decision

In order to maximize both performance without losing feature parity, we implemented logic whereby if the user passes a set of parameters which are supported by PyArrow, the library uses PyArrow for reading/writing. If not, the library defaults to the slower Pandas functions, which will support the set of parameter.

The following example will illustrate the difference:

```python
# This will be loaded by PyArrow, as `doublequote` is supported
wr.s3.read_csv(
    path="s3://my-bucket/my-path/",
    dataset=True,
    doublequote=False,
)

# This will be loaded using the Pandas I/O functions, as `comment` is not supported by PyArrow
wr.s3.read_csv(
    path="s3://my-bucket/my-path/",
    dataset=True,
    comment="#",
)
```

This logic is applied to the following functions:
1. `wr.s3.read_csv`
2. `wr.s3.read_json`
3. `wr.s3.to_json`
4. `wr.s3.to_csv`

## Consequences

The logic of switching between using PyArrow or Pandas functions in background was implemented as part of [#1699](https://github.com/aws/aws-sdk-pandas/pull/1699). It was later expanded to support more parameters in [#2008](https://github.com/aws/aws-sdk-pandas/pull/2008) and [#2019](https://github.com/aws/aws-sdk-pandas/pull/2019).
