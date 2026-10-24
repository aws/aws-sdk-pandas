import logging
from unittest.mock import Mock, patch

import boto3
import botocore.exceptions
import pandas as pd
import pyarrow as pa
import pytest

import awswrangler as wr
from awswrangler.s3._s3_tables_catalog import _build_catalog_properties, _parse_table_bucket_arn

logging.getLogger("awswrangler").setLevel(logging.DEBUG)


@pytest.mark.parametrize(
    "arn,expected",
    [
        ("arn:aws:s3tables:us-east-1:123456789012:bucket/my-bucket", ("us-east-1", "123456789012", "my-bucket")),
        ("arn:aws:s3tables:eu-west-1:999999999999:bucket/test-bucket", ("eu-west-1", "999999999999", "test-bucket")),
        (
            "arn:aws:s3tables:ap-southeast-2:111111111111:bucket/analytics",
            ("ap-southeast-2", "111111111111", "analytics"),
        ),
    ],
)
def test_parse_table_bucket_arn(arn, expected):
    assert _parse_table_bucket_arn(arn) == expected


@pytest.mark.parametrize("bad_arn", ["not-an-arn", "arn:aws:s3:::my-bucket"])
def test_parse_table_bucket_arn_invalid(bad_arn):
    with pytest.raises(Exception, match="Cannot parse ARN"):
        _parse_table_bucket_arn(bad_arn)


def test_build_catalog_properties():
    arn = "arn:aws:s3tables:us-west-2:123456789012:bucket/my-bucket"
    props = _build_catalog_properties(arn)

    assert props["type"] == "rest"
    assert props["warehouse"] == arn
    assert props["uri"] == "https://s3tables.us-west-2.amazonaws.com/iceberg"
    assert props["rest.sigv4-enabled"] == "true"
    assert props["rest.signing-name"] == "s3tables"
    assert props["rest.signing-region"] == "us-west-2"


def test_build_catalog_properties_region_from_arn():
    arn = "arn:aws:s3tables:eu-central-1:123456789012:bucket/test"
    props = _build_catalog_properties(arn)
    assert props["uri"] == "https://s3tables.eu-central-1.amazonaws.com/iceberg"
    assert props["rest.signing-region"] == "eu-central-1"


def test_build_catalog_properties_no_credentials():
    arn = "arn:aws:s3tables:us-east-1:123456789012:bucket/my-bucket"
    props = _build_catalog_properties(arn)
    assert not any(k for k in props if "access-key" in k or "secret" in k or "session-token" in k)


@pytest.mark.parametrize("token", ["session-token", None])
def test_build_catalog_properties_session_credentials(token):
    arn = "arn:aws:s3tables:us-east-1:123456789012:bucket/my-bucket"
    session = boto3.Session(
        aws_access_key_id="AKIACALLERSESSION000",
        aws_secret_access_key="caller-secret",
        aws_session_token=token,
        region_name="eu-west-1",
    )
    props = _build_catalog_properties(arn, boto3_session=session)

    assert props["client.access-key-id"] == "AKIACALLERSESSION000"
    assert props["client.secret-access-key"] == "caller-secret"
    assert props.get("client.session-token") == token
    # The signing region still comes from the table bucket ARN, not from the session.
    assert props["rest.signing-region"] == "us-east-1"
    assert "client.region" not in props


def test_build_catalog_properties_session_without_credentials():
    arn = "arn:aws:s3tables:us-east-1:123456789012:bucket/my-bucket"
    session = Mock(spec=boto3.Session)
    session.get_credentials.return_value = None
    # Falling back to the default credential chain would silently use another identity.
    with pytest.raises(botocore.exceptions.NoCredentialsError):
        _build_catalog_properties(arn, boto3_session=session)


@pytest.mark.parametrize("func_name", ["from_iceberg", "to_iceberg"])
@pytest.mark.parametrize("use_session", [True, False])
def test_iceberg_catalog_uses_boto3_session(func_name, use_session):
    arn = "arn:aws:s3tables:us-east-1:123456789012:bucket/my-bucket"
    session = (
        boto3.Session(
            aws_access_key_id="AKIACALLERSESSION000",
            aws_secret_access_key="caller-secret",
            region_name="us-east-1",
        )
        if use_session
        else None
    )
    with patch("pyiceberg.catalog.rest.RestCatalog") as rest_catalog:
        table = rest_catalog.return_value.load_table.return_value
        table.scan.return_value.to_arrow.return_value = pa.table({"id": [1]})
        if func_name == "from_iceberg":
            wr.s3.from_iceberg(table_bucket_arn=arn, namespace="ns", table_name="t", boto3_session=session)
        else:
            wr.s3.to_iceberg(
                df=pd.DataFrame({"id": [1]}),
                table_bucket_arn=arn,
                namespace="ns",
                table_name="t",
                boto3_session=session,
            )

    catalog_properties = rest_catalog.call_args.kwargs
    if use_session:
        assert catalog_properties["client.access-key-id"] == "AKIACALLERSESSION000"
        assert catalog_properties["client.secret-access-key"] == "caller-secret"
    else:
        assert not any(k for k in catalog_properties if "access-key" in k or "secret" in k)


def test_build_catalog_properties_glue_endpoint():
    arn = "arn:aws:s3tables:us-east-1:123456789012:bucket/my-bucket"
    import awswrangler as wr

    wr.config.s3tables_catalog_endpoint_url = "https://glue.us-east-1.amazonaws.com/iceberg"
    try:
        props = _build_catalog_properties(arn)
        assert props["uri"] == "https://glue.us-east-1.amazonaws.com/iceberg"
        assert props["rest.signing-name"] == "glue"
        assert props["warehouse"] == "123456789012:s3tablescatalog/my-bucket"
    finally:
        wr.config.s3tables_catalog_endpoint_url = None


def test_write_and_read(s3_table_namespace):
    bucket_arn, namespace = s3_table_namespace
    df = pd.DataFrame({"col_int": [1, 2, 3], "col_str": ["a", "b", "c"]})

    wr.s3.to_iceberg(
        df=df,
        table_bucket_arn=bucket_arn,
        namespace=namespace,
        table_name="test_rw",
    )

    df_out = wr.s3.from_iceberg(
        table_bucket_arn=bucket_arn,
        namespace=namespace,
        table_name="test_rw",
    )
    assert df_out.shape == (3, 2)
    assert set(df_out.columns) == {"col_int", "col_str"}


def test_write_append(s3_table_namespace):
    bucket_arn, namespace = s3_table_namespace
    df1 = pd.DataFrame({"id": [1, 2], "val": [10.0, 20.0]})
    df2 = pd.DataFrame({"id": [3], "val": [30.0]})

    wr.s3.to_iceberg(df=df1, table_bucket_arn=bucket_arn, namespace=namespace, table_name="test_append")
    wr.s3.to_iceberg(df=df2, table_bucket_arn=bucket_arn, namespace=namespace, table_name="test_append", mode="append")

    df_out = wr.s3.from_iceberg(table_bucket_arn=bucket_arn, namespace=namespace, table_name="test_append")
    assert len(df_out) == 3


def test_write_overwrite(s3_table_namespace):
    bucket_arn, namespace = s3_table_namespace
    df1 = pd.DataFrame({"id": [1, 2, 3]})
    df2 = pd.DataFrame({"id": [99]})

    wr.s3.to_iceberg(df=df1, table_bucket_arn=bucket_arn, namespace=namespace, table_name="test_overwrite")
    wr.s3.to_iceberg(
        df=df2, table_bucket_arn=bucket_arn, namespace=namespace, table_name="test_overwrite", mode="overwrite"
    )

    df_out = wr.s3.from_iceberg(table_bucket_arn=bucket_arn, namespace=namespace, table_name="test_overwrite")
    assert len(df_out) == 1
    assert df_out["id"].tolist() == [99]


def test_read_column_selection(s3_table_namespace):
    bucket_arn, namespace = s3_table_namespace
    df = pd.DataFrame({"a": [1], "b": [2], "c": [3]})

    wr.s3.to_iceberg(df=df, table_bucket_arn=bucket_arn, namespace=namespace, table_name="test_cols")

    df_out = wr.s3.from_iceberg(
        table_bucket_arn=bucket_arn, namespace=namespace, table_name="test_cols", columns=["a", "c"]
    )
    assert set(df_out.columns) == {"a", "c"}


def test_read_row_filter(s3_table_namespace):
    bucket_arn, namespace = s3_table_namespace
    df = pd.DataFrame({"x": [1, 2, 3, 4, 5]})

    wr.s3.to_iceberg(df=df, table_bucket_arn=bucket_arn, namespace=namespace, table_name="test_filter")

    df_out = wr.s3.from_iceberg(
        table_bucket_arn=bucket_arn, namespace=namespace, table_name="test_filter", row_filter="x > 3"
    )
    assert len(df_out) == 2


def test_read_limit(s3_table_namespace):
    bucket_arn, namespace = s3_table_namespace
    df = pd.DataFrame({"v": list(range(10))})

    wr.s3.to_iceberg(df=df, table_bucket_arn=bucket_arn, namespace=namespace, table_name="test_limit")

    df_out = wr.s3.from_iceberg(table_bucket_arn=bucket_arn, namespace=namespace, table_name="test_limit", limit=3)
    assert len(df_out) == 3
