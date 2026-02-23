import logging

import pytest

import awswrangler as wr
from awswrangler.s3_tables._catalog import _build_catalog_properties, _extract_region_from_arn

logging.getLogger("awswrangler").setLevel(logging.DEBUG)


def test_module_accessible():
    assert hasattr(wr, "s3_tables")


@pytest.mark.parametrize(
    "func_name",
    [
        "create_table_bucket",
        "create_namespace",
        "create_table",
        "delete_table_bucket",
        "delete_namespace",
        "delete_table",
        "read_table",
        "write_table",
    ],
)
def test_public_functions_exist(func_name):
    assert hasattr(wr.s3_tables, func_name)
    assert callable(getattr(wr.s3_tables, func_name))


@pytest.mark.parametrize(
    "arn,expected_region",
    [
        ("arn:aws:s3tables:us-east-1:123456789012:bucket/my-bucket", "us-east-1"),
        ("arn:aws:s3tables:eu-west-1:999999999999:bucket/test-bucket", "eu-west-1"),
        ("arn:aws:s3tables:ap-southeast-2:111111111111:bucket/analytics", "ap-southeast-2"),
    ],
)
def test_extract_region_from_arn(arn, expected_region):
    assert _extract_region_from_arn(arn) == expected_region


@pytest.mark.parametrize("bad_arn", ["not-an-arn", "arn:aws:s3:::my-bucket"])
def test_extract_region_from_arn_invalid(bad_arn):
    with pytest.raises(Exception, match="Cannot extract region from ARN"):
        _extract_region_from_arn(bad_arn)


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


def test_build_catalog_properties_no_credentials_without_custom_session():
    arn = "arn:aws:s3tables:us-east-1:123456789012:bucket/my-bucket"
    props = _build_catalog_properties(arn)
    assert "s3tables.access-key-id" not in props
    assert "s3tables.secret-access-key" not in props
    assert "s3tables.session-token" not in props
