"""Amazon DynamoDB Write Module."""

from awswrangler.dynamodb._delete import delete_items
from awswrangler.dynamodb._utils import get_table
from awswrangler.dynamodb._write import put_csv, put_df, put_items, put_json

__all__ = ["delete_items", "get_table", "put_csv", "put_df", "put_items", "put_json"]
