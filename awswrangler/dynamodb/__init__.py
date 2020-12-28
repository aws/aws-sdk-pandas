"""Amazon DynamoDB Write Module."""

from awswrangler.dynamodb._write import put_csv, put_df, put_json

__all__ = ["put_df", "put_json", "put_csv"]
