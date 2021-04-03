"""Amazon Merge Upsert Module which can upsert existing table (PRIVATE)."""

import logging
from typing import List, Optional

import boto3
import pandas

import awswrangler as wr
from awswrangler import _data_types
from awswrangler.exceptions import FailedQualityCheck

_logger: logging.Logger = logging.getLogger(__name__)


def _update_existing_table(
    existing_df: pandas.DataFrame,
    delta_df: pandas.DataFrame,
    primary_key: List[str],
    database: str,
    table: str,
    boto3_session: Optional[boto3.Session],
) -> None:
    """Perform Update else Insert onto an existing Glue table."""
    # Set Index on the pandas dataframe so that join/concat can be made
    existing_df = existing_df.set_index(keys=primary_key, drop=False, verify_integrity=True)
    delta_df = delta_df.set_index(keys=primary_key, drop=False, verify_integrity=True)
    # Merge-Upsert the data for both of the dataframe
    merged_df = pandas.concat([existing_df[~existing_df.index.isin(delta_df.index)], delta_df])
    # Remove the index and drop the index columns
    merged_df = merged_df.reset_index(drop=True)
    # Get existing tables location
    path = wr.catalog.get_table_location(database=database, table=table, boto3_session=boto3_session)
    # Write to Glue catalog
    response = wr.s3.to_parquet(
        df=merged_df,
        path=path,
        dataset=True,
        database=database,
        table=table,
        mode="overwrite",
        boto3_session=boto3_session,
    )
    _logger.info("Successfully upserted %s.%s and got response as %s", database, table, str(response))


def _is_data_quality_sufficient(
    existing_df: pandas.DataFrame, delta_df: pandas.DataFrame, primary_key: List[str]
) -> bool:
    """Check data quality of existing table and the new delta feed."""
    error_messages = list()
    existing_schema = _data_types.pyarrow_types_from_pandas(df=existing_df, index=False)
    delta_schema = _data_types.pyarrow_types_from_pandas(df=delta_df, index=False)
    # Check for duplicates on the primary key in the existing table
    if sum(pandas.DataFrame(existing_df, columns=primary_key).duplicated()) != 0:
        error_messages.append("Data inside the existing table has duplicates.")
    # Compare column name and column data types
    if existing_schema != delta_schema:
        error_messages.append(
            f"Column name or data types mismtach!"
            f"\n Columns in uploaded file are {delta_schema}"
            f"\n Columns in existing table are {existing_schema}"
        )
    # Check for duplicates in the delta dataframe
    if sum(pandas.DataFrame(delta_df, columns=primary_key).duplicated()) != 0:
        error_messages.append("Data inside the delta dataframe has duplicates.")
    # Return True only if no errors are encountered
    if len(error_messages) > 0:
        _logger.info("error_messages %s", error_messages)
        raise FailedQualityCheck("Data quality is insufficient to allow a merge. Please check errors above.")
    return True


def merge_upsert_table(
    delta_df: pandas.DataFrame,
    database: str,
    table: str,
    primary_key: List[str],
    boto3_session: Optional[boto3.Session] = None,
) -> None:
    """Perform Upsert (Update else Insert) onto an existing Glue table.

    Parameters
    ----------
    delta_df : pandas.DataFrame
        The delta dataframe has all the data which needs to be merged on the primary key
    database: Str
        An existing database name
    table: Str
        An existing table name
    primary_key: List[str]
        Pass the primary key as a List of string columns
        List['column_a', 'column_b']
    boto3_session : boto3.Session(), optional
        Boto3 Session. The default boto3 session will be used if boto3_session receive None.

    Returns
    -------
    None

    Examples
    --------
    Reading all Parquet files under a prefix
    >>> import awswrangler as wr
    >>> import pandas as pd
    >>> delta_df = pd.DataFrame({"id": [1], "cchar": ["foo"], "date": [datetime.date(2021, 1, 2)]})
    >>> primary_key = ["id", "cchar"]
    >>> wr.s3.merge_upsert_table(delta_df=delta_df, database='database', table='table', primary_key=primary_key)
    """
    # Check if table exists first
    if wr.catalog.does_table_exist(database=database, table=table, boto3_session=boto3_session):
        # Read the existing table into a pandas dataframe
        existing_df = wr.s3.read_parquet_table(database=database, table=table, boto3_session=boto3_session)
        # Check if data quality inside dataframes to be merged are sufficient
        if _is_data_quality_sufficient(existing_df=existing_df, delta_df=delta_df, primary_key=primary_key):
            # If data quality is sufficient then merge upsert the table
            _update_existing_table(
                existing_df=existing_df,
                delta_df=delta_df,
                primary_key=primary_key,
                database=database,
                table=table,
                boto3_session=boto3_session,
            )
    else:
        exception_message = f"database= {database} and table= {table}  does not exist"
        _logger.exception(exception_message)
        raise AttributeError(exception_message)
