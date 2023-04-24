"""Amazon Textract module."""

import logging
from typing import List, Optional

import boto3
import pandas as pd

from awswrangler import _utils

_logger: logging.Logger = logging.getLogger(__name__)


def analyze_document_table(
    s3_bucket_name: str, s3_key: str, boto3_session: Optional[boto3.Session] = None
) -> List[pd.DataFrame]:
    """Send message on an existing Chime Chat rooms.

    Parameters
    ----------
    :param s3_bucket_name : Bucket
        Bucket Name whithout the S3://
    :param s3_key : message
        S3 Path

    Returns
    -------
    List
        Return a list of pandas data frames, one df for every table found
        in the response
    """
    textract = _utils.client(service_name="textract", session=boto3_session)
    textract = boto3.client("textract")

    response = textract.analyze_document(
        Document={"S3Object": {"Bucket": s3_bucket_name, "Name": s3_key}}, FeatureTypes=["TABLES"]
    )

    blocks = response["Blocks"]
    tables = _map_blocks(blocks, "TABLE")
    cells = _map_blocks(blocks, "CELL")
    words = _map_blocks(blocks, "WORD")
    selections = _map_blocks(blocks, "SELECTION_ELEMENT")

    dataframes = []
    for table in tables.values():
        # Determine all the cells that belong to this table
        table_cells = [cells[cell_id] for cell_id in _get_children_ids(table)]
        # Determine the table's number of rows and columns
        n_rows = max(cell["RowIndex"] for cell in table_cells)
        n_cols = max(cell["ColumnIndex"] for cell in table_cells)
        content = [[None for _ in range(n_cols)] for _ in range(n_rows)]
        # Fill in each cell
        for cell in table_cells:
            cell_contents = [
                words[child_id]["Text"] if child_id in words else selections[child_id]["SelectionStatus"]
                for child_id in _get_children_ids(cell)
            ]
            i = cell["RowIndex"] - 1
            j = cell["ColumnIndex"] - 1
            content[i][j] = " ".join(cell_contents)
        # We assume that the first row corresponds to the column names
        dataframe = pd.DataFrame(content[1:], columns=content[0])
        dataframes.append(dataframe)

    return dataframes


def _get_children_ids(block):
    for rels in block.get("Relationships", []):
        if rels["Type"] == "CHILD":
            yield from rels["Ids"]


def _map_blocks(blocks, block_type):
    return {block["Id"]: block for block in blocks if block["BlockType"] == block_type}
