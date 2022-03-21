"""Amazon Neptune Utils Module (PRIVATE)."""

import logging
from enum import Enum
from typing import Any

import pandas as pd
from gremlin_python.process.graph_traversal import GraphTraversalSource, __
from gremlin_python.process.translator import Translator
from gremlin_python.process.traversal import Cardinality, T
from gremlin_python.structure.graph import Graph

from awswrangler import exceptions
from awswrangler.neptune.client import NeptuneClient

_logger: logging.Logger = logging.getLogger(__name__)


class WriteDFType(Enum):
    """Dataframe type enum."""

    VERTEX = 1
    EDGE = 2
    UPDATE = 3


def write_gremlin_df(client: NeptuneClient, df: pd.DataFrame, mode: WriteDFType, batch_size: int) -> bool:
    """Write the provided dataframe using Gremlin.

    Parameters
    ----------
    client : NeptuneClient
        The Neptune client to write the dataframe
    df : pd.DataFrame
        The dataframe to write
    mode : WriteDFType
        The type of dataframe to write
    batch_size : int
        The size of the batch to write

    Returns
    -------
    bool
        True if the write operation succeeded
    """
    g = Graph().traversal()
    # Loop through items in the DF
    for (index, row) in df.iterrows():
        # build up a query
        if mode == WriteDFType.EDGE:
            g = _build_gremlin_edges(g, row.to_dict())
        elif mode == WriteDFType.VERTEX:
            g = _build_gremlin_vertices(g, row.to_dict())
        else:
            g = _build_gremlin_update(g, row.to_dict())
        # run the query
        if index > 0 and index % batch_size == 0:
            res = _run_gremlin_insert(client, g)
            if res:
                g = Graph().traversal()
            else:
                _logger.debug(res)
                raise exceptions.QueryFailed(
                    """Failed to insert part or all of the data in the DataFrame, please check the log output."""
                )

    return _run_gremlin_insert(client, g)


def _run_gremlin_insert(client: NeptuneClient, g: GraphTraversalSource) -> bool:
    translator = Translator("g")
    s = translator.translate(g.bytecode)
    s = s.replace("Cardinality.", "")  # hack to fix parser error for set cardinality
    _logger.debug(s)
    res = client.write_gremlin(s)
    return res


def _build_gremlin_update(g: GraphTraversalSource, row: Any) -> GraphTraversalSource:
    g = g.V(str(row["~id"]))
    g = _build_gremlin_properties(g, row)

    return g


def _build_gremlin_vertices(g: GraphTraversalSource, row: Any) -> GraphTraversalSource:
    g = g.V(str(row["~id"])).fold().coalesce(__.unfold(), __.addV(row["~label"]).property(T.id, str(row["~id"])))
    g = _build_gremlin_properties(g, row)

    return g


def _build_gremlin_edges(g: GraphTraversalSource, row: pd.Series) -> GraphTraversalSource:
    g = (
        g.V(str(row["~from"]))
        .fold()
        .coalesce(__.unfold(), _build_gremlin_vertices(__, {"~id": row["~from"], "~label": "Vertex"}))
        .addE(row["~label"])
        .to(
            __.V(str(row["~to"]))
            .fold()
            .coalesce(__.unfold(), _build_gremlin_vertices(__, {"~id": row["~to"], "~label": "Vertex"}))
        )
    )
    g = _build_gremlin_properties(g, row)

    return g


def _build_gremlin_properties(g: GraphTraversalSource, row: Any) -> GraphTraversalSource:
    for (column, value) in row.items():
        if column not in ["~id", "~label", "~to", "~from"]:
            if isinstance(value, list) and len(value) > 0:
                for item in value:
                    g = g.property(Cardinality.set_, column, item)
            elif not pd.isna(value) and not pd.isnull(value):
                g = g.property(column, value)
    return g
