from awswrangler.neptune.client import NeptuneClient
from typing import Dict, Any
import pandas as pd
from awswrangler import exceptions
from gremlin_python.process.graph_traversal import GraphTraversalSource
from gremlin_python.process.translator import Translator
from gremlin_python.process.traversal import T
from gremlin_python.process.graph_traversal import __
from gremlin_python.process.traversal import Cardinality
from gremlin_python.structure.graph import Graph

import logging

_logger: logging.Logger = logging.getLogger(__name__)


def execute_gremlin(
        client: NeptuneClient,
        query: str,
        **kwargs: str
) -> pd.DataFrame:
    """Return results of a Gremlin traversal as pandas dataframe.

    Parameters
    ----------
    client : neptune.Client
        instance of the neptune client to use
    traversal : str
        The gremlin traversal to execute

    Returns
    -------
    Union[pandas.DataFrame, Iterator[pandas.DataFrame]]
        Results as Pandas DataFrame

    Examples
    --------
    Run a Gremlin Query

    >>> import awswrangler as wr
    >>> client = wr.neptune.connect(neptune_endpoint, neptune_port, ssl=False, iam_enabled=False)
    >>> df = wr.neptune.execute_gremlin(client, "g.V().limit(1)")
    """
    results = client.read_gremlin(query, **kwargs)
    df = pd.DataFrame.from_records(results)
    return df


def execute_opencypher(
        client: NeptuneClient,
        query: str
) -> pd.DataFrame:
    """Return results of a openCypher traversal as pandas dataframe.

    Parameters
    ----------
    client : NeptuneClient
        instance of the neptune client to use
    query : str
        The openCypher query to execute

    Returns
    -------
    Union[pandas.DataFrame, Iterator[pandas.DataFrame]]
        Results as Pandas DataFrame

    Examples
    --------
    Run an openCypher query

    >>> import awswrangler as wr
    >>> client = wr.neptune.connect(neptune_endpoint, neptune_port, ssl=True, iam_enabled=False)
    >>> resp = wr.neptune.execute_opencypher(client, "MATCH (n) RETURN n LIMIT 1")
    """
    resp = client.read_opencypher(query)
    df = pd.DataFrame.from_dict(resp)
    return df


def execute_sparql(
        client: NeptuneClient,
        query: str
) -> pd.DataFrame:
    """Return results of a SPARQL query as pandas dataframe.

    Parameters
    ----------
    client : NeptuneClient
        instance of the neptune client to use
    query : str
        The SPARQL traversal to execute

    Returns
    -------
    Union[pandas.DataFrame, Iterator[pandas.DataFrame]]
        Results as Pandas DataFrame

    Examples
    --------
    Run a SPARQL query

    >>> import awswrangler as wr
    >>> client = wr.neptune.Client(host='NEPTUNE-ENDPOINT')
    >>> df = wr.neptune.execute_sparql(client, "PREFIX foaf:  <http://xmlns.com/foaf/0.1/>
    SELECT ?name
    WHERE {
            ?person foaf:name ?name .
    }")
    """
    resp = client.read_sparql(query)
    data = resp.json()
    df = pd.DataFrame(data['results']['bindings'])
    df.applymap(lambda x: x['value'])
    return df


def to_property_graph(
        client: NeptuneClient,
        df: pd.DataFrame,
        batch_size: int = 50
) -> None:
    """Write records stored in a DataFrame into Amazon Neptune.    
    
    If writing to a property graph then DataFrames for vertices and edges must be written separately. 
    DataFrames for vertices must have a ~label column with the label and a ~id column for the vertex id.  
    If the ~id column does not exist, the specified id does not exists, or is empty then a new vertex will be added.  
    If no ~label column exists an exception will be thrown.  
    DataFrames for edges must have a ~id, ~label, ~to, and ~from column.  If the ~id column does not exist, 
    the specified id does not exists, or is empty then a new edge will be added. If no ~label, ~to, or ~from column exists an exception will be thrown.  

    Parameters
    ----------
    client : NeptuneClient
        instance of the neptune client to use
    df : pandas.DataFrame
        Pandas DataFrame https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html

    Returns
    -------
    None
        None.

    Examples
    --------
    Writing to Amazon Neptune 

    >>> import awswrangler as wr
    >>> client = wr.neptune.Client(host='NEPTUNE-ENDPOINT')
    >>> wr.neptune.gremlin.to_property_graph(
    ...     df=df
    ... )
    """
    # check if ~id and ~label column exist and if not throw error
    g = Graph().traversal()
    is_edge_df = False
    is_update_df=True
    if '~id' in df.columns:
        if '~label' in df.columns:
            is_update_df = False
            if '~to' in df.columns and '~from' in df.columns:
                is_edge_df = True
    else:
        raise exceptions.InvalidArgumentValue(
            "Dataframe must contain at least a ~id and a ~label column to be saved to Amazon Neptune"
        )

    # Loop through items in the DF
    for (index, row) in df.iterrows():
        # build up a query 
        if is_update_df:
            g = _build_gremlin_update(g, row)
        elif is_edge_df:
            g = _build_gremlin_insert_edges(g, row.to_dict())
        else:
            g = _build_gremlin_insert_vertices(g, row.to_dict())
        # run the query
        if index > 0 and index % batch_size == 0:
            res = _run_gremlin_insert(client, g)
            if res:
                g = Graph().traversal()

    return _run_gremlin_insert(client, g)


def _build_gremlin_update(g: GraphTraversalSource, row: Dict) -> str:
    g = g.V(str(row['~id']))
    for (column, value) in row.items():
        if column not in ['~id', '~label']:
            if type(value) is list and len(value) > 0:
                for item in value:
                    g = g.property(Cardinality.set_, column, item)
            elif not pd.isna(value) and not pd.isnull(value):
                g = g.property(column, value)

    return g

def _build_gremlin_insert_vertices(g: GraphTraversalSource, row: Dict) -> str:
    g = (g.V(str(row['~id'])).
        fold().
        coalesce(
        __.unfold(),
        __.addV(row['~label']).property(T.id, str(row['~id'])))
    )
    for (column, value) in row.items():
        if column not in ['~id', '~label']:
            if type(value) is list and len(value) > 0:
                for item in value:
                    g = g.property(Cardinality.set_, column, item)
            elif not pd.isna(value) and not pd.isnull(value):
                g = g.property(column, value)

    return g


def _build_gremlin_insert_edges(g: GraphTraversalSource, row: pd.Series) -> str:
    g = (g.V(str(row['~from'])).
         fold().
         coalesce(
        __.unfold(),
        _build_gremlin_insert_vertices(__, {"~id": row['~from'], "~label": "Vertex"})).
         addE(row['~label']).
         to(__.V(str(row['~to'])).fold().coalesce(__.unfold(), _build_gremlin_insert_vertices(__, {"~id": row['~to'],
                                                                                                   "~label": "Vertex"})))
         )
    for (column, value) in row.items():
        if column not in ['~id', '~label', '~to', '~from']:
            if type(value) is list and len(value) > 0:
                for item in value:
                    g = g.property(Cardinality.set_, column, item)
            elif not pd.isna(value) and not pd.isnull(value):
                g = g.property(column, value)

    return g


def _run_gremlin_insert(client: NeptuneClient, g: GraphTraversalSource) -> bool:
    translator = Translator('g')
    s = translator.translate(g.bytecode)
    s = s.replace('Cardinality.', '')  # hack to fix parser error for set cardinality
    _logger.debug(s)
    res = client.write_gremlin(s)
    return res


def to_rdf_graph(
        client: NeptuneClient,
        df: pd.DataFrame,
        batch_size: int = 50,
        subject_column:str = 's',
        predicate_column:str = 'p',
        object_column:str = 'o',
        graph_column:str = 'g'
) -> None:
    """Write records stored in a DataFrame into Amazon Neptune.    
    
    The DataFrame must consist of triples with column names for the subject, predicate, and object specified.  
    If you want to add data into a named graph then you will also need the graph column.

    Parameters
    ----------
    client : NeptuneClient
        instance of the neptune client to use
    df : pandas.DataFrame
        Pandas DataFrame https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html

    Returns
    -------
    None
        None.

    Examples
    --------
    Writing to Amazon Neptune 

    >>> import awswrangler as wr
    >>> client = wr.neptune.Client(host='NEPTUNE-ENDPOINT')
    >>> wr.neptune.gremlin.to_rdf_graph(
    ...     df=df
    ... )
    """
    is_quads = False
    if pd.Series([subject_column, object_column, predicate_column]).isin(df.columns).all():
        if graph_column in df.columns:
            is_quads = True
    else:
        raise exceptions.InvalidArgumentValue(
            "Dataframe must contain at least the subject, predicate, and object columns defined or the defaults (s, p, o) to be saved to Amazon Neptune"
        )

    query = ""
    # Loop through items in the DF
    for (index, row) in df.iterrows():
        # build up a query 
        if is_quads:            
            insert = f"INSERT DATA {{ GRAPH <{row[graph_column]}> {{<{row[subject_column]}> <{str(row[predicate_column])}> <{row[object_column]}> . }} }}; "
            query = query + insert
        else:
            insert = f"INSERT DATA {{ <{row[subject_column]}> <{str(row[predicate_column])}> <{row[object_column]}> . }}; "
            query = query + insert
        # run the query
        if index > 0 and index % batch_size == 0:
            res = client.write_sparql(query)
            if res:
                query=""
    return client.write_sparql(query)


def connect(host: str, port: str, iam_enabled: bool = False, **kwargs: Any) -> NeptuneClient:
    return NeptuneClient(host, port, iam_enabled, **kwargs)
