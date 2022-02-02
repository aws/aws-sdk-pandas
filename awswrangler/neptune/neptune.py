from awswrangler.neptune.client import NeptuneClient
from typing import Any, Dict
import pandas as pd


def read_gremlin(
    client: NeptuneClient,
    query: str,
    **kwargs
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
    >>> df = wr.neptune.read_gremlin(client, "g.V().limit(1)")
    """
    results = client.read_gremlin(query, kwargs)
    df = pd.DataFrame.from_records(results)
    return df


def read_opencypher(
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
    >>> resp = wr.neptune.read_opencypher(client, "MATCH (n) RETURN n LIMIT 1")
    """
    resp = client.read_opencypher(query)
    df = pd.DataFrame.from_dict(resp)
    return df


def read_sparql(
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
    >>> df = wr.neptune.read_sparql(client, "PREFIX foaf:  <http://xmlns.com/foaf/0.1/>
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
    batch_size: int=50,
    **kwargs
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
    #check if ~id and ~label column exist and if not throw error

    #Loop through items in the DF
        # build up a query 
        # run the query
    raise NotImplementedError


def to_rdf_graph(
    client: NeptuneClient,
    df: pd.DataFrame
) -> None:
    """Write records stored in a DataFrame into Amazon Neptune.    
    
    The DataFrame must consist of triples with column names of s, p, and o.

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
    raise NotImplementedError

