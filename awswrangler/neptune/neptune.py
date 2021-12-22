from awswrangler.neptune import client
from typing import Any
import pandas as pd

def read_gremlin(
    client: client,
    traversal: str
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
    >>> client = wr.neptune.Client(host='NEPTUNE-ENDPOINT')
    >>> df = wr.neptune.gremlin.read(client, "g.V().limit(5).valueMap()")


    """
    raise NotImplementedError

def read_opencypher(
    client: client,
    traversal: str
) -> pd.DataFrame:
    """Return results of a openCypher traversal as pandas dataframe.

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
    Run an openCypher query

    >>> import awswrangler as wr
    >>> client = wr.neptune.Client(host='NEPTUNE-ENDPOINT')
    >>> df = wr.neptune.gremlin.read(client, "MATCH (n) RETURN n LIMIT 5")


    """
    raise NotImplementedError

def read_sparql(
    client: client,
    traversal: str
) -> pd.DataFrame:
    """Return results of a SPARQL query as pandas dataframe.

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
    Run a SPARQL query

    >>> import awswrangler as wr
    >>> client = wr.neptune.Client(host='NEPTUNE-ENDPOINT')
    >>> df = wr.neptune.sparql.read(client, "PREFIX foaf:  <http://xmlns.com/foaf/0.1/>
    SELECT ?name
    WHERE {
            ?person foaf:name ?name .
    }")
    """
    raise NotImplementedError

def to_graph(
    client: client,
    df: pd.DataFrame
) -> None:
    """Write records of triples stored in a DataFrame into Amazon Neptune.

    Parameters
    ----------
    client : neptune.Client
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
    >>> wr.neptune.sparql.to_graph(
    ...     df=df
    ... )
    """
    raise NotImplementedError

def to_graph(
    client: client,
    df: pd.DataFrame
) -> None:
    """Write records stored in a DataFrame into Amazon Neptune.    
    
    If using property graphs then DataFrames for vertices and edges must be written as separete
    data frames. DataFrames for vertices must have a ~label column with the label and a ~id column for the vertex id.  
    If the ~id column does not exist, the specified id does not exists, or is empty then a new vertex will be added.  
    If no ~label column exists an exception will be thrown.  
    DataFrames for edges must have a ~id, ~label, ~to, and ~from column.  If the ~id column does not exist, 
    the specified id does not exists, or is empty then a new edge will be added. If no ~label, ~to, or ~from column exists an exception will be thrown.  

    If using RDF then the DataFrame must consist of triples with column names of s, p, and o.

    Parameters
    ----------
    client : neptune.Client
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
    >>> wr.neptune.gremlin.to_graph(
    ...     df=df
    ... )
    """
    raise NotImplementedError