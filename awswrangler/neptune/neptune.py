"""Amazon Neptune Module."""

import importlib.util
import inspect
import logging
import re
from typing import Any, Callable, TypeVar

import pandas as pd
from gremlin_python.process.anonymous_traversal import traversal
from gremlin_python.process.graph_traversal import GraphTraversalSource, __
from gremlin_python.process.translator import Translator
from gremlin_python.process.traversal import Cardinality, T
from gremlin_python.structure.graph import Graph

from awswrangler import exceptions
from awswrangler.neptune.client import NeptuneClient

_SPARQLWrapper_found = importlib.util.find_spec("SPARQLWrapper")

_logger: logging.Logger = logging.getLogger(__name__)
FuncT = TypeVar("FuncT", bound=Callable[..., Any])


def _check_for_sparqlwrapper(func: FuncT) -> FuncT:
    def inner(*args: Any, **kwargs: Any) -> Any:
        if not _SPARQLWrapper_found:
            raise ModuleNotFoundError(
                "You need to install SPARQLWrapper respectively the "
                "AWS SDK for pandas package with the `sparql` extra for being able to use SPARQL "
            )
        return func(*args, **kwargs)

    inner.__doc__ = func.__doc__
    inner.__name__ = func.__name__
    inner.__setattr__("__signature__", inspect.signature(func))  # pylint: disable=no-member
    return inner  # type: ignore


def execute_gremlin(client: NeptuneClient, query: str) -> pd.DataFrame:
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
        >>> client = wr.neptune.connect(neptune_endpoint, neptune_port, iam_enabled=False)
    >>> df = wr.neptune.execute_gremlin(client, "g.V().limit(1)")
    """
    results = client.read_gremlin(query)
    df = pd.DataFrame.from_records(results)
    return df


def execute_opencypher(client: NeptuneClient, query: str) -> pd.DataFrame:
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
    >>> client = wr.neptune.connect(neptune_endpoint, neptune_port, iam_enabled=False)
    >>> resp = wr.neptune.execute_opencypher(client, "MATCH (n) RETURN n LIMIT 1")
    """
    resp = client.read_opencypher(query)
    df = pd.DataFrame.from_dict(resp)
    return df


@_check_for_sparqlwrapper
def execute_sparql(client: NeptuneClient, query: str) -> pd.DataFrame:
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
    >>> client = wr.neptune.connect(neptune_endpoint, neptune_port, iam_enabled=False)
    >>> df = wr.neptune.execute_sparql(client, "PREFIX foaf:  <http://xmlns.com/foaf/0.1/>
    SELECT ?name
    WHERE {
            ?person foaf:name ?name .
    """
    data = client.read_sparql(query)
    df = None
    if "results" in data and "bindings" in data["results"]:
        df = pd.DataFrame(data["results"]["bindings"])
        df.applymap(lambda x: x["value"])
    else:
        df = pd.DataFrame(data)

    return df


def to_property_graph(
    client: NeptuneClient, df: pd.DataFrame, batch_size: int = 50, use_header_cardinality: bool = True
) -> bool:
    """Write records stored in a DataFrame into Amazon Neptune.

    If writing to a property graph then DataFrames for vertices and edges must be written separately.
    DataFrames for vertices must have a ~label column with the label and a ~id column for the vertex id.
    If the ~id column does not exist, the specified id does not exists, or is empty then a new vertex will be added.
    If no ~label column exists an exception will be thrown.
    DataFrames for edges must have a ~id, ~label, ~to, and ~from column.  If the ~id column does not exist
    the specified id does not exists, or is empty then a new edge will be added. If no ~label, ~to, or ~from column
    exists an exception will be thrown.

    If you would like to save data using `single` cardinality then you can postfix (single) to the column header and
    set use_header_cardinality=True (default).  e.g. A column named `name(single)` will save the `name` property
    as single
    cardinality.  You can disable this by setting by setting `use_header_cardinality=False`.

    Parameters
    ----------
    client : NeptuneClient
        instance of the neptune client to use
    df : pandas.DataFrame
        Pandas DataFrame https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html
    batch_size: int
        The number of rows to save at a time. Default 50
    use_header_cardinality: bool
        If True, then the header cardinality will be used to save the data. Default True

    Returns
    -------
    bool
        True if records were written

    Examples
    --------
    Writing to Amazon Neptune

    >>> import awswrangler as wr
    >>> client = wr.neptune.connect(neptune_endpoint, neptune_port, iam_enabled=False)
    >>> wr.neptune.gremlin.to_property_graph(
    ...     df=df
    ... )
    """
    # check if ~id and ~label column exist and if not throw error
    g = traversal().withGraph(Graph())
    is_edge_df = False
    is_update_df = True
    if "~id" in df.columns:
        if "~label" in df.columns:
            is_update_df = False
            if "~to" in df.columns and "~from" in df.columns:
                is_edge_df = True
    else:
        raise exceptions.InvalidArgumentValue(
            "Dataframe must contain at least a ~id and a ~label column to be saved to Amazon Neptune"
        )

    # Loop through items in the DF
    for (index, row) in df.iterrows():
        # build up a query
        if is_update_df:
            g = _build_gremlin_update(g, row, use_header_cardinality)
        elif is_edge_df:
            g = _build_gremlin_insert_edges(g, row.to_dict(), use_header_cardinality)
        else:
            g = _build_gremlin_insert_vertices(g, row.to_dict(), use_header_cardinality)
        # run the query
        if index > 0 and index % batch_size == 0:
            res = _run_gremlin_insert(client, g)
            if res:
                g = Graph().traversal()

    return _run_gremlin_insert(client, g)


@_check_for_sparqlwrapper
def to_rdf_graph(
    client: NeptuneClient,
    df: pd.DataFrame,
    batch_size: int = 50,
    subject_column: str = "s",
    predicate_column: str = "p",
    object_column: str = "o",
    graph_column: str = "g",
) -> bool:
    """Write records stored in a DataFrame into Amazon Neptune.

    The DataFrame must consist of triples with column names for the subject, predicate, and object specified.
    If you want to add data into a named graph then you will also need the graph column.

    Parameters
    ----------
    client (NeptuneClient) :
        instance of the neptune client to use
    df (pandas.DataFrame) :
        Pandas DataFrame https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html
    subject_column (str, optional) :
        The column name in the dataframe for the subject.  Defaults to 's'
    predicate_column (str, optional) :
        The column name in the dataframe for the predicate.  Defaults to 'p'
    object_column (str, optional) :
        The column name in the dataframe for the object.  Defaults to 'o'
    graph_column (str, optional) :
        The column name in the dataframe for the graph if sending across quads.  Defaults to 'g'

    Returns
    -------
    bool
        True if records were written

    Examples
    --------
    Writing to Amazon Neptune

    >>> import awswrangler as wr
    >>> client = wr.neptune.connect(neptune_endpoint, neptune_port, iam_enabled=False)
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
            """Dataframe must contain at least the subject, predicate, and object columns defined or the defaults
            (s, p, o) to be saved to Amazon Neptune"""
        )

    query = ""
    # Loop through items in the DF
    for (index, row) in df.iterrows():
        # build up a query
        if is_quads:
            insert = f"""INSERT DATA {{ GRAPH <{row[graph_column]}> {{<{row[subject_column]}>
                    <{str(row[predicate_column])}> <{row[object_column]}> . }} }}; """
            query = query + insert
        else:
            insert = f"""INSERT DATA {{ <{row[subject_column]}> <{str(row[predicate_column])}>
                    <{row[object_column]}> . }}; """
            query = query + insert
        # run the query
        if index > 0 and index % batch_size == 0:
            res = client.write_sparql(query)
            if res:
                query = ""
    return client.write_sparql(query)


def connect(host: str, port: int, iam_enabled: bool = False, **kwargs: Any) -> NeptuneClient:
    """Create a connection to a Neptune cluster.

    Parameters
    ----------
    host : str
        The host endpoint to connect to
    port : int
        The port endpoint to connect to
    iam_enabled : bool, optional
        True if IAM is enabled on the cluster. Defaults to False.

    Returns
    -------
    NeptuneClient
        [description]
    """
    return NeptuneClient(host, port, iam_enabled, **kwargs)


def _get_column_name(column: str) -> str:
    if "(single)" in column.lower():
        return re.compile(r"\(single\)", re.IGNORECASE).sub("", column)
    return column


def _set_properties(
    g: GraphTraversalSource, use_header_cardinality: bool, row: Any, ignore_cardinality: bool = False
) -> GraphTraversalSource:
    for (column, value) in row.items():
        if column not in ["~id", "~label", "~to", "~from"]:
            if ignore_cardinality and pd.notna(value):
                g = g.property(_get_column_name(column), value)
            else:
                # If the column header is specifying the cardinality then use it
                if use_header_cardinality:
                    if column.lower().find("(single)") > 0 and pd.notna(value):
                        g = g.property(Cardinality.single, _get_column_name(column), value)
                    else:
                        g = _expand_properties(g, _get_column_name(column), value)
                else:
                    # If not using header cardinality then use the default of set
                    g = _expand_properties(g, column, value)
    return g


def _expand_properties(g: GraphTraversalSource, column: str, value: Any) -> GraphTraversalSource:
    # If this is a list then expand it out into multiple property calls
    if isinstance(value, list) and len(value) > 0:
        for item in value:
            g = g.property(Cardinality.set_, column, item)
    elif pd.notna(value):
        g = g.property(Cardinality.set_, column, value)
    return g


def _build_gremlin_update(g: GraphTraversalSource, row: Any, use_header_cardinality: bool) -> GraphTraversalSource:
    g = g.V(str(row["~id"]))
    g = _set_properties(g, use_header_cardinality, row)
    return g


def _build_gremlin_insert_vertices(
    g: GraphTraversalSource, row: Any, use_header_cardinality: bool = False
) -> GraphTraversalSource:
    g = g.V(str(row["~id"])).fold().coalesce(__.unfold(), __.addV(row["~label"]).property(T.id, str(row["~id"])))
    g = _set_properties(g, use_header_cardinality, row)
    return g


def _build_gremlin_insert_edges(
    g: GraphTraversalSource, row: pd.Series, use_header_cardinality: bool
) -> GraphTraversalSource:
    g = (
        g.V(str(row["~from"]))
        .fold()
        .coalesce(__.unfold(), _build_gremlin_insert_vertices(__, {"~id": row["~from"], "~label": "Vertex"}))
        .addE(row["~label"])
        .property(T.id, str(row["~id"]))
        .to(
            __.V(str(row["~to"]))
            .fold()
            .coalesce(__.unfold(), _build_gremlin_insert_vertices(__, {"~id": row["~to"], "~label": "Vertex"}))
        )
    )
    g = _set_properties(g, use_header_cardinality, row, ignore_cardinality=True)

    return g


def _run_gremlin_insert(client: NeptuneClient, g: GraphTraversalSource) -> bool:
    translator = Translator("g")
    s = translator.translate(g.bytecode)
    s = s.replace("Cardinality.", "")  # hack to fix parser error for set cardinality
    s = s.replace(
        ".values('shape')", ""
    )  # hack to fix parser error for adding unknown values('shape') steps to translation.
    _logger.debug(s)
    res = client.write_gremlin(s)
    return res


def flatten_nested_df(
    df: pd.DataFrame, include_prefix: bool = True, seperator: str = "_", recursive: bool = True
) -> pd.DataFrame:
    """Flatten the lists and dictionaries of the input data frame.

    Parameters
    ----------
    df : pd.DataFrame
        The input data frame
    include_prefix : bool, optional
        If True, then it will prefix the new column name with the original column name.
        Defaults to True.
    seperator : str, optional
        The seperator to use between field names when a dictionary is exploded.
        Defaults to "_".
    recursive : bool, optional
        If True, then this will recurse the fields in the data frame. Defaults to True.

    Returns
    -------
        pd.DataFrame: The flattened data frame
    """
    if seperator is None:
        seperator = "_"
    df = df.reset_index()

    # search for list and map
    s = (df.applymap(type) == list).all()
    list_columns = s[s].index.tolist()

    s = (df.applymap(type) == dict).all()
    dict_columns = s[s].index.tolist()

    if len(list_columns) > 0 or len(dict_columns) > 0:
        new_columns = []

        for col in dict_columns:
            # expand dictionaries horizontally
            expanded = None
            if include_prefix:
                expanded = pd.json_normalize(df[col], sep=seperator).add_prefix(f"{col}{seperator}")
            else:
                expanded = pd.json_normalize(df[col], sep=seperator).add_prefix(f"{seperator}")
            expanded.index = df.index
            df = pd.concat([df, expanded], axis=1).drop(columns=[col])
            new_columns.extend(expanded.columns)

        for col in list_columns:
            df = df.drop(columns=[col]).join(df[col].explode().to_frame())
            new_columns.append(col)

        # check if there are still dict o list fields to flatten
        s = (df[new_columns].applymap(type) == list).all()
        list_columns = s[s].index.tolist()

        s = (df[new_columns].applymap(type) == dict).all()
        dict_columns = s[s].index.tolist()
        if recursive and (len(list_columns) > 0 or len(dict_columns) > 0):
            df = flatten_nested_df(df, include_prefix=include_prefix, seperator=seperator, recursive=recursive)

    return df
