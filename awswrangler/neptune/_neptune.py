# mypy: disable-error-code=name-defined
"""Amazon Neptune Module."""

import logging
import re
import time
from typing import Any, Callable, Dict, List, Literal, Optional, TypeVar, Union

import boto3

import awswrangler.neptune._gremlin_init as gremlin
import awswrangler.pandas as pd
from awswrangler import _utils, exceptions, s3
from awswrangler._config import apply_configs
from awswrangler.neptune._client import BulkLoadParserConfiguration, NeptuneClient

gremlin_python = _utils.import_optional_dependency("gremlin_python")
opencypher = _utils.import_optional_dependency("requests")
sparql = _utils.import_optional_dependency("SPARQLWrapper")

_logger: logging.Logger = logging.getLogger(__name__)
FuncT = TypeVar("FuncT", bound=Callable[..., Any])


@_utils.check_optional_dependency(gremlin_python, "gremlin_python")
def execute_gremlin(client: NeptuneClient, query: str) -> pd.DataFrame:
    """Return results of a Gremlin traversal as pandas DataFrame.

    Parameters
    ----------
    client: neptune.Client
        instance of the neptune client to use
    query: str
        The gremlin traversal to execute

    Returns
    -------
    pandas.DataFrame
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


@_utils.check_optional_dependency(opencypher, "opencypher")
def execute_opencypher(client: NeptuneClient, query: str) -> pd.DataFrame:
    """Return results of a openCypher traversal as pandas DataFrame.

    Parameters
    ----------
    client: NeptuneClient
        instance of the neptune client to use
    query: str
        The openCypher query to execute

    Returns
    -------
    pandas.DataFrame
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


@_utils.check_optional_dependency(sparql, "SPARQLWrapper")
def execute_sparql(client: NeptuneClient, query: str) -> pd.DataFrame:
    """Return results of a SPARQL query as pandas DataFrame.

    Parameters
    ----------
    client: NeptuneClient
        instance of the neptune client to use
    query: str
        The SPARQL traversal to execute

    Returns
    -------
    pandas.DataFrame
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
        df = pd.DataFrame(data["results"]["bindings"], columns=data.get("head", {}).get("vars"))
        df = df.applymap(lambda d: d["value"] if "value" in d else None)
    else:
        df = pd.DataFrame(data)

    return df


@_utils.check_optional_dependency(gremlin_python, "gremlin_python")
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
    client: NeptuneClient
        instance of the neptune client to use
    df: pandas.DataFrame
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
    g = gremlin.traversal().withGraph(gremlin.Graph())
    is_edge_df = False
    is_update_df = True
    if "~id" in df.columns:
        if "~label" in df.columns:
            is_update_df = False
            if "~to" in df.columns and "~from" in df.columns:
                is_edge_df = True
    else:
        raise exceptions.InvalidArgumentValue(
            "DataFrame must contain at least a ~id and a ~label column to be saved to Amazon Neptune"
        )

    # Loop through items in the DF
    for index, row in df.iterrows():
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
                g = gremlin.Graph().traversal()

    return _run_gremlin_insert(client, g)


@_utils.check_optional_dependency(sparql, "SPARQLWrapper")
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
        The column name in the DataFrame for the subject.  Defaults to 's'
    predicate_column (str, optional) :
        The column name in the DataFrame for the predicate.  Defaults to 'p'
    object_column (str, optional) :
        The column name in the DataFrame for the object.  Defaults to 'o'
    graph_column (str, optional) :
        The column name in the DataFrame for the graph if sending across quads.  Defaults to 'g'

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
            """DataFrame must contain at least the subject, predicate, and object columns defined or the defaults
            (s, p, o) to be saved to Amazon Neptune"""
        )

    query = ""
    # Loop through items in the DF
    for index, row in df.iterrows():
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


BULK_LOAD_IN_PROGRESS_STATES = {"LOAD_IN_QUEUE", "LOAD_NOT_STARTED", "LOAD_IN_PROGRESS"}


@_utils.validate_distributed_kwargs(
    unsupported_kwargs=["boto3_session", "s3_additional_kwargs"],
)
@apply_configs
@_utils.check_optional_dependency(sparql, "SPARQLWrapper")
def bulk_load(
    client: NeptuneClient,
    df: pd.DataFrame,
    path: str,
    iam_role: str,
    neptune_load_wait_polling_delay: float = 0.25,
    load_parallelism: Literal["LOW", "MEDIUM", "HIGH", "OVERSUBSCRIBE"] = "HIGH",
    parser_configuration: Optional[BulkLoadParserConfiguration] = None,
    update_single_cardinality_properties: Literal["TRUE", "FALSE"] = "FALSE",
    queue_request: Literal["TRUE", "FALSE"] = "FALSE",
    dependencies: Optional[List[str]] = None,
    keep_files: bool = False,
    use_threads: Union[bool, int] = True,
    boto3_session: Optional[boto3.Session] = None,
    s3_additional_kwargs: Optional[Dict[str, str]] = None,
) -> None:
    """
    Write records into Amazon Neptune using the Neptune Bulk Loader.

    The DataFrame will be written to S3 and then loaded to Neptune using the
    `Bulk Loader <https://docs.aws.amazon.com/neptune/latest/userguide/bulk-load.html>`_.

    Parameters
    ----------
    client: NeptuneClient
        Instance of the neptune client to use
    df: DataFrame, optional
        `Pandas DataFrame <https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html>`_ to write to Neptune.
    path: str
        S3 Path that the Neptune Bulk Loader will load data from.
    iam_role: str
        The Amazon Resource Name (ARN) for an IAM role to be assumed by the Neptune DB instance for access to the S3 bucket.
        For information about creating a role that has access to Amazon S3 and then associating it with a Neptune cluster,
        see `Prerequisites: IAM Role and Amazon S3 Access <https://docs.aws.amazon.com/neptune/latest/userguide/bulk-load-tutorial-IAM.html>`_.
    neptune_load_wait_polling_delay: float
        Interval in seconds for how often the function will check if the Neptune bulk load has completed.
    load_parallelism: str
        Specifies the number of threads used by Neptune's bulk load process.
    parser_configuration: dict[str, Any], optional
        An optional object with additional parser configuration values.
        Each of the child parameters is also optional: ``namedGraphUri``, ``baseUri`` and ``allowEmptyStrings``.
    update_single_cardinality_properties: str
        An optional parameter that controls how the bulk loader
        treats a new value for single-cardinality vertex or edge properties.
    queue_request: str
        An optional flag parameter that indicates whether the load request can be queued up or not.

        If omitted or set to ``"FALSE"``, the load request will fail if another load job is already running.
    dependencies: list[str], optional
        An optional parameter that can make a queued load request contingent on the successful completion of one or more previous jobs in the queue.
    keep_files: bool
        Whether to keep stage files or delete them. False by default.
    use_threads: bool | int
        True to enable concurrent requests, False to disable multiple threads.
        If enabled os.cpu_count() will be used as the max number of threads.
        If integer is provided, specified number is used.
    boto3_session: boto3.Session(), optional
        Boto3 Session. The default boto3 session will be used if boto3_session receive None.
    s3_additional_kwargs: Dict[str, str], optional
        Forwarded to botocore requests.
        e.g. ``s3_additional_kwargs={'ServerSideEncryption': 'aws:kms', 'SSEKMSKeyId': 'YOUR_KMS_KEY_ARN'}``

    Examples
    --------
    >>> import awswrangler as wr
    >>> import pandas as pd
    >>> client = wr.neptune.connect("MY_NEPTUNE_ENDPOINT", 8182)
    >>> frame = pd.DataFrame([{"~id": "0", "~labels": ["version"], "~properties": {"type": "version"}}])
    >>> wr.neptune.bulk_load(
    ...     client=client,
    ...     df=frame,
    ...     path="s3://my-bucket/stage-files/",
    ...     iam_role="arn:aws:iam::XXX:role/XXX"
    ... )
    """
    path = path[:-1] if path.endswith("*") else path
    path = path if path.endswith("/") else f"{path}/"
    if s3.list_objects(path=path, boto3_session=boto3_session, s3_additional_kwargs=s3_additional_kwargs):
        raise exceptions.InvalidArgument(
            f"The received S3 path ({path}) is not empty. "
            "Please, provide a different path or use wr.s3.delete_objects() to clean up the current one."
        )

    try:
        s3.to_csv(df, path, use_threads=use_threads, dataset=True, index=False)

        bulk_load_from_files(
            client=client,
            path=path,
            iam_role=iam_role,
            format="csv",
            neptune_load_wait_polling_delay=neptune_load_wait_polling_delay,
            load_parallelism=load_parallelism,
            parser_configuration=parser_configuration,
            update_single_cardinality_properties=update_single_cardinality_properties,
            queue_request=queue_request,
            dependencies=dependencies,
        )
    finally:
        if keep_files is False:
            _logger.debug("Deleting objects in S3 path: %s", path)
            s3.delete_objects(
                path=path,
                use_threads=use_threads,
                boto3_session=boto3_session,
                s3_additional_kwargs=s3_additional_kwargs,
            )


@apply_configs
@_utils.check_optional_dependency(sparql, "SPARQLWrapper")
def bulk_load_from_files(
    client: NeptuneClient,
    path: str,
    iam_role: str,
    format: Literal["csv", "opencypher", "ntriples", "nquads", "rdfxml", "turtle"] = "csv",
    neptune_load_wait_polling_delay: float = 0.25,
    load_parallelism: Literal["LOW", "MEDIUM", "HIGH", "OVERSUBSCRIBE"] = "HIGH",
    parser_configuration: Optional[BulkLoadParserConfiguration] = None,
    update_single_cardinality_properties: Literal["TRUE", "FALSE"] = "FALSE",
    queue_request: Literal["TRUE", "FALSE"] = "FALSE",
    dependencies: Optional[List[str]] = None,
) -> None:
    """
    Load files from S3 into Amazon Neptune using the Neptune Bulk Loader.

    For more information about the Bulk Loader see
    `here <https://docs.aws.amazon.com/neptune/latest/userguide/bulk-load.html>`_.

    Parameters
    ----------
    client: NeptuneClient
        Instance of the neptune client to use
    path: str
        S3 Path that the Neptune Bulk Loader will load data from.
    iam_role: str
        The Amazon Resource Name (ARN) for an IAM role to be assumed by the Neptune DB instance for access to the S3 bucket.
        For information about creating a role that has access to Amazon S3 and then associating it with a Neptune cluster,
        see `Prerequisites: IAM Role and Amazon S3 Access <https://docs.aws.amazon.com/neptune/latest/userguide/bulk-load-tutorial-IAM.html>`_.
    format: str
        The format of the data.
    neptune_load_wait_polling_delay: float
        Interval in seconds for how often the function will check if the Neptune bulk load has completed.
    load_parallelism: str
        Specifies the number of threads used by Neptune's bulk load process.
    parser_configuration: dict[str, Any], optional
        An optional object with additional parser configuration values.
        Each of the child parameters is also optional: ``namedGraphUri``, ``baseUri`` and ``allowEmptyStrings``.
    update_single_cardinality_properties: str
        An optional parameter that controls how the bulk loader
        treats a new value for single-cardinality vertex or edge properties.
    queue_request: str
        An optional flag parameter that indicates whether the load request can be queued up or not.

        If omitted or set to ``"FALSE"``, the load request will fail if another load job is already running.
    dependencies: list[str], optional
        An optional parameter that can make a queued load request contingent on the successful completion of one or more previous jobs in the queue.


    Examples
    --------
    >>> import awswrangler as wr
    >>> client = wr.neptune.connect("MY_NEPTUNE_ENDPOINT", 8182)
    >>> wr.neptune.bulk_load_from_files(
    ...     client=client,
    ...     path="s3://my-bucket/stage-files/",
    ...     iam_role="arn:aws:iam::XXX:role/XXX",
    ...     format="csv",
    ... )
    """
    _logger.debug("Starting Neptune Bulk Load from %s", path)
    load_id = client.load(
        path,
        iam_role,
        format=format,
        parallelism=load_parallelism,
        parser_configuration=parser_configuration,
        update_single_cardinality_properties=update_single_cardinality_properties,
        queue_request=queue_request,
        dependencies=dependencies,
    )

    while True:
        status_response = client.load_status(load_id)

        status: str = status_response["payload"]["overallStatus"]["status"]
        if status == "LOAD_COMPLETED":
            break

        if status not in BULK_LOAD_IN_PROGRESS_STATES:
            raise exceptions.NeptuneLoadError(f"Load {load_id} failed with {status}: {status_response}")

        time.sleep(neptune_load_wait_polling_delay)

    _logger.debug("Neptune load %s has succeeded in loading %s data from %s", load_id, format, path)


def connect(host: str, port: int, iam_enabled: bool = False, **kwargs: Any) -> NeptuneClient:
    """Create a connection to a Neptune cluster.

    Parameters
    ----------
    host: str
        The host endpoint to connect to
    port: int
        The port endpoint to connect to
    iam_enabled: bool, optional
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
    g: "gremlin.GraphTraversalSource",
    use_header_cardinality: bool,
    row: Any,
    ignore_cardinality: bool = False,
) -> "gremlin.GraphTraversalSource":
    for column, value in row.items():
        if column not in ["~id", "~label", "~to", "~from"]:
            if ignore_cardinality and pd.notna(value):
                g = g.property(_get_column_name(column), value)
            elif use_header_cardinality:
                # If the column header is specifying the cardinality then use it
                if column.lower().find("(single)") > 0 and pd.notna(value):
                    g = g.property(gremlin.Cardinality.single, _get_column_name(column), value)
                else:
                    g = _expand_properties(g, _get_column_name(column), value)
            else:
                # If not using header cardinality then use the default of set
                g = _expand_properties(g, column, value)
    return g


def _expand_properties(g: "gremlin.GraphTraversalSource", column: str, value: Any) -> "gremlin.GraphTraversalSource":
    # If this is a list then expand it out into multiple property calls
    if isinstance(value, list) and len(value) > 0:
        for item in value:
            g = g.property(gremlin.Cardinality.set_, column, item)
    elif pd.notna(value):
        g = g.property(gremlin.Cardinality.set_, column, value)
    return g


def _build_gremlin_update(
    g: "gremlin.GraphTraversalSource", row: Any, use_header_cardinality: bool
) -> "gremlin.GraphTraversalSource":
    g = g.V(str(row["~id"]))
    g = _set_properties(g, use_header_cardinality, row)
    return g


def _build_gremlin_insert_vertices(
    g: "gremlin.GraphTraversalSource", row: Any, use_header_cardinality: bool = False
) -> "gremlin.GraphTraversalSource":
    g = (
        g.V(str(row["~id"]))
        .fold()
        .coalesce(
            gremlin.__.unfold(),
            gremlin.__.addV(row["~label"]).property(gremlin.T.id, str(row["~id"])),
        )
    )
    g = _set_properties(g, use_header_cardinality, row)
    return g


def _build_gremlin_insert_edges(
    g: "gremlin.GraphTraversalSource", row: pd.Series, use_header_cardinality: bool
) -> "gremlin.GraphTraversalSource":
    g = (
        g.V(str(row["~from"]))
        .fold()
        .coalesce(
            gremlin.__.unfold(),
            _build_gremlin_insert_vertices(gremlin.__, {"~id": row["~from"], "~label": "Vertex"}),
        )
        .addE(row["~label"])
        .property(gremlin.T.id, str(row["~id"]))
        .to(
            gremlin.__.V(str(row["~to"]))
            .fold()
            .coalesce(
                gremlin.__.unfold(),
                _build_gremlin_insert_vertices(gremlin.__, {"~id": row["~to"], "~label": "Vertex"}),
            )
        )
    )
    g = _set_properties(g, use_header_cardinality, row, ignore_cardinality=True)

    return g


def _run_gremlin_insert(client: NeptuneClient, g: "gremlin.GraphTraversalSource") -> bool:
    translator = gremlin.Translator("g")
    s = translator.translate(g.bytecode)
    s = s.replace("Cardinality.", "")  # hack to fix parser error for set cardinality
    s = s.replace(
        ".values('shape')", ""
    )  # hack to fix parser error for adding unknown values('shape') steps to translation.
    _logger.debug(s)
    res = client.write_gremlin(s)
    return res


def flatten_nested_df(
    df: pd.DataFrame, include_prefix: bool = True, separator: str = "_", recursive: bool = True
) -> pd.DataFrame:
    """Flatten the lists and dictionaries of the input data frame.

    Parameters
    ----------
    df: pd.DataFrame
        The input data frame
    include_prefix: bool, optional
        If True, then it will prefix the new column name with the original column name.
        Defaults to True.
    separator: str, optional
        The separator to use between field names when a dictionary is exploded.
        Defaults to "_".
    recursive: bool, optional
        If True, then this will recurse the fields in the data frame. Defaults to True.

    Returns
    -------
        pd.DataFrame: The flattened data frame
    """
    if separator is None:
        separator = "_"
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
                expanded = pd.json_normalize(df[col], sep=separator).add_prefix(f"{col}{separator}")
            else:
                expanded = pd.json_normalize(df[col], sep=separator).add_prefix(f"{separator}")
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
            df = flatten_nested_df(df, include_prefix=include_prefix, separator=separator, recursive=recursive)

    return df
