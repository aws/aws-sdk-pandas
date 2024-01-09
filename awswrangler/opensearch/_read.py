# mypy: disable-error-code=name-defined
"""Amazon OpenSearch Read Module (PRIVATE)."""

from __future__ import annotations

from typing import Any, Collection, Mapping

import awswrangler.pandas as pd
from awswrangler import _utils, exceptions
from awswrangler.opensearch._utils import _get_distribution, _is_serverless

opensearchpy = _utils.import_optional_dependency("opensearchpy")


def _resolve_fields(row: Mapping[str, Any]) -> Mapping[str, Any]:
    fields = {}
    for field in row:
        if isinstance(row[field], dict):
            nested_fields = _resolve_fields(row[field])
            for n_field, val in nested_fields.items():
                fields[f"{field}.{n_field}"] = val
        else:
            fields[field] = row[field]
    return fields


def _hit_to_row(hit: Mapping[str, Any]) -> Mapping[str, Any]:
    row: dict[str, Any] = {}
    for k in hit.keys():
        if k == "_source":
            solved_fields = _resolve_fields(hit["_source"])
            row.update(solved_fields)
        elif k.startswith("_"):
            row[k] = hit[k]
    return row


def _search_response_to_documents(response: Mapping[str, Any]) -> list[Mapping[str, Any]]:
    return [_hit_to_row(hit) for hit in response.get("hits", {}).get("hits", [])]


def _search_response_to_df(response: Mapping[str, Any] | Any) -> pd.DataFrame:
    return pd.DataFrame(_search_response_to_documents(response))


@_utils.check_optional_dependency(opensearchpy, "opensearchpy")
def search(
    client: "opensearchpy.OpenSearch",
    index: str | None = "_all",
    search_body: dict[str, Any] | None = None,
    doc_type: str | None = None,
    is_scroll: bool | None = False,
    filter_path: str | Collection[str] | None = None,
    **kwargs: Any,
) -> pd.DataFrame:
    """Return results matching query DSL as pandas DataFrame.

    Parameters
    ----------
    client : OpenSearch
        instance of opensearchpy.OpenSearch to use.
    index : str, optional
        A comma-separated list of index names to search.
        use `_all` or empty string to perform the operation on all indices.
    search_body : Dict[str, Any], optional
        The search definition using the `Query DSL <https://opensearch.org/docs/opensearch/query-dsl/full-text/>`_.
    doc_type : str, optional
        Name of the document type (for Elasticsearch versions 5.x and earlier).
    is_scroll : bool, optional
        Allows to retrieve a large numbers of results from a single search request using
        `scroll <https://opensearch.org/docs/opensearch/rest-api/scroll/>`_
        for example, for machine learning jobs.
        Because scroll search contexts consume a lot of memory, we suggest you don’t use the scroll operation
        for frequent user queries.
    filter_path : Union[str, Collection[str]], optional
        Use the filter_path parameter to reduce the size of the OpenSearch Service response \
(default: ['hits.hits._id','hits.hits._source'])
    **kwargs :
        KEYWORD arguments forwarded to `opensearchpy.OpenSearch.search \
<https://opensearch-py.readthedocs.io/en/latest/api.html#opensearchpy.OpenSearch.search>`_
        and also to `opensearchpy.helpers.scan <https://opensearch-py.readthedocs.io/en/master/helpers.html#scan>`_
        if `is_scroll=True`

    Returns
    -------
    Union[pandas.DataFrame, Iterator[pandas.DataFrame]]
        Results as Pandas DataFrame

    Examples
    --------
    Searching an index using query DSL

    >>> import awswrangler as wr
    >>> client = wr.opensearch.connect(host='DOMAIN-ENDPOINT')
    >>> df = wr.opensearch.search(
    ...         client=client,
    ...         index='movies',
    ...         search_body={
    ...           "query": {
    ...             "match": {
    ...               "title": "wind"
    ...             }
    ...           }
    ...         }
    ...      )


    """
    if is_scroll and _is_serverless(client):
        raise exceptions.NotSupported("Scrolled search is not currently available for OpenSearch Serverless.")

    if doc_type:
        kwargs["doc_type"] = doc_type

    if filter_path is None:
        filter_path = ["hits.hits._id", "hits.hits._source"]

    if is_scroll:
        if isinstance(filter_path, str):
            filter_path = [filter_path]
        filter_path = ["_scroll_id", "_shards"] + list(filter_path)  # required for scroll
        documents_generator = opensearchpy.helpers.scan(
            client, index=index, query=search_body, filter_path=filter_path, **kwargs
        )
        documents = [_hit_to_row(doc) for doc in documents_generator]
        df = pd.DataFrame(documents)
    else:
        response = client.search(index=index, body=search_body, filter_path=filter_path, **kwargs)
        df = _search_response_to_df(response)
    return df


@_utils.check_optional_dependency(opensearchpy, "opensearchpy")
def search_by_sql(client: "opensearchpy.OpenSearch", sql_query: str, **kwargs: Any) -> pd.DataFrame:
    """Return results matching `SQL query <https://opensearch.org/docs/search-plugins/sql/index/>`_ as pandas DataFrame.

    Parameters
    ----------
    client : OpenSearch
        instance of opensearchpy.OpenSearch to use.
    sql_query : str
        SQL query
    **kwargs :
        KEYWORD arguments forwarded to request url (e.g.: filter_path, etc.)

    Returns
    -------
    Union[pandas.DataFrame, Iterator[pandas.DataFrame]]
        Results as Pandas DataFrame

    Examples
    --------
    Searching an index using SQL query

    >>> import awswrangler as wr
    >>> client = wr.opensearch.connect(host='DOMAIN-ENDPOINT')
    >>> df = wr.opensearch.search_by_sql(
    >>>         client=client,
    >>>         sql_query='SELECT * FROM my-index LIMIT 50'
    >>>      )


    """
    if _is_serverless(client):
        raise exceptions.NotSupported("SQL plugin is not currently available for OpenSearch Serverless.")

    if _get_distribution(client) == "opensearch":
        url = "/_plugins/_sql"
    else:
        url = "/_opendistro/_sql"

    kwargs["format"] = "json"
    body = {"query": sql_query}
    for size_att in ["size", "fetch_size"]:
        if size_att in kwargs:
            body["fetch_size"] = kwargs[size_att]
            del kwargs[size_att]  # unrecognized parameter
    response = client.transport.perform_request(
        "POST", url, headers={"content-type": "application/json"}, body=body, params=kwargs
    )
    df = _search_response_to_df(response)
    return df
