"""Amazon OpenSearch Read Module (PRIVATE)."""

from pandasticsearch import Select, DataFrame
from typing import Any, Dict, Optional
from elasticsearch import Elasticsearch
from elasticsearch.helpers import scan
from awswrangler.opensearch._utils import _get_distribution
import pandas as pd


def search(
    client: Elasticsearch,
    index: Optional[str] = '_all',
    search_body: Optional[Dict[str, Any]] = None,
    doc_type: Optional[str] = None,
    is_scroll: Optional[bool] = False,
    **kwargs
) -> DataFrame:
    """Returns results matching query DSL as pandas dataframe.

    Parameters
    ----------
    client : Elasticsearch
        instance of elasticsearch.Elasticsearch to use.
    index : str, optional
        A comma-separated list of index names to search.
        use `_all` or empty string to perform the operation on all indices.
    search_body : Dict[str, Any], optional
        The search definition using the [Query DSL](https://opensearch.org/docs/opensearch/query-dsl/full-text/).
    doc_type : str, optional
        Name of the document type (for Elasticsearch versions 5.x and earlier).
    is_scroll : bool, optional
        Allows to retrieve a large numbers of results from a single search request using [scroll](https://opensearch.org/docs/opensearch/rest-api/scroll/)
        for example, for machine learning jobs.
        Because scroll search contexts consume a lot of memory, we suggest you don’t use the scroll operation for frequent user queries.
    **kwargs :
        KEYWORD arguments forwarded to [elasticsearch.Elasticsearch.search](https://elasticsearch-py.readthedocs.io/en/v7.13.4/api.html#elasticsearch.Elasticsearch.search)
        and also to [elasticsearch.helpers.scan](https://elasticsearch-py.readthedocs.io/en/master/helpers.html#scan) if `is_scroll=True`

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
    if doc_type:
        kwargs['doc_type'] = doc_type

    # pandasticsearch.Select.from_dict requires `took` field
    if 'filter_path' in kwargs:
        if 'took' not in kwargs['filter_path']:
            kwargs['filter_path'].append('took')
    if is_scroll:
        documents_generator = scan(
            client,
            index=index,
            query=search_body,
            **kwargs
        )
        s = Select()
        documents = map(lambda x: s.hit_to_row(x), documents_generator)
        df = pd.DataFrame(documents)
    else:
        documents = client.search(index=index, body=search_body, **kwargs)
        df = Select.from_dict(documents).to_pandas()
    return df


def search_by_sql(
    client: Elasticsearch,
    sql_query: str,
    **kwargs
) -> DataFrame:
    """Returns results matching [SQL query](https://opensearch.org/docs/search-plugins/sql/index/) as pandas dataframe

    Parameters
    ----------
    client : Elasticsearch
        instance of elasticsearch.Elasticsearch to use.
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

    # can be used if not passing format
    def _sql_response_to_docs(response: Dict[str, Any]):
        header = list(map(lambda x: x['name'], response.get('schema', [])))
        for datarow in response.get('datarows', []):
            yield dict(zip(header, datarow))

    if _get_distribution(client) == 'opensearch':
        url = '/_plugins/_sql'
    else:
        url = '/_opendistro/_sql'

    kwargs['format'] = 'json'
    body = {'query': sql_query}
    for size_att in ['size', 'fetch_size']:
        if size_att in kwargs:
            body['fetch_size'] = kwargs[size_att]
            del kwargs[size_att]  # unrecognized parameter
    response = client.transport.perform_request(
        "POST",
        url,
        headers={'Content-Type': 'application/json'},
        body=body,
        params=kwargs
    )

    df = Select.from_dict(response).to_pandas()
    return df
