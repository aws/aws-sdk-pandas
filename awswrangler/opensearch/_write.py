"""Amazon OpenSearch Write Module (PRIVATE)."""

import json
import logging
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional, Union, Tuple, Iterable

import boto3
import pandas as pd

from elasticsearch import Elasticsearch

_logger: logging.Logger = logging.getLogger(__name__)


def create_index(
    index: str,
    doc_type: Optional[str] = None,
    settings: Optional[Dict[str, Any]] = None,
    mappings: Optional[Dict[str, Any]] = None,
    boto3_session: Optional[boto3.Session] = None,
    con: Optional[Elasticsearch] = None
) -> Dict[str, Any]:
    """Creates an index.

    Parameters
    ----------
    index : str
        Name of the index.
    doc_type : str
        Name of the document type (for Elasticsearch versions 5.x and earlier).
    settings : Dict[str, Any], optional
        Index settings
        https://opensearch.org/docs/opensearch/rest-api/create-index/#index-settings
    mappings : Dict[str, Any], optional
        Index mappings
        https://opensearch.org/docs/opensearch/rest-api/create-index/#mappings
    boto3_session : boto3.Session(), optional
        Boto3 Session. The default boto3 Session will be used if boto3_session receive None.
    con : elasticsearch.Elasticsearch, optional
        Elasticsearch client. A new connection will be established if con receive None.

    Returns
    -------
    Dict[str, Any]
        OpenSearch rest api response
        https://opensearch.org/docs/opensearch/rest-api/create-index/#response.

    Examples
    --------
    Creating an index.

    >>> import awswrangler as wr
    >>> response = wr.opensearch.create_index(
    ...     index="sample-index1",
    ...     mappings={
    ...        "properties": {
    ...          "age":  { "type" : "integer" }
    ...        }
    ...     },
    ...     settings={
    ...         "index": {
    ...             "number_of_shards": 2,
    ...             "number_of_replicas": 1
    ...          }
    ...     }
    ... )

    """


def index_json(
    path: Union[str, Path],
    index: str,
    doc_type: Optional[str] = None,
    bulk_params: Optional[Union[List[Any], Tuple[Any], Dict[Any, Any]]] = None,
    boto3_session: Optional[boto3.Session] = None,
    **kwargs
) -> Dict[str, Any]:
    """Index all documents from JSON file to OpenSearch index.

    The JSON file should be in a JSON-Lines text format (newline-delimited JSON) - https://jsonlines.org/.

    Parameters
    ----------
    path : Union[str, Path]
        Path as str or Path object to the JSON file which contains the documents.
    index : str
        Name of the index.
    doc_type : str
        Name of the document type (only for Elasticsearch versions 5.x and earlier).
    bulk_params :  Union[List, Tuple, Dict], optional
        List of parameters to pass to bulk operation.
        References:
        elasticsearch >= 7.10.2 / opensearch: https://opensearch.org/docs/opensearch/rest-api/document-apis/bulk/#url-parameters
        elasticsearch < 7.10.2: https://opendistro.github.io/for-elasticsearch-docs/docs/elasticsearch/rest-api-reference/#url-parameters
    boto3_session : boto3.Session(), optional
        Boto3 Session. The default boto3 Session will be used if boto3_session receive None.
    **kwargs :
        KEYWORD arguments forwarded to :func:`~awswrangler.opensearch.index_documents`
        which is used to execute the operation

    Returns
    -------
    Dict[str, Any]
        Response payload
        https://opensearch.org/docs/opensearch/rest-api/document-apis/bulk/#response.

    Examples
    --------
    Writing contents of JSON file

    >>> import awswrangler as wr
    >>> wr.opensearch.index_json(
    ...     path='docs.json',
    ...     index='sample-index1'
    ... )
    """
    # Loading data from file

    pass  # TODO: load data from json file


def index_csv(
    path: Union[str, Path],
    index: str,
    doc_type: Optional[str] = None,
    boto3_session: Optional[boto3.Session] = None,
    pandas_params: Optional[Dict[str, Any]] = None
) -> Dict[str, Any]:
    """Index all documents from a CSV file to OpenSearch index.

    Parameters
    ----------
    path : Union[str, Path]
        Path as str or Path object to the CSV file which contains the documents.
    index : str
        Name of the index.
    doc_type : str
        Name of the document type (only for Elasticsearch versions 5.x and older).
    boto3_session : boto3.Session(), optional
        Boto3 Session. The default boto3 Session will be used if boto3_session receive None.
    pandas_params :
        Dictionary of arguments forwarded to pandas.read_csv().
        e.g. pandas_kwargs={'sep': '|', 'na_values': ['null', 'none'], 'skip_blank_lines': True}
        https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.read_csv.html

    Returns
    -------
    Dict[str, Any]
        Response payload
        https://opensearch.org/docs/opensearch/rest-api/document-apis/bulk/#response.

    Examples
    --------
    Writing contents of CSV file

    >>> import awswrangler as wr
    >>> wr.opensearch.index_csv(
    ...     path='docs.csv',
    ...     index='sample-index1'
    ... )

    Writing contents of CSV file using pandas_kwargs

    >>> import awswrangler as wr
    >>> wr.opensearch.index_csv(
    ...     path='docs.csv',
    ...     index='sample-index1',
    ...     pandas_params={'sep': '|', 'na_values': ['null', 'none'], 'skip_blank_lines': True}
    ... )
    """
    pass  # TODO: load data from csv file


def index_df(
    df: pd.DataFrame,
    index: str,
    doc_type: Optional[str] = None,
    boto3_session: Optional[boto3.Session] = None,
) -> Dict[str, Any]:
    """Index all documents from a DataFrame to OpenSearch index.

    Parameters
    ----------
    df : pd.DataFrame
        Pandas DataFrame https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html
    index : str
        Name of the index.
    doc_type : str
        Name of the document type (only for Elasticsearch versions 5.x and older).
    boto3_session : boto3.Session(), optional
        Boto3 Session. The default boto3 Session will be used if boto3_session receive None.

    Returns
    -------
    Dict[str, Any]
        Response payload
        https://opensearch.org/docs/opensearch/rest-api/document-apis/bulk/#response.

    Examples
    --------
    Writing rows of DataFrame

    >>> import awswrangler as wr
    >>> import pandas as pd
    >>> wr.opensearch.index_df(
    ...     df=pd.DataFrame([{'_id': '1'}, {'_id': '2'}, {'_id': '3'}]),
    ...     index='sample-index1'
    ... )
    """
    pass  # TODO: load data from dataframe


def index_documents(
    documents: Union[Iterable[Dict[str, Any]], Iterable[Mapping[str, Any]]],
    index: str,
    doc_type: Optional[str] = None,
    boto3_session: Optional[boto3.Session] = None,
    con: Optional[Elasticsearch] = None,
    ignore_status: Optional[Union[List[Any], Tuple[Any]]] = None,
    chunk_size: Optional[int] = 500,
    max_chunk_bytes: Optional[int] = 100 * 1024 * 1024,
    max_retries: Optional[int] = 0,
    initial_backoff: Optional[int] = 2,
    max_backoff: Optional[int] = 600,
    **kwargs

) -> Dict[str, Any]:
    """Index all documents to OpenSearch index.

    Note
    ----
    Some of the args are referenced from elasticsearch-py client library (bulk helpers)
    https://elasticsearch-py.readthedocs.io/en/v7.13.4/helpers.html#elasticsearch.helpers.bulk
    https://elasticsearch-py.readthedocs.io/en/v7.13.4/helpers.html#elasticsearch.helpers.streaming_bulk

    Parameters
    ----------
    documents : Union[Iterable[Dict[str, Any]], Iterable[Mapping[str, Any]]]
        List which contains the documents that will be inserted.
    index : str
        Name of the index.
    doc_type : str
        Name of the document type (only for Elasticsearch versions 5.x and older).
    boto3_session : boto3.Session(), optional
        Boto3 Session. The default boto3 Session will be used if boto3_session receive None.
    con : elasticsearch.Elasticsearch, optional
        Elasticsearch client. A new connection will be established if con receive None.
    ignore_status:  Union[List[Any], Tuple[Any]], optional
        list of HTTP status codes that you want to ignore (not raising an exception)
    chunk_size : int, optional
        number of docs in one chunk sent to es (default: 500)
    max_chunk_bytes: int, optional
        the maximum size of the request in bytes (default: 100MB)
    max_retries : int, optional
        maximum number of times a document will be retried when
        ``429`` is received, set to 0 (default) for no retries on ``429`` (default: 0)
    initial_backoff : int, optional
        number of seconds we should wait before the first retry.
        Any subsequent retries will be powers of ``initial_backoff*2**retry_number`` (default: 2)
    max_backoff: int, optional
        maximum number of seconds a retry will wait (default: 600)
    **kwargs :
        KEYWORD arguments forwarded to bulk operation
        elasticsearch >= 7.10.2 / opensearch: https://opensearch.org/docs/opensearch/rest-api/document-apis/bulk/#url-parameters
        elasticsearch < 7.10.2: https://opendistro.github.io/for-elasticsearch-docs/docs/elasticsearch/rest-api-reference/#url-parameters

    Returns
    -------
    Dict[str, Any]
        Response payload
        https://opensearch.org/docs/opensearch/rest-api/document-apis/bulk/#response.

    Examples
    --------
    Writing documents

    >>> import awswrangler as wr
    >>> wr.opensearch.index_documents(
    ...     documents=[{'_id': '1', 'value': 'foo'}, {'_id': '2', 'value': 'bar'}],
    ...     index='sample-index1'
    ... )
    """
    pass  # TODO: load documents
