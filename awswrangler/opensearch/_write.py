"""Amazon OpenSearch Write Module (PRIVATE)."""

import logging
import uuid
import boto3
import json
import ast
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional, Union, Tuple, Iterable
from awswrangler.opensearch._utils import _get_distribution, _get_version_major
from awswrangler._utils import parse_path
import pandas as pd
from pandas import notna

from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk

_logger: logging.Logger = logging.getLogger(__name__)


def _selected_keys(document: Dict, keys_to_write: Optional[List[str]]):
    if keys_to_write is None:
        keys_to_write = document.keys()
    keys_to_write = filter(lambda x: x != '_id', keys_to_write)
    return {key: document[key] for key in keys_to_write }


def _actions_generator(documents: Union[Iterable[Dict[str, Any]], Iterable[Mapping[str, Any]]],
                   index: str,
                   doc_type: Optional[str],
                   keys_to_write: Optional[List[str]],
                   id_keys: Optional[List[str]]):
    for document in documents:
        if id_keys:
            _id = '-'.join(list(map(lambda x: str(document[x]), id_keys)))
        else:
            _id = document.get('_id', uuid.uuid4())
        yield {
                "_index": index,
                "_type": doc_type,
                "_id" : _id,
                "_source": _selected_keys(document, keys_to_write),
            }


def _df_doc_generator(df: pd.DataFrame):
    def _deserialize(v):
        if isinstance(v, str):
            v = v.strip()
            if (v.startswith('{') and v.endswith('}')
                    or
                    v.startswith('[') and v.endswith(']')
            ):
                try:
                    v = json.loads(v)
                except Exception as e:
                    try:
                        v = ast.literal_eval(v)  # if properties are enclosed with single quotes
                    except:
                        _logger.warning(f'could not convert string to json: {v}')
                        _logger.warning(e)
        return v

    df_iter = df.iterrows()
    for i, document in df_iter:
        # print(document)
        # yield document
        yield {k: _deserialize(v) for k, v in document.items() if notna(v)}


def _file_line_generator(path: str, is_json: bool = False):
    with open(path) as fp:
        for line in fp:
            if is_json:
                yield json.loads(line)
            else:
                yield line.strip()


def create_index(
    client: Elasticsearch,
    index: str,
    doc_type: Optional[str] = None,
    settings: Optional[Dict[str, Any]] = None,
    mappings: Optional[Dict[str, Any]] = None
) -> Dict[str, Any]:
    """Creates an index.

    Parameters
    ----------
    client : Elasticsearch
        instance of elasticsearch.Elasticsearch to use.
    index : str
        Name of the index.
    doc_type : str, optional
        Name of the document type (for Elasticsearch versions 5.x and earlier).
    settings : Dict[str, Any], optional
        Index settings
        https://opensearch.org/docs/opensearch/rest-api/create-index/#index-settings
    mappings : Dict[str, Any], optional
        Index mappings
        https://opensearch.org/docs/opensearch/rest-api/create-index/#mappings

    Returns
    -------
    Dict[str, Any]
        OpenSearch rest api response
        https://opensearch.org/docs/opensearch/rest-api/create-index/#response.

    Examples
    --------
    Creating an index.

    >>> import awswrangler as wr
    >>> client = wr.opensearch.connect(host='DOMAIN-ENDPOINT')
    >>> response = wr.opensearch.create_index(
    ...     client=client,
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

    body = {}
    if mappings:
        if _get_distribution(client) == 'opensearch' or _get_version_major(client) >= 7:
            body['mappings'] = mappings  # doc type deprecated
        else:
            if doc_type:
                body['mappings'] = {doc_type: mappings}
            else:
                body['mappings'] = {index: mappings}
    if settings:
        body['settings'] = settings
    if body == {}:
        body = None
    response = client.indices.create(index, body, ignore=[400, 404])
    if 'error' in response:
        _logger.warning(response)
        if str(response['error']).startswith(u'MapperParsingException'):
            raise ValueError(response['error'])
    return response


def delete_index(
    client: Elasticsearch,
    index: str
) -> Dict[str, Any]:
    """Creates an index.

    Parameters
    ----------
    client : Elasticsearch
        instance of elasticsearch.Elasticsearch to use.
    index : str
        Name of the index.

    Returns
    -------
    Dict[str, Any]
        OpenSearch rest api response

    Examples
    --------
    Creating an index.

    >>> import awswrangler as wr
    >>> client = wr.opensearch.connect(host='DOMAIN-ENDPOINT')
    >>> response = wr.opensearch.delete_index(
    ...     client=client,
    ...     index="sample-index1"
    ... )

    """
    response = client.indices.delete(index, ignore=[400, 404])
    if 'error' in response:
        _logger.warning(response)
    return response


def index_json(
    client: Elasticsearch,
    path: Union[str, Path],
    index: str,
    doc_type: Optional[str] = None,
    boto3_session: Optional[boto3.Session] = boto3.Session(),
    **kwargs
) -> Dict[str, Any]:
    """Index all documents from JSON file to OpenSearch index.

    The JSON file should be in a JSON-Lines text format (newline-delimited JSON) - https://jsonlines.org/.

    Parameters
    ----------
    client : Elasticsearch
        instance of elasticsearch.Elasticsearch to use.
    path : Union[str, Path]
        s3 or local path to the JSON file which contains the documents.
    index : str
        Name of the index.
    doc_type : str, optional
        Name of the document type (only for Elasticsearch versions 5.x and earlier).
    boto3_session : boto3.Session(), optional
        Boto3 Session to be used to access s3 if s3 path is provided.
        The default boto3 Session will be used if boto3_session receive None.
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
    >>> client = wr.opensearch.connect(host='DOMAIN-ENDPOINT')
    >>> wr.opensearch.index_json(
    ...     client=client,
    ...     path='docs.json',
    ...     index='sample-index1'
    ... )
    """
    # Loading data from file

    if path.startswith("s3://"):
        bucket, key = parse_path(path)
        s3 = boto3_session.client('s3')
        obj = s3.get_object(Bucket=bucket, Key=key)
        body = obj['Body'].read()
        lines = body.splitlines()
        documents = map(lambda x: json.loads(x), lines)
    else: # local path
        documents = _file_line_generator(path, is_json=True)
    return index_documents(
        client=client,
        documents=documents,
        index=index,
        doc_type=doc_type,
        **kwargs
    )


def index_csv(
    client: Elasticsearch,
    path: Union[str, Path],
    index: str,
    doc_type: Optional[str] = None,
    pandas_kwargs: Optional[Dict[str, Any]] = {},
    **kwargs
) -> Dict[str, Any]:
    """Index all documents from a CSV file to OpenSearch index.

    Parameters
    ----------
    client : Elasticsearch
        instance of elasticsearch.Elasticsearch to use.
    path : Union[str, Path]
        s3 or local path to the CSV file which contains the documents.
    index : str
        Name of the index.
    doc_type : str, optional
        Name of the document type (only for Elasticsearch versions 5.x and older).
    pandas_kwargs :
        Dictionary of arguments forwarded to pandas.read_csv().
        e.g. pandas_kwargs={'sep': '|', 'na_values': ['null', 'none'], 'skip_blank_lines': True}
        https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.read_csv.html
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
    Writing contents of CSV file

    >>> import awswrangler as wr
    >>> client = wr.opensearch.connect(host='DOMAIN-ENDPOINT')
    >>> wr.opensearch.index_csv(
    ...     client=client,
    ...     path='docs.csv',
    ...     index='sample-index1'
    ... )

    Writing contents of CSV file using pandas_kwargs

    >>> import awswrangler as wr
    >>> client = wr.opensearch.connect(host='DOMAIN-ENDPOINT')
    >>> wr.opensearch.index_csv(
    ...     client=client,
    ...     path='docs.csv',
    ...     index='sample-index1',
    ...     pandas_kwargs={'sep': '|', 'na_values': ['null', 'none'], 'skip_blank_lines': True}
    ... )
    """
    custom_pandas_params = {
        'skip_blank_lines': True,
        'na_filter': True # will generate Nan value for empty cells. We remove Nan keys in _df_doc_generator
    }
    pandas_kwargs.update(custom_pandas_params)
    df = pd.read_csv(path, **pandas_kwargs)
    return index_df(
        client,
        df=df,
        index=index,
        doc_type=doc_type,
        **kwargs
    )


def index_df(
    client: Elasticsearch,
    df: pd.DataFrame,
    index: str,
    doc_type: Optional[str] = None,
    **kwargs
) -> Dict[str, Any]:
    """Index all documents from a DataFrame to OpenSearch index.

    Parameters
    ----------
    client : Elasticsearch
        instance of elasticsearch.Elasticsearch to use.
    df : pd.DataFrame
        Pandas DataFrame https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html
    index : str
        Name of the index.
    doc_type : str, optional
        Name of the document type (only for Elasticsearch versions 5.x and older).
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
    Writing rows of DataFrame

    >>> import awswrangler as wr
    >>> import pandas as pd
    >>> client = wr.opensearch.connect(host='DOMAIN-ENDPOINT')
    >>> wr.opensearch.index_df(
    ...     client=client,
    ...     df=pd.DataFrame([{'_id': '1'}, {'_id': '2'}, {'_id': '3'}]),
    ...     index='sample-index1'
    ... )
    """

    return index_documents(
        client=client,
        documents=_df_doc_generator(df),
        index=index,
        doc_type=doc_type,
        **kwargs
    )


def index_documents(
    client: Elasticsearch,
    documents: Union[Iterable[Dict[str, Any]], Iterable[Mapping[str, Any]]],
    index: str,
    doc_type: Optional[str] = None,
    keys_to_write: Optional[List[str]] = None,
    id_keys: Optional[List[str]] = None,
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
    client : Elasticsearch
        instance of elasticsearch.Elasticsearch to use.
    documents : Union[Iterable[Dict[str, Any]], Iterable[Mapping[str, Any]]]
        List which contains the documents that will be inserted.
    index : str
        Name of the index.
    doc_type : str, optional
        Name of the document type (only for Elasticsearch versions 5.x and older).
    keys_to_write : List[str], optional
        list of keys to index. If not provided all keys will be indexed
    id_keys : List[str], optional
        list of keys that compound document unique id. If not provided will use `_id` key if exists,
        otherwise will generate unique identifier for each document.
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
    >>> client = wr.opensearch.connect(host='DOMAIN-ENDPOINT')
    >>> wr.opensearch.index_documents(
    ...     documents=[{'_id': '1', 'value': 'foo'}, {'_id': '2', 'value': 'bar'}],
    ...     index='sample-index1'
    ... )
    """
    success, errors = bulk(
        client=client,
        actions=_actions_generator(documents, index, doc_type, keys_to_write=keys_to_write, id_keys=id_keys),
        ignore_status=ignore_status,
        chunk_size=chunk_size,
        max_chunk_bytes=max_chunk_bytes,
        max_retries=max_retries,
        initial_backoff=initial_backoff,
        max_backoff=max_backoff,
        **kwargs
    )
    return {
        'success': success,
        'errors': errors
    }
