"""Amazon OpenSearch Write Module (PRIVATE)."""

import ast
import json
import logging
import uuid
from typing import Any, Dict, Generator, Iterable, List, Mapping, Optional, Tuple, Union

import boto3
import numpy as np
import pandas as pd
import progressbar
from jsonpath_ng import parse
from jsonpath_ng.exceptions import JsonPathParserError
from opensearchpy import OpenSearch, TransportError
from opensearchpy.exceptions import NotFoundError
from opensearchpy.helpers import bulk
from pandas import notna

from awswrangler._utils import parse_path
from awswrangler.opensearch._utils import _get_distribution, _get_version_major

_logger: logging.Logger = logging.getLogger(__name__)

_DEFAULT_REFRESH_INTERVAL = "1s"


def _selected_keys(document: Mapping[str, Any], keys_to_write: Optional[List[str]]) -> Mapping[str, Any]:
    if keys_to_write is None:
        keys_to_write = list(document.keys())
    keys_to_write = list(filter(lambda x: x != "_id", keys_to_write))
    return {key: document[key] for key in keys_to_write}


def _actions_generator(
    documents: Union[Iterable[Dict[str, Any]], Iterable[Mapping[str, Any]]],
    index: str,
    doc_type: Optional[str],
    keys_to_write: Optional[List[str]],
    id_keys: Optional[List[str]],
    bulk_size: int = 10000,
) -> Generator[List[Dict[str, Any]], None, None]:
    bulk_chunk_documents = []
    for i, document in enumerate(documents):
        if id_keys:
            _id = "-".join([str(document[id_key]) for id_key in id_keys])
        else:
            _id = document.get("_id", uuid.uuid4())
        bulk_chunk_documents.append(
            {
                "_index": index,
                "_type": doc_type,
                "_id": _id,
                "_source": _selected_keys(document, keys_to_write),
            }
        )
        if (i + 1) % bulk_size == 0:
            yield bulk_chunk_documents
            bulk_chunk_documents = []
    if len(bulk_chunk_documents) > 0:
        yield bulk_chunk_documents


def _df_doc_generator(df: pd.DataFrame) -> Generator[Dict[str, Any], None, None]:
    def _deserialize(v: Any) -> Any:
        if isinstance(v, str):
            v = v.strip()
            if v.startswith("{") and v.endswith("}") or v.startswith("[") and v.endswith("]"):
                try:
                    v = json.loads(v)
                except json.decoder.JSONDecodeError:
                    try:
                        v = ast.literal_eval(v)  # if properties are enclosed with single quotes
                        if not isinstance(v, dict):
                            _logger.warning("could not convert string to json: %s", v)
                    except SyntaxError as e:
                        _logger.warning("could not convert string to json: %s", v)
                        _logger.warning(e)
        return v

    df_iter = df.iterrows()
    for _, document in df_iter:
        yield {k: _deserialize(v) for k, v in document.items() if np.array(notna(v)).any()}


def _file_line_generator(path: str, is_json: bool = False) -> Generator[Any, None, None]:
    with open(path) as fp:  # pylint: disable=W1514
        for line in fp:
            if is_json:
                yield json.loads(line)
            else:
                yield line.strip()


def _get_documents_w_json_path(documents: List[Mapping[str, Any]], json_path: str) -> List[Any]:
    try:
        jsonpath_expression = parse(json_path)
    except JsonPathParserError as e:
        _logger.error("invalid json_path: %s", json_path)
        raise e
    output_documents = []
    for doc in documents:
        for match in jsonpath_expression.find(doc):
            match_value = match.value
            if isinstance(match_value, list):
                output_documents += match_value
            elif isinstance(match_value, dict):
                output_documents.append(match_value)
            else:
                msg = f"expected json_path value to be a list/dict. received type {type(match_value)} ({match_value})"
                raise ValueError(msg)
    return output_documents


def _get_refresh_interval(client: OpenSearch, index: str) -> Any:
    url = f"/{index}/_settings"
    try:
        response = client.transport.perform_request("GET", url)
        index_settings = response.get(index, {}).get("index", {})  # type: ignore
        refresh_interval = index_settings.get("refresh_interval", _DEFAULT_REFRESH_INTERVAL)
        return refresh_interval
    except NotFoundError:
        return None


def _set_refresh_interval(client: OpenSearch, index: str, refresh_interval: Optional[Any]) -> Any:
    url = f"/{index}/_settings"
    body = {"index": {"refresh_interval": refresh_interval}}
    response = client.transport.perform_request("PUT", url, headers={"Content-Type": "application/json"}, body=body)

    return response


def _disable_refresh_interval(
    client: OpenSearch,
    index: str,
) -> Any:
    return _set_refresh_interval(client=client, index=index, refresh_interval="-1")


def create_index(
    client: OpenSearch,
    index: str,
    doc_type: Optional[str] = None,
    settings: Optional[Dict[str, Any]] = None,
    mappings: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Create an index.

    Parameters
    ----------
    client : OpenSearch
        instance of opensearchpy.OpenSearch to use.
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
        if _get_distribution(client) == "opensearch" or _get_version_major(client) >= 7:
            body["mappings"] = mappings  # doc type deprecated
        else:
            if doc_type:
                body["mappings"] = {doc_type: mappings}
            else:
                body["mappings"] = {index: mappings}
    if settings:
        body["settings"] = settings
    if not body:
        body = None  # type: ignore

    # ignore 400 cause by IndexAlreadyExistsException when creating an index
    response: Dict[str, Any] = client.indices.create(index, body=body, ignore=400)
    if "error" in response:
        _logger.warning(response)
        if str(response["error"]).startswith("MapperParsingException"):
            raise ValueError(response["error"])
    return response


def delete_index(client: OpenSearch, index: str) -> Dict[str, Any]:
    """Delete an index.

    Parameters
    ----------
    client : OpenSearch
        instance of opensearchpy.OpenSearch to use.
    index : str
        Name of the index.

    Returns
    -------
    Dict[str, Any]
        OpenSearch rest api response

    Examples
    --------
    Deleting an index.

    >>> import awswrangler as wr
    >>> client = wr.opensearch.connect(host='DOMAIN-ENDPOINT')
    >>> response = wr.opensearch.delete_index(
    ...     client=client,
    ...     index="sample-index1"
    ... )

    """
    # ignore 400/404 IndexNotFoundError exception
    response: Dict[str, Any] = client.indices.delete(index, ignore=[400, 404])
    if "error" in response:
        _logger.warning(response)
    return response


def index_json(
    client: OpenSearch,
    path: str,
    index: str,
    doc_type: Optional[str] = None,
    boto3_session: Optional[boto3.Session] = boto3.Session(),
    json_path: Optional[str] = None,
    **kwargs: Any,
) -> Dict[str, Any]:
    """Index all documents from JSON file to OpenSearch index.

    The JSON file should be in a JSON-Lines text format (newline-delimited JSON) - https://jsonlines.org/
    OR if the is a single large JSON please provide `json_path`.

    Parameters
    ----------
    client : OpenSearch
        instance of opensearchpy.OpenSearch to use.
    path : str
        s3 or local path to the JSON file which contains the documents.
    index : str
        Name of the index.
    doc_type : str, optional
        Name of the document type (for Elasticsearch versions 5.x and earlier).
    json_path : str, optional
        JsonPath expression to specify explicit path to a single name element
        in a JSON hierarchical data structure.
        Read more about `JsonPath <https://jsonpath.com>`_
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
    _logger.debug("indexing %s from %s", index, path)

    if boto3_session is None:
        raise ValueError("boto3_session cannot be None")

    if path.startswith("s3://"):
        bucket, key = parse_path(path)
        s3 = boto3_session.client("s3")
        obj = s3.get_object(Bucket=bucket, Key=key)
        body = obj["Body"].read()
        lines = body.splitlines()
        documents = [json.loads(line) for line in lines]
        if json_path:
            documents = _get_documents_w_json_path(documents, json_path)
    else:  # local path
        documents = list(_file_line_generator(path, is_json=True))
        if json_path:
            documents = _get_documents_w_json_path(documents, json_path)
    return index_documents(client=client, documents=documents, index=index, doc_type=doc_type, **kwargs)


def index_csv(
    client: OpenSearch,
    path: str,
    index: str,
    doc_type: Optional[str] = None,
    pandas_kwargs: Optional[Dict[str, Any]] = None,
    **kwargs: Any,
) -> Dict[str, Any]:
    """Index all documents from a CSV file to OpenSearch index.

    Parameters
    ----------
    client : OpenSearch
        instance of opensearchpy.OpenSearch to use.
    path : str
        s3 or local path to the CSV file which contains the documents.
    index : str
        Name of the index.
    doc_type : str, optional
        Name of the document type (for Elasticsearch versions 5.x and earlier).
    pandas_kwargs : Dict[str, Any], optional
        Dictionary of arguments forwarded to pandas.read_csv().
        e.g. pandas_kwargs={'sep': '|', 'na_values': ['null', 'none']}
        https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.read_csv.html
        Note: these params values are enforced: `skip_blank_lines=True`
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
    ...     pandas_kwargs={'sep': '|', 'na_values': ['null', 'none']}
    ... )
    """
    _logger.debug("indexing %s from %s", index, path)
    if pandas_kwargs is None:
        pandas_kwargs = {}
    enforced_pandas_params = {
        "skip_blank_lines": True,
        # 'na_filter': True  # will generate Nan value for empty cells. We remove Nan keys in _df_doc_generator
        # Note: if the user will pass na_filter=False null fields will be indexed as well ({"k1": null, "k2": null})
    }
    pandas_kwargs.update(enforced_pandas_params)
    df = pd.read_csv(path, **pandas_kwargs)
    return index_df(client, df=df, index=index, doc_type=doc_type, **kwargs)


def index_df(
    client: OpenSearch, df: pd.DataFrame, index: str, doc_type: Optional[str] = None, **kwargs: Any
) -> Dict[str, Any]:
    """Index all documents from a DataFrame to OpenSearch index.

    Parameters
    ----------
    client : OpenSearch
        instance of opensearchpy.OpenSearch to use.
    df : pd.DataFrame
        Pandas DataFrame https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html
    index : str
        Name of the index.
    doc_type : str, optional
        Name of the document type (for Elasticsearch versions 5.x and earlier).
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
    return index_documents(client=client, documents=_df_doc_generator(df), index=index, doc_type=doc_type, **kwargs)


def index_documents(
    client: OpenSearch,
    documents: Iterable[Mapping[str, Any]],
    index: str,
    doc_type: Optional[str] = None,
    keys_to_write: Optional[List[str]] = None,
    id_keys: Optional[List[str]] = None,
    ignore_status: Optional[Union[List[Any], Tuple[Any]]] = None,
    bulk_size: int = 1000,
    chunk_size: Optional[int] = 500,
    max_chunk_bytes: Optional[int] = 100 * 1024 * 1024,
    max_retries: Optional[int] = 5,
    initial_backoff: Optional[int] = 2,
    max_backoff: Optional[int] = 600,
    **kwargs: Any,
) -> Dict[str, Any]:
    """Index all documents to OpenSearch index.

    Note
    ----
    Some of the args are referenced from opensearch-py client library (bulk helpers)
    https://opensearch-py.readthedocs.io/en/latest/helpers.html#opensearchpy.helpers.bulk
    https://opensearch-py.readthedocs.io/en/latest/helpers.html#opensearchpy.helpers.streaming_bulk

    If you receive `Error 429 (Too Many Requests) /_bulk` please to to decrease `bulk_size` value.
    Please also consider modifying the cluster size and instance type -
    Read more here: https://aws.amazon.com/premiumsupport/knowledge-center/resolve-429-error-es/

    Parameters
    ----------
    client : OpenSearch
        instance of opensearchpy.OpenSearch to use.
    documents : Iterable[Mapping[str, Any]]
        List which contains the documents that will be inserted.
    index : str
        Name of the index.
    doc_type : str, optional
        Name of the document type (for Elasticsearch versions 5.x and earlier).
    keys_to_write : List[str], optional
        list of keys to index. If not provided all keys will be indexed
    id_keys : List[str], optional
        list of keys that compound document unique id. If not provided will use `_id` key if exists,
        otherwise will generate unique identifier for each document.
    ignore_status:  Union[List[Any], Tuple[Any]], optional
        list of HTTP status codes that you want to ignore (not raising an exception)
    bulk_size: int,
        number of docs in each _bulk request (default: 1000)
    chunk_size : int, optional
        number of docs in one chunk sent to es (default: 500)
    max_chunk_bytes: int, optional
        the maximum size of the request in bytes (default: 100MB)
    max_retries : int, optional
        maximum number of times a document will be retried when
        ``429`` is received, set to 0 (default) for no retries on ``429`` (default: 2)
    initial_backoff : int, optional
        number of seconds we should wait before the first retry.
        Any subsequent retries will be powers of ``initial_backoff*2**retry_number`` (default: 2)
    max_backoff: int, optional
        maximum number of seconds a retry will wait (default: 600)
    **kwargs :
        KEYWORD arguments forwarded to bulk operation
        elasticsearch >= 7.10.2 / opensearch: \
https://opensearch.org/docs/opensearch/rest-api/document-apis/bulk/#url-parameters
        elasticsearch < 7.10.2: \
https://opendistro.github.io/for-elasticsearch-docs/docs/elasticsearch/rest-api-reference/#url-parameters

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
    if not isinstance(documents, list):
        documents = list(documents)
    total_documents = len(documents)
    _logger.debug("indexing %s documents into %s", total_documents, index)

    actions = _actions_generator(
        documents, index, doc_type, keys_to_write=keys_to_write, id_keys=id_keys, bulk_size=bulk_size
    )

    success = 0
    errors: List[Any] = []
    refresh_interval = None
    try:
        widgets = [
            progressbar.Percentage(),
            progressbar.SimpleProgress(format=" (%(value_s)s/%(max_value_s)s)"),
            progressbar.Bar(),
            progressbar.Timer(),
        ]
        progress_bar = progressbar.ProgressBar(widgets=widgets, max_value=total_documents, prefix="Indexing: ").start()
        for i, bulk_chunk_documents in enumerate(actions):
            if i == 1:  # second bulk iteration, in case the index didn't exist before
                refresh_interval = _get_refresh_interval(client, index)
                _disable_refresh_interval(client, index)
            _logger.debug("running bulk index of %s documents", len(bulk_chunk_documents))
            _success, _errors = bulk(
                client=client,
                actions=bulk_chunk_documents,
                ignore_status=ignore_status,
                chunk_size=chunk_size,
                max_chunk_bytes=max_chunk_bytes,
                max_retries=max_retries,
                initial_backoff=initial_backoff,
                max_backoff=max_backoff,
                request_timeout=30,
                **kwargs,
            )
            success += _success
            errors += _errors  # type: ignore
            _logger.debug("indexed %s documents (%s/%s)", _success, success, total_documents)
            progress_bar.update(success, force=True)
    except TransportError as e:
        if str(e.status_code) == "429":  # Too Many Requests
            _logger.error(
                "Error 429 (Too Many Requests):"
                "Try to tune bulk_size parameter."
                "Read more here: https://aws.amazon.com/premiumsupport/knowledge-center/resolve-429-error-es"
            )
            raise e

    finally:
        _set_refresh_interval(client, index, refresh_interval)

    return {"success": success, "errors": errors}
