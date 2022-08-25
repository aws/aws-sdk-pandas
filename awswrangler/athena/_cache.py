"""Cache Module for Amazon Athena."""
import datetime
import logging
import re
from heapq import heappop, heappush
from typing import Any, Dict, List, Match, NamedTuple, Optional, Tuple, Union

import boto3

from awswrangler import _utils

_logger: logging.Logger = logging.getLogger(__name__)


class _CacheInfo(NamedTuple):
    has_valid_cache: bool
    file_format: Optional[str] = None
    query_execution_id: Optional[str] = None
    query_execution_payload: Optional[Dict[str, Any]] = None


class _LocalMetadataCacheManager:
    def __init__(self) -> None:
        self._cache: Dict[str, Any] = {}
        self._pqueue: List[Tuple[datetime.datetime, str]] = []
        self._max_cache_size = 100

    def update_cache(self, items: List[Dict[str, Any]]) -> None:
        """
        Update the local metadata cache with new query metadata.

        Parameters
        ----------
        items : List[Dict[str, Any]]
            List of query execution metadata which is returned by boto3 `batch_get_query_execution()`.

        Returns
        -------
        None
            None.
        """
        if self._pqueue:
            oldest_item = self._cache[self._pqueue[0][1]]
            items = list(
                filter(lambda x: x["Status"]["SubmissionDateTime"] > oldest_item["Status"]["SubmissionDateTime"], items)
            )

        cache_oversize = len(self._cache) + len(items) - self._max_cache_size
        for _ in range(cache_oversize):
            _, query_execution_id = heappop(self._pqueue)
            del self._cache[query_execution_id]

        for item in items[: self._max_cache_size]:
            heappush(self._pqueue, (item["Status"]["SubmissionDateTime"], item["QueryExecutionId"]))
            self._cache[item["QueryExecutionId"]] = item

    def sorted_successful_generator(self) -> List[Dict[str, Any]]:
        """
        Sorts the entries in the local cache based on query Completion DateTime.

        This is useful to guarantee LRU caching rules.

        Returns
        -------
        List[Dict[str, Any]]
            Returns successful DDL and DML queries sorted by query completion time.
        """
        filtered: List[Dict[str, Any]] = []
        for query in self._cache.values():
            if (query["Status"].get("State") == "SUCCEEDED") and (query.get("StatementType") in ["DDL", "DML"]):
                filtered.append(query)
        return sorted(filtered, key=lambda e: str(e["Status"]["CompletionDateTime"]), reverse=True)

    def __contains__(self, key: str) -> bool:
        return key in self._cache

    @property
    def max_cache_size(self) -> int:
        """Property max_cache_size."""
        return self._max_cache_size

    @max_cache_size.setter
    def max_cache_size(self, value: int) -> None:
        self._max_cache_size = value


def _parse_select_query_from_possible_ctas(possible_ctas: str) -> Optional[str]:
    """Check if `possible_ctas` is a valid parquet-generating CTAS and returns the full SELECT statement."""
    possible_ctas = possible_ctas.lower()
    parquet_format_regex: str = r"format\s*=\s*\'parquet\'\s*"
    is_parquet_format: Optional[Match[str]] = re.search(pattern=parquet_format_regex, string=possible_ctas)
    if is_parquet_format is not None:
        unstripped_select_statement_regex: str = r"\s+as\s+\(*(select|with).*"
        unstripped_select_statement_match: Optional[Match[str]] = re.search(
            unstripped_select_statement_regex, possible_ctas, re.DOTALL
        )
        if unstripped_select_statement_match is not None:
            stripped_select_statement_match: Optional[Match[str]] = re.search(
                r"(select|with).*", unstripped_select_statement_match.group(0), re.DOTALL
            )
            if stripped_select_statement_match is not None:
                return stripped_select_statement_match.group(0)
    return None


def _compare_query_string(sql: str, other: str) -> bool:
    comparison_query = _prepare_query_string_for_comparison(query_string=other)
    _logger.debug("sql: %s", sql)
    _logger.debug("comparison_query: %s", comparison_query)
    if sql == comparison_query:
        return True
    return False


def _prepare_query_string_for_comparison(query_string: str) -> str:
    """To use cached data, we need to compare queries. Returns a query string in canonical form."""
    # for now this is a simple complete strip, but it could grow into much more sophisticated
    # query comparison data structures
    query_string = "".join(query_string.split()).strip().lower()
    while query_string.startswith("(") and query_string.endswith(")"):
        query_string = query_string[1:-1]
    query_string = query_string[:-1] if query_string.endswith(";") else query_string
    return query_string


def _get_last_query_infos(
    max_remote_cache_entries: int,
    boto3_session: Optional[boto3.Session] = None,
    workgroup: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """Return an iterator of `query_execution_info`s run by the workgroup in Athena."""
    client_athena: boto3.client = _utils.client(service_name="athena", session=boto3_session)
    page_size = 50
    args: Dict[str, Union[str, Dict[str, int]]] = {
        "PaginationConfig": {"MaxItems": max_remote_cache_entries, "PageSize": page_size}
    }
    if workgroup is not None:
        args["WorkGroup"] = workgroup
    paginator = client_athena.get_paginator("list_query_executions")
    uncached_ids = []
    for page in paginator.paginate(**args):
        _logger.debug("paginating Athena's queries history...")
        query_execution_id_list: List[str] = page["QueryExecutionIds"]
        for query_execution_id in query_execution_id_list:
            if query_execution_id not in _cache_manager:
                uncached_ids.append(query_execution_id)
    if uncached_ids:
        new_execution_data = []
        for i in range(0, len(uncached_ids), page_size):
            new_execution_data.extend(
                client_athena.batch_get_query_execution(QueryExecutionIds=uncached_ids[i : i + page_size]).get(
                    "QueryExecutions"
                )
            )
        _cache_manager.update_cache(new_execution_data)
    return _cache_manager.sorted_successful_generator()


def _check_for_cached_results(
    sql: str,
    boto3_session: boto3.Session,
    workgroup: Optional[str],
    max_cache_seconds: int,
    max_cache_query_inspections: int,
    max_remote_cache_entries: int,
) -> _CacheInfo:
    """
    Check whether `sql` has been run before, within the `max_cache_seconds` window, by the `workgroup`.

    If so, returns a dict with Athena's `query_execution_info` and the data format.
    """
    if max_cache_seconds <= 0:
        return _CacheInfo(has_valid_cache=False)
    num_executions_inspected: int = 0
    comparable_sql: str = _prepare_query_string_for_comparison(sql)
    current_timestamp: datetime.datetime = datetime.datetime.now(datetime.timezone.utc)
    _logger.debug("current_timestamp: %s", current_timestamp)
    for query_info in _get_last_query_infos(
        max_remote_cache_entries=max_remote_cache_entries,
        boto3_session=boto3_session,
        workgroup=workgroup,
    ):
        query_execution_id: str = query_info["QueryExecutionId"]
        query_timestamp: datetime.datetime = query_info["Status"]["CompletionDateTime"]
        _logger.debug("query_timestamp: %s", query_timestamp)
        if (current_timestamp - query_timestamp).total_seconds() > max_cache_seconds:
            return _CacheInfo(
                has_valid_cache=False, query_execution_id=query_execution_id, query_execution_payload=query_info
            )
        statement_type: Optional[str] = query_info.get("StatementType")
        if statement_type == "DDL" and query_info["Query"].startswith("CREATE TABLE"):
            parsed_query: Optional[str] = _parse_select_query_from_possible_ctas(possible_ctas=query_info["Query"])
            if parsed_query is not None:
                if _compare_query_string(sql=comparable_sql, other=parsed_query):
                    return _CacheInfo(
                        has_valid_cache=True,
                        file_format="parquet",
                        query_execution_id=query_execution_id,
                        query_execution_payload=query_info,
                    )
        elif statement_type == "DML" and not query_info["Query"].startswith("INSERT"):
            if _compare_query_string(sql=comparable_sql, other=query_info["Query"]):
                return _CacheInfo(
                    has_valid_cache=True,
                    file_format="csv",
                    query_execution_id=query_execution_id,
                    query_execution_payload=query_info,
                )
        num_executions_inspected += 1
        _logger.debug("num_executions_inspected: %s", num_executions_inspected)
        if num_executions_inspected >= max_cache_query_inspections:
            return _CacheInfo(has_valid_cache=False)
    return _CacheInfo(has_valid_cache=False)


_cache_manager = _LocalMetadataCacheManager()
