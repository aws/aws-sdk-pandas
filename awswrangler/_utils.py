"""Internal (private) Utilities Module."""

import copy
import itertools
import logging
import math
import os
import random
import time
from concurrent.futures import FIRST_COMPLETED, Future, wait
from typing import Any, Callable, Dict, Generator, List, Optional, Sequence, Tuple, Union, cast

import boto3
import botocore.config
import numpy as np
import pandas as pd
import psycopg2

from awswrangler import exceptions

_logger: logging.Logger = logging.getLogger(__name__)

Boto3PrimitivesType = Dict[str, Optional[str]]


def ensure_session(session: Union[None, boto3.Session, Boto3PrimitivesType] = None) -> boto3.Session:
    """Ensure that a valid boto3.Session will be returned."""
    if isinstance(session, dict):  # Primitives received
        return boto3_from_primitives(primitives=session)
    if session is not None:
        return session
    # Ensure the boto3's default session is used so that its parameters can be
    # set via boto3.setup_default_session()
    if boto3.DEFAULT_SESSION is not None:
        return boto3.DEFAULT_SESSION
    return boto3.Session()


def boto3_to_primitives(boto3_session: Optional[boto3.Session] = None) -> Boto3PrimitivesType:
    """Convert Boto3 Session to Python primitives."""
    _boto3_session: boto3.Session = ensure_session(session=boto3_session)
    credentials = _boto3_session.get_credentials()
    return {
        "aws_access_key_id": getattr(credentials, "access_key", None),
        "aws_secret_access_key": getattr(credentials, "secret_key", None),
        "aws_session_token": getattr(credentials, "token", None),
        "region_name": _boto3_session.region_name,
        "profile_name": _boto3_session.profile_name,
    }


def boto3_from_primitives(primitives: Optional[Boto3PrimitivesType] = None) -> boto3.Session:
    """Convert Python primitives to Boto3 Session."""
    if primitives is None:
        return ensure_session()
    _primitives: Boto3PrimitivesType = copy.deepcopy(primitives)
    profile_name: Optional[str] = _primitives.get("profile_name", None)
    _primitives["profile_name"] = None if profile_name in (None, "default") else profile_name
    args: Dict[str, str] = {k: v for k, v in _primitives.items() if v is not None}
    return boto3.Session(**args)


def botocore_config() -> botocore.config.Config:
    """Botocore configuration."""
    return botocore.config.Config(retries={"max_attempts": 5}, connect_timeout=10, max_pool_connections=10)


def client(service_name: str, session: Optional[boto3.Session] = None) -> boto3.client:
    """Create a valid boto3.client."""
    return ensure_session(session=session).client(service_name=service_name, use_ssl=True, config=botocore_config())


def resource(service_name: str, session: Optional[boto3.Session] = None) -> boto3.resource:
    """Create a valid boto3.resource."""
    return ensure_session(session=session).resource(service_name=service_name, use_ssl=True, config=botocore_config())


def parse_path(path: str) -> Tuple[str, str]:
    """Split a full S3 path in bucket and key strings.

    's3://bucket/key' -> ('bucket', 'key')

    Parameters
    ----------
    path : str
        S3 path (e.g. s3://bucket/key).

    Returns
    -------
    Tuple[str, str]
        Tuple of bucket and key strings

    Examples
    --------
    >>> from awswrangler._utils import parse_path
    >>> bucket, key = parse_path('s3://bucket/key')

    """
    if path.startswith("s3://") is False:
        raise exceptions.InvalidArgumentValue(f"'{path}' is not a valid path. It MUST start with 's3://'")
    parts = path.replace("s3://", "").split("/", 1)
    bucket: str = parts[0]
    key: str = ""
    if len(parts) == 2:
        key = key if parts[1] is None else parts[1]
    return bucket, key


def ensure_cpu_count(use_threads: bool = True) -> int:
    """Get the number of cpu cores to be used.

    Note
    ----
    In case of `use_threads=True` the number of threads that could be spawned will be get from os.cpu_count().

    Parameters
    ----------
    use_threads : bool
            True to enable multi-core utilization, False to disable.

    Returns
    -------
    int
        Number of cpu cores to be used.

    Examples
    --------
    >>> from awswrangler._utils import ensure_cpu_count
    >>> ensure_cpu_count(use_threads=True)
    4
    >>> ensure_cpu_count(use_threads=False)
    1

    """
    cpus: int = 1
    if use_threads is True:
        cpu_cnt: Optional[int] = os.cpu_count()
        if cpu_cnt is not None:
            cpus = cpu_cnt if cpu_cnt > cpus else cpus
    return cpus


def chunkify(lst: List[Any], num_chunks: int = 1, max_length: Optional[int] = None) -> List[List[Any]]:
    """Split a list in a List of List (chunks) with even sizes.

    Parameters
    ----------
    lst: List
        List of anything to be splitted.
    num_chunks: int, optional
        Maximum number of chunks.
    max_length: int, optional
        Max length of each chunk. Has priority over num_chunks.

    Returns
    -------
    List[List[Any]]
        List of List (chunks) with even sizes.

    Examples
    --------
    >>> from awswrangler._utils import chunkify
    >>> chunkify(list(range(13)), num_chunks=3)
    [[0, 1, 2, 3, 4], [5, 6, 7, 8], [9, 10, 11, 12]]
    >>> chunkify(list(range(13)), max_length=4)
    [[0, 1, 2, 3], [4, 5, 6], [7, 8, 9], [10, 11, 12]]

    """
    if not lst:
        return []
    n: int = num_chunks if max_length is None else int(math.ceil((float(len(lst)) / float(max_length))))
    np_chunks = np.array_split(lst, n)
    return [arr.tolist() for arr in np_chunks if len(arr) > 0]


def empty_generator() -> Generator[None, None, None]:
    """Empty Generator."""
    yield from ()


def ensure_postgresql_casts() -> None:
    """Ensure that psycopg2 will handle some data types right."""
    psycopg2.extensions.register_adapter(bytes, psycopg2.Binary)
    typecast_bytea = lambda data, cur: None if data is None else bytes(psycopg2.BINARY(data, cur))  # noqa
    BYTEA = psycopg2.extensions.new_type(psycopg2.BINARY.values, "BYTEA", typecast_bytea)
    psycopg2.extensions.register_type(BYTEA)


def get_directory(path: str) -> str:
    """Extract directory path."""
    return path.rsplit(sep="/", maxsplit=1)[0] + "/"


def get_region_from_subnet(subnet_id: str, boto3_session: Optional[boto3.Session] = None) -> str:
    """Extract region from Subnet ID."""
    session: boto3.Session = ensure_session(session=boto3_session)
    client_ec2: boto3.client = client(service_name="ec2", session=session)
    return cast(str, client_ec2.describe_subnets(SubnetIds=[subnet_id])["Subnets"][0]["AvailabilityZone"][:-1])


def get_region_from_session(boto3_session: Optional[boto3.Session] = None, default_region: Optional[str] = None) -> str:
    """Extract region from session."""
    session: boto3.Session = ensure_session(session=boto3_session)
    region: Optional[str] = session.region_name
    if region is not None:
        return region
    if default_region is not None:
        return default_region
    raise exceptions.InvalidArgument("There is no region_name defined on boto3, please configure it.")


def list_sampling(lst: List[Any], sampling: float) -> List[Any]:
    """Random List sampling."""
    if sampling > 1.0 or sampling <= 0.0:
        raise exceptions.InvalidArgumentValue(f"Argument <sampling> must be [0.0 < value <= 1.0]. {sampling} received.")
    _len: int = len(lst)
    if _len == 0:
        return []
    num_samples: int = int(round(_len * sampling))
    num_samples = _len if num_samples > _len else num_samples
    num_samples = 1 if num_samples < 1 else num_samples
    _logger.debug("_len: %s", _len)
    _logger.debug("sampling: %s", sampling)
    _logger.debug("num_samples: %s", num_samples)
    return random.sample(population=lst, k=num_samples)


def ensure_df_is_mutable(df: pd.DataFrame) -> pd.DataFrame:
    """Ensure that all columns has the writeable flag True."""
    for column in df.columns.to_list():
        if hasattr(df[column].values, "flags") is True:
            if df[column].values.flags.writeable is False:
                s: pd.Series = df[column]
                df[column] = None
                df[column] = s
    return df


def insert_str(text: str, token: str, insert: str) -> str:
    """Insert string into other."""
    index: int = text.find(token)
    return text[:index] + insert + text[index:]


def check_duplicated_columns(df: pd.DataFrame) -> Any:
    """Raise an exception if there are duplicated columns names."""
    duplicated: List[str] = df.loc[:, df.columns.duplicated()].columns.to_list()
    if duplicated:
        raise exceptions.InvalidDataFrame(
            f"There are duplicated column names in your DataFrame: {duplicated}. "
            f"Note that your columns may have been sanitized and it can be the cause of "
            f"the duplicity. Wrangler sanitization removes all special characters and "
            f"also converts CamelCase to snake_case. So you must avoid columns like "
            f"['MyCol', 'my_col'] in your DataFrame."
        )


def try_it(f: Callable[..., Any], ex: Any, base: float = 1.0, max_num_tries: int = 3, **kwargs: Any) -> Any:
    """Run function with decorrelated Jitter.

    Reference: https://aws.amazon.com/blogs/architecture/exponential-backoff-and-jitter/
    """
    delay: float = base
    for i in range(max_num_tries):
        try:
            return f(**kwargs)
        except ex as exception:
            if i == (max_num_tries - 1):
                raise exception
            delay = random.uniform(base, delay * 3)
            _logger.error("Retrying %s | Fail number %s/%s | Exception: %s", f, i + 1, max_num_tries, exception)
            time.sleep(delay)


def get_even_chunks_sizes(total_size: int, chunk_size: int, upper_bound: bool) -> Tuple[int, ...]:
    """Calculate even chunks sizes (Best effort)."""
    round_func: Callable[[float], float] = math.ceil if upper_bound is True else math.floor
    num_chunks: int = int(round_func(float(total_size) / float(chunk_size)))
    num_chunks = 1 if num_chunks < 1 else num_chunks
    base_size: int = int(total_size / num_chunks)
    rest: int = total_size % num_chunks
    sizes: List[int] = list(itertools.repeat(base_size, num_chunks))
    for i in range(rest):
        i_cycled: int = i % len(sizes)
        sizes[i_cycled] += 1
    return tuple(sizes)


def get_running_futures(seq: Sequence[Future]) -> Tuple[Future, ...]:  # type: ignore
    """Filter only running futures."""
    return tuple(f for f in seq if f.running())


def wait_any_future_available(seq: Sequence[Future]) -> None:  # type: ignore
    """Wait until any future became available."""
    wait(fs=seq, timeout=None, return_when=FIRST_COMPLETED)


def block_waiting_available_thread(seq: Sequence[Future], max_workers: int) -> None:  # type: ignore
    """Block until any thread became available."""
    running: Tuple[Future, ...] = get_running_futures(seq=seq)  # type: ignore
    while len(running) >= max_workers:
        wait_any_future_available(seq=running)
        running = get_running_futures(seq=running)
