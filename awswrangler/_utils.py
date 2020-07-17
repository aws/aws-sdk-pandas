"""Internal (private) Utilities Module."""

import copy
import logging
import math
import os
import random
from typing import Any, Dict, Generator, List, Optional, Tuple, Union

import boto3  # type: ignore
import botocore.config  # type: ignore
import numpy as np  # type: ignore
import pandas as pd  # type: ignore
import psycopg2  # type: ignore
import s3fs  # type: ignore

from awswrangler import exceptions

_logger: logging.Logger = logging.getLogger(__name__)


def ensure_session(session: Optional[Union[boto3.Session, Dict[str, Optional[str]]]] = None) -> boto3.Session:
    """Ensure that a valid boto3.Session will be returned."""
    if isinstance(session, dict):  # Primitives received
        return boto3_from_primitives(primitives=session)
    if session is not None:
        return session
    # Ensure the boto3's default session is used so that its parameters can be
    # set via boto3.setup_default_session()
    if boto3.DEFAULT_SESSION is not None:
        return boto3.DEFAULT_SESSION
    return boto3.Session()  # pragma: no cover


def boto3_to_primitives(boto3_session: Optional[boto3.Session] = None) -> Dict[str, Optional[str]]:
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


def boto3_from_primitives(primitives: Dict[str, Optional[str]] = None) -> boto3.Session:
    """Convert Python primitives to Boto3 Session."""
    if primitives is None:
        return boto3.DEFAULT_SESSION  # pragma: no cover
    _primitives: Dict[str, Optional[str]] = copy.deepcopy(primitives)
    profile_name: Optional[str] = _primitives.get("profile_name", None)
    _primitives["profile_name"] = None if profile_name in (None, "default") else profile_name
    args: Dict[str, str] = {k: v for k, v in _primitives.items() if v is not None}
    return boto3.Session(**args)


def client(service_name: str, session: Optional[boto3.Session] = None) -> boto3.client:
    """Create a valid boto3.client."""
    return ensure_session(session=session).client(
        service_name=service_name, use_ssl=True, config=botocore.config.Config(retries={"max_attempts": 15})
    )


def resource(service_name: str, session: Optional[boto3.Session] = None) -> boto3.resource:
    """Create a valid boto3.resource."""
    return ensure_session(session=session).resource(
        service_name=service_name, use_ssl=True, config=botocore.config.Config(retries={"max_attempts": 15})
    )


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
        return []  # pragma: no cover
    n: int = num_chunks if max_length is None else int(math.ceil((float(len(lst)) / float(max_length))))
    np_chunks = np.array_split(lst, n)
    return [arr.tolist() for arr in np_chunks if len(arr) > 0]


def get_fs(
    session: Optional[Union[boto3.Session, Dict[str, Optional[str]]]] = None,
    s3_additional_kwargs: Optional[Dict[str, str]] = None,
) -> s3fs.S3FileSystem:
    """Build a S3FileSystem from a given boto3 session."""
    fs: s3fs.S3FileSystem = s3fs.S3FileSystem(
        anon=False,
        use_ssl=True,
        default_cache_type="readahead",
        default_fill_cache=False,
        default_block_size=1_073_741_824,  # 1024 MB (1024 * 2**20)
        config_kwargs={"retries": {"max_attempts": 15}},
        session=ensure_session(session=session)._session,  # pylint: disable=protected-access
        s3_additional_kwargs=s3_additional_kwargs,
        use_listings_cache=False,
        skip_instance_cache=True,
    )
    fs.invalidate_cache()
    fs.clear_instance_cache()
    return fs


def empty_generator() -> Generator:
    """Empty Generator."""
    yield from ()


def ensure_postgresql_casts():
    """Ensure that psycopg2 will handle some data types right."""
    psycopg2.extensions.register_adapter(bytes, psycopg2.Binary)
    typecast_bytea = lambda data, cur: None if data is None else bytes(psycopg2.BINARY(data, cur))  # noqa
    BYTEA = psycopg2.extensions.new_type(psycopg2.BINARY.values, "BYTEA", typecast_bytea)
    psycopg2.extensions.register_type(BYTEA)


def get_directory(path: str) -> str:
    """Extract directory path."""
    return path.rsplit(sep="/", maxsplit=1)[0] + "/"


def get_region_from_subnet(subnet_id: str, boto3_session: Optional[boto3.Session] = None) -> str:  # pragma: no cover
    """Extract region from Subnet ID."""
    session: boto3.Session = ensure_session(session=boto3_session)
    client_ec2: boto3.client = client(service_name="ec2", session=session)
    return client_ec2.describe_subnets(SubnetIds=[subnet_id])["Subnets"][0]["AvailabilityZone"][:-1]


def get_region_from_session(boto3_session: Optional[boto3.Session] = None, default_region: Optional[str] = None) -> str:
    """Extract region from session."""
    session: boto3.Session = ensure_session(session=boto3_session)
    region: Optional[str] = session.region_name
    if region is not None:
        return region
    if default_region is not None:  # pragma: no cover
        return default_region
    raise exceptions.InvalidArgument(
        "There is no region_name defined on boto3, please configure it."
    )  # pragma: no cover


def extract_partitions_metadata_from_paths(
    path: str, paths: List[str]
) -> Tuple[Optional[Dict[str, str]], Optional[Dict[str, List[str]]]]:
    """Extract partitions metadata from Amazon S3 paths."""
    path = path if path.endswith("/") else f"{path}/"
    partitions_types: Dict[str, str] = {}
    partitions_values: Dict[str, List[str]] = {}
    for p in paths:
        if path not in p:
            raise exceptions.InvalidArgumentValue(
                f"Object {p} is not under the root path ({path})."
            )  # pragma: no cover
        path_wo_filename: str = p.rpartition("/")[0] + "/"
        if path_wo_filename not in partitions_values:
            path_wo_prefix: str = path_wo_filename.replace(f"{path}/", "")
            dirs: List[str] = [x for x in path_wo_prefix.split("/") if (x != "") and ("=" in x)]
            if dirs:
                values_tups: List[Tuple[str, str]] = [tuple(x.split("=")[:2]) for x in dirs]  # type: ignore
                values_dics: Dict[str, str] = dict(values_tups)
                p_values: List[str] = list(values_dics.values())
                p_types: Dict[str, str] = {x: "string" for x in values_dics.keys()}
                if not partitions_types:
                    partitions_types = p_types
                if p_values:
                    partitions_types = p_types
                    partitions_values[path_wo_filename] = p_values
                elif p_types != partitions_types:  # pragma: no cover
                    raise exceptions.InvalidSchemaConvergence(
                        f"At least two different partitions schema detected: {partitions_types} and {p_types}"
                    )
    if not partitions_types:
        return None, None
    return partitions_types, partitions_values


def extract_partitions_from_path(path_root: str, path: str) -> Dict[str, Any]:
    """Extract partitions values and names from Amazon S3 path."""
    path_root = path_root if path_root.endswith("/") else f"{path_root}/"
    if path_root not in path:
        raise exceptions.InvalidArgumentValue(
            f"Object {path} is not under the root path ({path_root})."
        )  # pragma: no cover
    path_wo_filename: str = path.rpartition("/")[0] + "/"
    path_wo_prefix: str = path_wo_filename.replace(f"{path_root}/", "")
    dirs: List[str] = [x for x in path_wo_prefix.split("/") if (x != "") and ("=" in x)]
    if not dirs:
        return {}  # pragma: no cover
    values_tups: List[Tuple[str, str]] = [tuple(x.split("=")[:2]) for x in dirs]  # type: ignore
    values_dics: Dict[str, str] = dict(values_tups)
    return values_dics


def list_sampling(lst: List[Any], sampling: float) -> List[Any]:
    """Random List sampling."""
    if sampling > 1.0 or sampling <= 0.0:  # pragma: no cover
        raise exceptions.InvalidArgumentValue(f"Argument <sampling> must be [0.0 < value <= 1.0]. {sampling} received.")
    _len: int = len(lst)
    if _len == 0:
        return []  # pragma: no cover
    num_samples: int = int(round(_len * sampling))
    num_samples = _len if num_samples > _len else num_samples
    num_samples = 1 if num_samples < 1 else num_samples
    _logger.debug("_len: %s", _len)
    _logger.debug("sampling: %s", sampling)
    _logger.debug("num_samples: %s", num_samples)
    return random.sample(population=lst, k=num_samples)


def ensure_df_is_mutable(df: pd.DataFrame) -> pd.DataFrame:
    """Ensure that all columns has the writeable flag True."""
    columns: List[str] = df.columns.to_list()
    for column in columns:
        if hasattr(df[column].values, "flags") is True:
            if df[column].values.flags.writeable is False:  # pragma: no cover
                df = df.copy(deep=True)
                break
    return df


def insert_str(text: str, token: str, insert: str) -> str:
    """Insert string into other."""
    index: int = text.find(token)
    return text[:index] + insert + text[index:]
