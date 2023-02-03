"""Internal (private) Utilities Module."""

import itertools
import logging
import math
import os
import random
import time
from concurrent.futures import FIRST_COMPLETED, Future, wait
from functools import partial, wraps
from typing import TYPE_CHECKING, Any, Callable, Dict, Generator, List, Optional, Sequence, Tuple, Type, Union, overload

import boto3
import botocore.credentials
import numpy as np
import pandas as pd
import pyarrow as pa
from botocore.config import Config

from awswrangler import _config, exceptions
from awswrangler.__metadata__ import __version__
from awswrangler._arrow import _table_to_df
from awswrangler._config import apply_configs
from awswrangler._distributed import engine

if TYPE_CHECKING:
    from boto3.resources.base import ServiceResource
    from botocore.client import BaseClient
    from mypy_boto3_athena import AthenaClient
    from mypy_boto3_athena.literals import ServiceName
    from mypy_boto3_dynamodb import DynamoDBClient, DynamoDBServiceResource
    from mypy_boto3_ec2 import EC2Client
    from mypy_boto3_emr.client import EMRClient
    from mypy_boto3_glue import GlueClient
    from mypy_boto3_kms.client import KMSClient
    from mypy_boto3_lakeformation.client import LakeFormationClient
    from mypy_boto3_logs.client import CloudWatchLogsClient
    from mypy_boto3_opensearch.client import OpenSearchServiceClient
    from mypy_boto3_opensearchserverless.client import OpenSearchServiceServerlessClient
    from mypy_boto3_quicksight.client import QuickSightClient
    from mypy_boto3_redshift.client import RedshiftClient
    from mypy_boto3_redshift_data.client import RedshiftDataAPIServiceClient
    from mypy_boto3_s3 import S3Client, S3ServiceResource
    from mypy_boto3_secretsmanager import SecretsManagerClient
    from mypy_boto3_sts.client import STSClient
    from mypy_boto3_timestream_query.client import TimestreamQueryClient
    from mypy_boto3_timestream_write.client import TimestreamWriteClient
    from typing_extensions import Literal

_logger: logging.Logger = logging.getLogger(__name__)

Boto3PrimitivesType = Dict[str, Optional[str]]


def flatten_list(elements: List[List[Any]]) -> List[Any]:
    """Flatten a list of lists."""
    return [item for sublist in elements for item in sublist]


def ensure_session(session: Union[None, boto3.Session] = None) -> boto3.Session:
    """Ensure that a valid boto3.Session will be returned."""
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


def default_botocore_config() -> botocore.config.Config:
    """Botocore configuration."""
    retries_config: Dict[str, Union[str, int]] = {
        "max_attempts": int(os.getenv("AWS_MAX_ATTEMPTS", "5")),
    }
    mode = os.getenv("AWS_RETRY_MODE")
    if mode:
        retries_config["mode"] = mode
    return Config(
        retries=retries_config,  # type: ignore[arg-type]
        connect_timeout=10,
        max_pool_connections=10,
        user_agent_extra=f"awswrangler/{__version__}",
    )


def _get_endpoint_url(service_name: str) -> Optional[str]:
    endpoint_url: Optional[str] = None
    if service_name == "s3" and _config.config.s3_endpoint_url is not None:
        endpoint_url = _config.config.s3_endpoint_url
    elif service_name == "athena" and _config.config.athena_endpoint_url is not None:
        endpoint_url = _config.config.athena_endpoint_url
    elif service_name == "sts" and _config.config.sts_endpoint_url is not None:
        endpoint_url = _config.config.sts_endpoint_url
    elif service_name == "glue" and _config.config.glue_endpoint_url is not None:
        endpoint_url = _config.config.glue_endpoint_url
    elif service_name == "redshift" and _config.config.redshift_endpoint_url is not None:
        endpoint_url = _config.config.redshift_endpoint_url
    elif service_name == "kms" and _config.config.kms_endpoint_url is not None:
        endpoint_url = _config.config.kms_endpoint_url
    elif service_name == "emr" and _config.config.emr_endpoint_url is not None:
        endpoint_url = _config.config.emr_endpoint_url
    elif service_name == "lakeformation" and _config.config.lakeformation_endpoint_url is not None:
        endpoint_url = _config.config.lakeformation_endpoint_url
    elif service_name == "dynamodb" and _config.config.dynamodb_endpoint_url is not None:
        endpoint_url = _config.config.dynamodb_endpoint_url
    elif service_name == "secretsmanager" and _config.config.secretsmanager_endpoint_url is not None:
        endpoint_url = _config.config.secretsmanager_endpoint_url
    elif service_name == "timestream-write" and _config.config.timestream_write_endpoint_url is not None:
        endpoint_url = _config.config.timestream_write_endpoint_url
    elif service_name == "timestream-query" and _config.config.timestream_query_endpoint_url is not None:
        endpoint_url = _config.config.timestream_query_endpoint_url

    return endpoint_url


@overload
def client(
    service_name: 'Literal["athena"]',
    session: Optional[boto3.Session] = None,
    botocore_config: Optional[Config] = None,
    verify: Optional[Union[str, bool]] = None,
) -> "AthenaClient":
    ...


@overload
def client(
    service_name: 'Literal["lakeformation"]',
    session: Optional[boto3.Session] = None,
    botocore_config: Optional[Config] = None,
    verify: Optional[Union[str, bool]] = None,
) -> "LakeFormationClient":
    ...


@overload
def client(
    service_name: 'Literal["logs"]',
    session: Optional[boto3.Session] = None,
    botocore_config: Optional[Config] = None,
    verify: Optional[Union[str, bool]] = None,
) -> "CloudWatchLogsClient":
    ...


@overload
def client(
    service_name: 'Literal["dynamodb"]',
    session: Optional[boto3.Session] = None,
    botocore_config: Optional[Config] = None,
    verify: Optional[Union[str, bool]] = None,
) -> "DynamoDBClient":
    ...


@overload
def client(
    service_name: 'Literal["ec2"]',
    session: Optional[boto3.Session] = None,
    botocore_config: Optional[Config] = None,
    verify: Optional[Union[str, bool]] = None,
) -> "EC2Client":
    ...


@overload
def client(
    service_name: 'Literal["emr"]',
    session: Optional[boto3.Session] = None,
    botocore_config: Optional[Config] = None,
    verify: Optional[Union[str, bool]] = None,
) -> "EMRClient":
    ...


@overload
def client(
    service_name: 'Literal["glue"]',
    session: Optional[boto3.Session] = None,
    botocore_config: Optional[Config] = None,
    verify: Optional[Union[str, bool]] = None,
) -> "GlueClient":
    ...


@overload
def client(
    service_name: 'Literal["kms"]',
    session: Optional[boto3.Session] = None,
    botocore_config: Optional[Config] = None,
    verify: Optional[Union[str, bool]] = None,
) -> "KMSClient":
    ...


@overload
def client(
    service_name: 'Literal["opensearch"]',
    session: Optional[boto3.Session] = None,
    botocore_config: Optional[Config] = None,
    verify: Optional[Union[str, bool]] = None,
) -> "OpenSearchServiceClient":
    ...


@overload
def client(
    service_name: 'Literal["opensearchserverless"]',
    session: Optional[boto3.Session] = None,
    botocore_config: Optional[Config] = None,
    verify: Optional[Union[str, bool]] = None,
) -> "OpenSearchServiceServerlessClient":
    ...


@overload
def client(
    service_name: 'Literal["quicksight"]',
    session: Optional[boto3.Session] = None,
    botocore_config: Optional[Config] = None,
    verify: Optional[Union[str, bool]] = None,
) -> "QuickSightClient":
    ...


@overload
def client(
    service_name: 'Literal["redshift"]',
    session: Optional[boto3.Session] = None,
    botocore_config: Optional[Config] = None,
    verify: Optional[Union[str, bool]] = None,
) -> "RedshiftClient":
    ...


@overload
def client(
    service_name: 'Literal["redshift-data"]',
    session: Optional[boto3.Session] = None,
    botocore_config: Optional[Config] = None,
    verify: Optional[Union[str, bool]] = None,
) -> "RedshiftDataAPIServiceClient":
    ...


@overload
def client(
    service_name: 'Literal["s3"]',
    session: Optional[boto3.Session] = None,
    botocore_config: Optional[Config] = None,
    verify: Optional[Union[str, bool]] = None,
) -> "S3Client":
    ...


@overload
def client(
    service_name: 'Literal["secretsmanager"]',
    session: Optional[boto3.Session] = None,
    botocore_config: Optional[Config] = None,
    verify: Optional[Union[str, bool]] = None,
) -> "SecretsManagerClient":
    ...


@overload
def client(
    service_name: 'Literal["sts"]',
    session: Optional[boto3.Session] = None,
    botocore_config: Optional[Config] = None,
    verify: Optional[Union[str, bool]] = None,
) -> "STSClient":
    ...


@overload
def client(
    service_name: 'Literal["timestream-query"]',
    session: Optional[boto3.Session] = None,
    botocore_config: Optional[Config] = None,
    verify: Optional[Union[str, bool]] = None,
) -> "TimestreamQueryClient":
    ...


@overload
def client(
    service_name: 'Literal["timestream-write"]',
    session: Optional[boto3.Session] = None,
    botocore_config: Optional[Config] = None,
    verify: Optional[Union[str, bool]] = None,
) -> "TimestreamWriteClient":
    ...


@overload
def client(
    service_name: "ServiceName",
    session: Optional[boto3.Session] = None,
    botocore_config: Optional[Config] = None,
    verify: Optional[Union[str, bool]] = None,
) -> "BaseClient":
    ...


@apply_configs
def client(
    service_name: "ServiceName",
    session: Optional[boto3.Session] = None,
    botocore_config: Optional[Config] = None,
    verify: Optional[Union[str, bool]] = None,
) -> "BaseClient":
    """Create a valid boto3.client."""
    endpoint_url: Optional[str] = _get_endpoint_url(service_name=service_name)
    return ensure_session(session=session).client(
        service_name=service_name,
        endpoint_url=endpoint_url,
        use_ssl=True,
        config=botocore_config or default_botocore_config(),
        verify=verify or _config.config.verify,
    )


@overload
def resource(
    service_name: 'Literal["dynamodb"]',
    session: Optional[boto3.Session] = None,
    botocore_config: Optional[Config] = None,
    verify: Optional[Union[str, bool]] = None,
) -> "DynamoDBServiceResource":
    ...


@overload
def resource(
    service_name: 'Literal["s3"]',
    session: Optional[boto3.Session] = None,
    botocore_config: Optional[Config] = None,
    verify: Optional[Union[str, bool]] = None,
) -> "S3ServiceResource":
    ...


@apply_configs
def resource(
    service_name: Union['Literal["dynamodb"]', 'Literal["s3"]'],
    session: Optional[boto3.Session] = None,
    botocore_config: Optional[Config] = None,
    verify: Optional[Union[str, bool]] = None,
) -> "ServiceResource":
    """Create a valid boto3.resource."""
    endpoint_url: Optional[str] = _get_endpoint_url(service_name=service_name)
    return ensure_session(session=session).resource(
        service_name=service_name,
        endpoint_url=endpoint_url,
        use_ssl=True,
        verify=verify,
        config=default_botocore_config() if botocore_config is None else botocore_config,
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

    >>> from awswrangler._utils import parse_path
    >>> bucket, key = parse_path('s3://arn:aws:s3:<awsregion>:<awsaccount>:accesspoint/<ap_name>/<key>')
    """
    if path.startswith("s3://") is False:
        raise exceptions.InvalidArgumentValue(f"'{path}' is not a valid path. It MUST start with 's3://'")
    parts = path.replace("s3://", "").replace(":accesspoint/", ":accesspoint:").split("/", 1)
    bucket: str = parts[0]
    if "/" in bucket:
        raise exceptions.InvalidArgumentValue(f"'{bucket}' is not a valid bucket name.")
    key: str = ""
    if len(parts) == 2:
        key = key if parts[1] is None else parts[1]
    return bucket, key


def ensure_cpu_count(use_threads: Union[bool, int] = True) -> int:
    """Get the number of cpu cores to be used.

    Note
    ----
    In case of `use_threads=True` the number of threads that could be spawned will be get from os.cpu_count().

    Parameters
    ----------
    use_threads : Union[bool, int]
            True to enable multi-core utilization, False to disable.
            If given an int will simply return the input value.

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
    if type(use_threads) == int:  # pylint: disable=unidiomatic-typecheck
        if use_threads < 1:
            return 1
        return use_threads
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


def get_directory(path: str) -> str:
    """Extract directory path."""
    return path.rsplit(sep="/", maxsplit=1)[0] + "/"


def get_region_from_subnet(subnet_id: str, boto3_session: Optional[boto3.Session] = None) -> str:
    """Extract region from Subnet ID."""
    session: boto3.Session = ensure_session(session=boto3_session)
    client_ec2 = client(service_name="ec2", session=session)
    return client_ec2.describe_subnets(SubnetIds=[subnet_id])["Subnets"][0]["AvailabilityZone"][:-1]


def get_region_from_session(boto3_session: Optional[boto3.Session] = None, default_region: Optional[str] = None) -> str:
    """Extract region from session."""
    session: boto3.Session = ensure_session(session=boto3_session)
    region: Optional[str] = session.region_name
    if region is not None:
        return region
    if default_region is not None:
        return default_region
    raise exceptions.InvalidArgument("There is no region_name defined on boto3, please configure it.")


def get_credentials_from_session(
    boto3_session: Optional[boto3.Session] = None,
) -> botocore.credentials.ReadOnlyCredentials:
    """Get AWS credentials from boto3 session."""
    session: boto3.Session = ensure_session(session=boto3_session)
    credentials: botocore.credentials.Credentials = session.get_credentials()
    frozen_credentials: botocore.credentials.ReadOnlyCredentials = credentials.get_frozen_credentials()
    return frozen_credentials


def list_sampling(lst: List[Any], sampling: float) -> List[Any]:
    """Random List sampling."""
    if sampling == 1.0:
        return lst
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
    random_lst: List[Any] = random.sample(population=lst, k=num_samples)
    random_lst.sort()
    return random_lst


def ensure_df_is_mutable(df: pd.DataFrame) -> pd.DataFrame:
    """Ensure that all columns has the writeable flag True."""
    for column in df.columns.to_list():
        if hasattr(df[column].values, "flags") is True:
            if df[column].values.flags.writeable is False:
                s: pd.Series = df[column]
                df[column] = None
                df[column] = s
    return df


def check_duplicated_columns(df: pd.DataFrame) -> Any:
    """Raise an exception if there are duplicated columns names."""
    duplicated: List[str] = df.loc[:, df.columns.duplicated()].columns.to_list()
    if duplicated:
        raise exceptions.InvalidDataFrame(
            f"There are duplicated column names in your DataFrame: {duplicated}. "
            f"Note that your columns may have been sanitized and it can be the cause of "
            f"the duplicity."
        )


def retry(
    ex: Type[Exception],
    ex_code: Optional[str] = None,
    base: float = 1.0,
    max_num_tries: int = 3,
) -> Callable[..., Any]:
    """
    Decorate function with decorrelated Jitter retries.

    Parameters
    ----------
    ex : Exception
        Exception to retry on
    ex_code : Optional[str]
        Response error code
    base : float
        Base delay
    max_num_tries : int
        Maximum number of retries

    Returns
    -------
    Callable[..., Any]
        Function
    """

    def wrapper(f: Callable[..., Any]) -> Any:
        return wraps(f)(partial(try_it, f, ex, ex_code=ex_code, base=base, max_num_tries=max_num_tries))

    return wrapper


def try_it(
    f: Callable[..., Any],
    ex: Any,
    *args: Any,
    ex_code: Optional[str] = None,
    base: float = 1.0,
    max_num_tries: int = 3,
    **kwargs: Any,
) -> Any:
    """Run function with decorrelated Jitter.

    Reference: https://aws.amazon.com/blogs/architecture/exponential-backoff-and-jitter/
    """
    delay: float = base
    for i in range(max_num_tries):
        try:
            return f(*args, **kwargs)
        except ex as exception:
            if ex_code is not None and hasattr(exception, "response"):
                if exception.response["Error"]["Code"] != ex_code:
                    raise
            if i == (max_num_tries - 1):
                raise
            delay = random.uniform(base, delay * 3)
            _logger.error("Retrying %s | Fail number %s/%s | Exception: %s", f, i + 1, max_num_tries, exception)
            time.sleep(delay)
    raise RuntimeError()


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


def check_schema_changes(columns_types: Dict[str, str], table_input: Optional[Dict[str, Any]], mode: str) -> None:
    """Check schema changes."""
    if (table_input is not None) and (mode in ("append", "overwrite_partitions")):
        catalog_cols: Dict[str, str] = {x["Name"]: x["Type"] for x in table_input["StorageDescriptor"]["Columns"]}
        for c, t in columns_types.items():
            if c not in catalog_cols:
                raise exceptions.InvalidArgumentValue(
                    f"Schema change detected: New column {c} with type {t}. "
                    "Please pass schema_evolution=True to allow new columns "
                    "behaviour."
                )
            if t != catalog_cols[c]:  # Data type change detected!
                raise exceptions.InvalidArgumentValue(
                    f"Schema change detected: Data type change on column {c} "
                    f"(Old type: {catalog_cols[c]} / New type {t})."
                )


@engine.dispatch_on_engine
def split_pandas_frame(df: pd.DataFrame, splits: int) -> List[pd.DataFrame]:
    """Split a DataFrame into n chunks."""
    return [sub_df for sub_df in np.array_split(df, splits) if not sub_df.empty]  # type: ignore


@engine.dispatch_on_engine
def table_refs_to_df(tables: List[pa.Table], kwargs: Dict[str, Any]) -> pd.DataFrame:
    """Build Pandas dataframe from list of PyArrow tables."""
    return _table_to_df(pa.concat_tables(tables, promote=True), kwargs=kwargs)


@engine.dispatch_on_engine
def is_pandas_frame(obj: Any) -> bool:
    """Check if the passed objected is a Pandas DataFrame."""
    return isinstance(obj, pd.DataFrame)


@engine.dispatch_on_engine
def copy_df_shallow(df: pd.DataFrame) -> pd.DataFrame:
    """Create a shallow copy of the Pandas DataFrame."""
    return df.copy(deep=False)


def list_to_arrow_table(
    mapping: List[Dict[str, Any]],
    schema: Optional[pa.Schema] = None,
    metadata: Optional[Dict[str, Any]] = None,
) -> pa.Table:
    """Construct a PyArrow Table from list of dictionaries."""
    arrays = []
    if not schema:
        names = []
        if mapping:
            names = list(mapping[0].keys())
        for n in names:
            v = [row[n] if n in row else None for row in mapping]
            arrays.append(v)
        return pa.Table.from_arrays(arrays, names, metadata=metadata)
    for n in schema.names:
        v = [row[n] if n in row else None for row in mapping]
        arrays.append(v)
    # Will raise if metadata is not None
    return pa.Table.from_arrays(arrays, schema=schema, metadata=metadata)
