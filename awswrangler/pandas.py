from typing import TYPE_CHECKING, Dict, List, Tuple, Optional, Any, Union, Iterator
from io import BytesIO, StringIO
import multiprocessing as mp
from logging import getLogger, Logger, INFO
from math import floor
import copy
import csv
from datetime import datetime, date
from decimal import Decimal
from ast import literal_eval

from botocore.exceptions import ClientError, HTTPClientError  # type: ignore
from boto3 import client  # type: ignore
import pandas as pd  # type: ignore
import pyarrow as pa  # type: ignore
from pyarrow import parquet as pq  # type: ignore
import tenacity  # type: ignore
from s3fs import S3FileSystem  # type: ignore

from awswrangler import data_types
from awswrangler.exceptions import (UnsupportedWriteMode, UnsupportedFileFormat, AthenaQueryError, EmptyS3Object,
                                    LineTerminatorNotFound, EmptyDataframe, InvalidSerDe, InvalidCompression,
                                    InvalidParameters, InvalidEngine)
from awswrangler.utils import calculate_bounders
from awswrangler.s3 import S3, get_fs, mkdir_if_not_exists
from awswrangler.athena import Athena
from awswrangler.aurora import Aurora

if TYPE_CHECKING:
    from awswrangler.session import Session, SessionPrimitives

logger: Logger = getLogger(__name__)

MIN_NUMBER_OF_ROWS_TO_DISTRIBUTE: int = 1000


def _get_bounders(dataframe: pd.DataFrame, num_partitions: int):
    num_rows: int = len(dataframe.index)
    return calculate_bounders(num_items=num_rows, num_groups=num_partitions)


class Pandas:
    VALID_CSV_SERDES: List[str] = ["OpenCSVSerDe", "LazySimpleSerDe"]
    VALID_CSV_COMPRESSIONS: List[Optional[str]] = [None]
    VALID_PARQUET_COMPRESSIONS: List[Optional[str]] = [None, "snappy", "gzip"]

    def __init__(self, session: "Session"):
        self._session: "Session" = session
        self._client_s3: client = session.boto3_session.client(service_name="s3",
                                                               use_ssl=True,
                                                               config=session.botocore_config)

    @staticmethod
    def _parse_path(path: str) -> Tuple[str, str]:
        path2: str = path.replace("s3://", "")
        parts: Tuple[str, str, str] = path2.partition("/")
        return parts[0], parts[2]

    def read_csv(
        self,
        path: str,
        max_result_size: Optional[int] = None,
        header="infer",
        names=None,
        usecols=None,
        dtype=None,
        sep=",",
        thousands=None,
        decimal=".",
        lineterminator="\n",
        quotechar='"',
        quoting=csv.QUOTE_MINIMAL,
        escapechar=None,
        parse_dates: Union[bool, Dict, List] = False,
        infer_datetime_format=False,
        encoding="utf-8",
        converters=None,
    ):
        """
        Read CSV file from AWS S3 using optimized strategies.
        Try to mimic as most as possible pandas.read_csv()
        https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.read_csv.html
        P.S. max_result_size != None tries to mimic the chunksize behaviour in pandas.read_sql()

        :param path: AWS S3 path (E.g. S3://BUCKET_NAME/KEY_NAME)
        :param max_result_size: Max number of bytes on each request to S3
        :param header: Same as pandas.read_csv()
        :param names: Same as pandas.read_csv()
        :param usecols: Same as pandas.read_csv()
        :param dtype: Same as pandas.read_csv()
        :param sep: Same as pandas.read_csv()
        :param thousands: Same as pandas.read_csv()
        :param decimal: Same as pandas.read_csv()
        :param lineterminator: Same as pandas.read_csv()
        :param quotechar: Same as pandas.read_csv()
        :param quoting: Same as pandas.read_csv()
        :param escapechar: Same as pandas.read_csv()
        :param parse_dates: Same as pandas.read_csv()
        :param infer_datetime_format: Same as pandas.read_csv()
        :param encoding: Same as pandas.read_csv()
        :param converters: Same as pandas.read_csv()
        :return: Pandas Dataframe or Iterator of Pandas Dataframes if max_result_size != None
        """
        bucket_name, key_path = self._parse_path(path)
        if max_result_size is not None:
            ret = self._read_csv_iterator(bucket_name=bucket_name,
                                          key_path=key_path,
                                          max_result_size=max_result_size,
                                          header=header,
                                          names=names,
                                          usecols=usecols,
                                          dtype=dtype,
                                          sep=sep,
                                          thousands=thousands,
                                          decimal=decimal,
                                          lineterminator=lineterminator,
                                          quotechar=quotechar,
                                          quoting=quoting,
                                          escapechar=escapechar,
                                          parse_dates=parse_dates,
                                          infer_datetime_format=infer_datetime_format,
                                          encoding=encoding,
                                          converters=converters)
        else:
            ret = self._read_csv_once(bucket_name=bucket_name,
                                      key_path=key_path,
                                      header=header,
                                      names=names,
                                      usecols=usecols,
                                      dtype=dtype,
                                      sep=sep,
                                      thousands=thousands,
                                      decimal=decimal,
                                      lineterminator=lineterminator,
                                      quotechar=quotechar,
                                      quoting=quoting,
                                      escapechar=escapechar,
                                      parse_dates=parse_dates,
                                      infer_datetime_format=infer_datetime_format,
                                      encoding=encoding,
                                      converters=converters)
        return ret

    def _read_csv_iterator(
        self,
        bucket_name,
        key_path,
        max_result_size=200_000_000,  # 200 MB
        header="infer",
        names=None,
        usecols=None,
        dtype=None,
        sep=",",
        thousands=None,
        decimal=".",
        lineterminator="\n",
        quotechar='"',
        quoting=csv.QUOTE_MINIMAL,
        escapechar=None,
        parse_dates: Union[bool, Dict, List] = False,
        infer_datetime_format=False,
        encoding="utf-8",
        converters=None,
    ):
        """
        Read CSV file from AWS S3 using optimized strategies.
        Try to mimic as most as possible pandas.read_csv()
        https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.read_csv.html

        :param bucket_name: S3 bucket name
        :param key_path: S3 key path (W/o bucket)
        :param max_result_size: Max number of bytes on each request to S3
        :param header: Same as pandas.read_csv()
        :param names: Same as pandas.read_csv()
        :param usecols: Same as pandas.read_csv()
        :param dtype: Same as pandas.read_csv()
        :param sep: Same as pandas.read_csv()
        :param thousands: Same as pandas.read_csv()
        :param decimal: Same as pandas.read_csv()
        :param lineterminator: Same as pandas.read_csv()
        :param quotechar: Same as pandas.read_csv()
        :param quoting: Same as pandas.read_csv()
        :param escapechar: Same as pandas.read_csv()
        :param parse_dates: Same as pandas.read_csv()
        :param infer_datetime_format: Same as pandas.read_csv()
        :param encoding: Same as pandas.read_csv()
        :param converters: Same as pandas.read_csv()
        :return: Pandas Dataframe
        """
        metadata = S3.head_object_with_retry(client_s3=self._client_s3, bucket=bucket_name, key=key_path)
        total_size = metadata["ContentLength"]
        logger.debug(f"total_size: {total_size}")
        if total_size <= 0:
            raise EmptyS3Object(metadata)
        elif total_size <= max_result_size:
            yield self._read_csv_once(bucket_name=bucket_name,
                                      key_path=key_path,
                                      header=header,
                                      names=names,
                                      usecols=usecols,
                                      dtype=dtype,
                                      sep=sep,
                                      thousands=thousands,
                                      decimal=decimal,
                                      lineterminator=lineterminator,
                                      quotechar=quotechar,
                                      quoting=quoting,
                                      escapechar=escapechar,
                                      parse_dates=parse_dates,
                                      infer_datetime_format=infer_datetime_format,
                                      encoding=encoding,
                                      converters=converters)
        else:
            bounders = calculate_bounders(num_items=total_size, max_size=max_result_size)
            logger.debug(f"bounders: {bounders}")
            bounders_len: int = len(bounders)
            count: int = 0
            forgotten_bytes: int = 0
            for ini, end in bounders:
                count += 1

                ini -= forgotten_bytes
                end -= 1  # Range is inclusive, contrary from Python's List
                bytes_range = "bytes={}-{}".format(ini, end)
                logger.debug(f"bytes_range: {bytes_range}")
                body = self._client_s3.get_object(Bucket=bucket_name, Key=key_path, Range=bytes_range)["Body"].read()
                chunk_size = len(body)
                logger.debug(f"chunk_size (bytes): {chunk_size}")

                if count == 1:  # first chunk
                    last_char = Pandas._find_terminator(body=body,
                                                        sep=sep,
                                                        quoting=quoting,
                                                        quotechar=quotechar,
                                                        lineterminator=lineterminator)
                    forgotten_bytes = len(body[last_char:])
                elif count == bounders_len:  # Last chunk
                    last_char = chunk_size
                else:
                    last_char = Pandas._find_terminator(body=body,
                                                        sep=sep,
                                                        quoting=quoting,
                                                        quotechar=quotechar,
                                                        lineterminator=lineterminator)
                    forgotten_bytes = len(body[last_char:])

                df = pd.read_csv(StringIO(body[:last_char].decode("utf-8")),
                                 header=header,
                                 names=names,
                                 usecols=usecols,
                                 sep=sep,
                                 thousands=thousands,
                                 decimal=decimal,
                                 quotechar=quotechar,
                                 quoting=quoting,
                                 escapechar=escapechar,
                                 parse_dates=parse_dates,
                                 infer_datetime_format=infer_datetime_format,
                                 lineterminator=lineterminator,
                                 dtype=dtype,
                                 encoding=encoding,
                                 converters=converters)
                yield df
                if count == 1:  # first chunk
                    names = df.columns
                    header = None

    @staticmethod
    def _extract_terminator_profile(body, sep, quotechar, lineterminator, last_index):
        """
        Backward parser for quoted CSV lines

        :param body: String
        :param sep: Same as pandas.read_csv()
        :param quotechar: Same as pandas.read_csv()
        :param lineterminator: Same as pandas.read_csv()
        :return: Dict with the profile
        """
        sep_int = int.from_bytes(bytes=sep.encode(encoding="utf-8"), byteorder="big")  # b"," -> 44
        quote_int = int.from_bytes(bytes=quotechar.encode(encoding="utf-8"), byteorder="big")  # b'"' -> 34
        terminator_int = int.from_bytes(bytes=lineterminator.encode(encoding="utf-8"), byteorder="big")  # b"\n" -> 10
        logger.debug(f"sep_int: {sep_int}")
        logger.debug(f"quote_int: {quote_int}")
        logger.debug(f"terminator_int: {terminator_int}")
        last_terminator_suspect_index = None
        first_non_special_byte_index = None
        sep_counter = 0
        quote_counter = 0
        for i in range((len(body[:last_index]) - 1), -1, -1):
            b = body[i]
            if last_terminator_suspect_index:
                if b == quote_int:
                    quote_counter += 1
                elif b == sep_int:
                    sep_counter += 1
                elif b == terminator_int:
                    pass
                else:
                    first_non_special_byte_index = i
                    break
            if b == terminator_int:
                if not last_terminator_suspect_index:
                    last_terminator_suspect_index = i
                elif last_terminator_suspect_index - 1 == i:
                    first_non_special_byte_index = i
                    break
        logger.debug(f"last_terminator_suspect_index: {last_terminator_suspect_index}")
        logger.debug(f"first_non_special_byte_index: {first_non_special_byte_index}")
        logger.debug(f"sep_counter: {sep_counter}")
        logger.debug(f"quote_counter: {quote_counter}")
        return {
            "last_terminator_suspect_index": last_terminator_suspect_index,
            "first_non_special_byte_index": first_non_special_byte_index,
            "sep_counter": sep_counter,
            "quote_counter": quote_counter
        }

    @staticmethod
    def _find_terminator(body, sep, quoting, quotechar, lineterminator):
        """
        Find for any suspicious of line terminator (From end to start)

        :param body: String
        :param sep: Same as pandas.read_csv()
        :param quoting: Same as pandas.read_csv()
        :param quotechar: Same as pandas.read_csv()
        :param lineterminator: Same as pandas.read_csv()
        :return: The index of the suspect line terminator
        """
        try:
            last_index = None
            if quoting == csv.QUOTE_ALL:
                while True:
                    profile = Pandas._extract_terminator_profile(body=body,
                                                                 sep=sep,
                                                                 quotechar=quotechar,
                                                                 lineterminator=lineterminator,
                                                                 last_index=last_index)
                    if profile["last_terminator_suspect_index"] and profile["first_non_special_byte_index"]:
                        if profile["quote_counter"] % 2 == 0 or profile["quote_counter"] == 0:
                            last_index = profile["last_terminator_suspect_index"]
                        else:
                            index = profile["last_terminator_suspect_index"]
                            break
                    else:
                        raise LineTerminatorNotFound()
            else:
                index = body.rindex(lineterminator.encode(encoding="utf-8"))
        except ValueError:
            raise LineTerminatorNotFound()
        return index

    def _read_csv_once(
        self,
        bucket_name,
        key_path,
        header="infer",
        names=None,
        usecols=None,
        dtype=None,
        sep=",",
        thousands=None,
        decimal=".",
        lineterminator="\n",
        quotechar='"',
        quoting=0,
        escapechar=None,
        parse_dates: Union[bool, Dict, List] = False,
        infer_datetime_format=False,
        encoding=None,
        converters=None,
    ):
        """
        Read CSV file from AWS S3 using optimized strategies.
        Try to mimic as most as possible pandas.read_csv()
        https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.read_csv.html

        :param bucket_name: S3 bucket name
        :param key_path: S3 key path (W/o bucket)
        :param header: Same as pandas.read_csv()
        :param names: Same as pandas.read_csv()
        :param usecols: Same as pandas.read_csv()
        :param dtype: Same as pandas.read_csv()
        :param sep: Same as pandas.read_csv()
        :param thousands: Same as pandas.read_csv()
        :param decimal: Same as pandas.read_csv()
        :param lineterminator: Same as pandas.read_csv()
        :param quotechar: Same as pandas.read_csv()
        :param quoting: Same as pandas.read_csv()
        :param escapechar: Same as pandas.read_csv()
        :param parse_dates: Same as pandas.read_csv()
        :param infer_datetime_format: Same as pandas.read_csv()
        :param encoding: Same as pandas.read_csv()
        :param converters: Same as pandas.read_csv()
        :return: Pandas Dataframe
        """
        buff = BytesIO()
        self._client_s3.download_fileobj(Bucket=bucket_name, Key=key_path, Fileobj=buff)
        buff.seek(0),
        dataframe = pd.read_csv(
            buff,
            header=header,
            names=names,
            usecols=usecols,
            sep=sep,
            thousands=thousands,
            decimal=decimal,
            quotechar=quotechar,
            quoting=quoting,
            escapechar=escapechar,
            parse_dates=parse_dates,
            infer_datetime_format=infer_datetime_format,
            lineterminator=lineterminator,
            dtype=dtype,
            encoding=encoding,
            converters=converters,
        )
        buff.close()
        return dataframe

    @staticmethod
    def _list_parser(value: str) -> List[Union[int, float, str, None]]:
        # try resolve with a simple literal_eval
        try:
            return literal_eval(value)
        except ValueError:
            pass  # keep trying

        # sanity check
        if len(value) <= 1:
            return []

        items: List[Union[None, str]] = [None if x == "null" else x for x in value[1:-1].split(", ")]
        array_type: Optional[type] = None

        # check if all values are integers
        for item in items:
            if item is not None:
                try:
                    int(item)  # type: ignore
                except ValueError:
                    break
        else:
            array_type = int

        # check if all values are floats
        if array_type is None:
            for item in items:
                if item is not None:
                    try:
                        float(item)  # type: ignore
                    except ValueError:
                        break
            else:
                array_type = float

        # check if all values are strings
        array_type = str if array_type is None else array_type

        return [array_type(x) if x is not None else None for x in items]

    def _get_query_dtype(self, query_execution_id: str) -> Tuple[Dict[str, str], List[str], List[str], Dict[str, Any]]:
        cols_metadata: Dict[str, str] = self._session.athena.get_query_columns_metadata(
            query_execution_id=query_execution_id)
        logger.debug(f"cols_metadata: {cols_metadata}")
        dtype: Dict[str, str] = {}
        parse_timestamps: List[str] = []
        parse_dates: List[str] = []
        converters: Dict[str, Any] = {}
        col_name: str
        col_type: str
        for col_name, col_type in cols_metadata.items():
            pandas_type: str = data_types.athena2pandas(dtype=col_type)
            if pandas_type in ["datetime64", "date"]:
                parse_timestamps.append(col_name)
                if pandas_type == "date":
                    parse_dates.append(col_name)
            elif pandas_type == "list":
                converters[col_name] = Pandas._list_parser
            elif pandas_type == "bool":
                logger.debug(f"Ignoring bool column: {col_name}")
            elif pandas_type == "decimal":
                converters[col_name] = lambda x: Decimal(str(x)) if str(x) != "" else None
            else:
                dtype[col_name] = pandas_type
        logger.debug(f"dtype: {dtype}")
        logger.debug(f"parse_timestamps: {parse_timestamps}")
        logger.debug(f"parse_dates: {parse_dates}")
        logger.debug(f"converters: {converters}")
        return dtype, parse_timestamps, parse_dates, converters

    def read_sql_athena(self,
                        sql: str,
                        database: Optional[str] = None,
                        s3_output: Optional[str] = None,
                        workgroup: Optional[str] = None,
                        encryption: Optional[str] = None,
                        kms_key: Optional[str] = None,
                        ctas_approach: bool = None,
                        procs_cpu_bound: Optional[int] = None,
                        max_result_size: Optional[int] = None):
        """
        Executes any SQL query on AWS Athena and return a Dataframe of the result.
        There are two approaches to be defined through ctas_approach parameter:

        1 - ctas_approach True (For Huge results):
        Wrap the query with a CTAS and then reads the table data as parquet directly from s3.
        PROS: Faster and has a better handle of nested types
        CONS: Can't use max_result_size and must have create and drop table permissions

        2 - ctas_approach False (Default):
        Does a regular query on Athena and parse the regular CSV result on s3
        PROS: Accepts max_result_size.
        CONS: Slower (But stills faster than other libraries that uses the Athena API) and does not handle nested types so well

        P.S. If ctas_approach is False and max_result_size is passed, then a iterator of Dataframes is returned.
        P.S.S. All default values will be inherited from the Session()

        :param sql: SQL Query
        :param database: Glue/Athena Database
        :param s3_output: AWS S3 path
        :param workgroup: The name of the workgroup in which the query is being started. (By default uses de Session() workgroup)
        :param encryption: None|'SSE_S3'|'SSE_KMS'|'CSE_KMS'
        :param kms_key: For SSE-KMS and CSE-KMS , this is the KMS key ARN or ID.
        :param ctas_approach: Wraps the query with a CTAS (Session's default is False)
        :param procs_cpu_bound: Number of cores used for CPU bound tasks
        :param max_result_size: Max number of bytes on each request to S3 (VALID ONLY FOR ctas_approach=False)
        :return: Pandas Dataframe or Iterator of Pandas Dataframes if max_result_size was passed
        """
        ctas_approach = ctas_approach if ctas_approach is not None else self._session.athena_ctas_approach if self._session.athena_ctas_approach is not None else False
        if ctas_approach is True and max_result_size is not None:
            raise InvalidParameters("ctas_approach can't use max_result_size!")
        if s3_output is None:
            if self._session.athena_s3_output is not None:
                s3_output = self._session.athena_s3_output
            else:
                s3_output = self._session.athena.create_athena_bucket()
        if ctas_approach is False:
            return self._read_sql_athena_regular(sql=sql,
                                                 database=database,
                                                 s3_output=s3_output,
                                                 workgroup=workgroup,
                                                 encryption=encryption,
                                                 kms_key=kms_key,
                                                 max_result_size=max_result_size)
        else:
            return self._read_sql_athena_ctas(sql=sql,
                                              database=database,
                                              s3_output=s3_output,
                                              workgroup=workgroup,
                                              encryption=encryption,
                                              kms_key=kms_key,
                                              procs_cpu_bound=procs_cpu_bound)

    def _read_sql_athena_ctas(self,
                              sql: str,
                              s3_output: str,
                              database: Optional[str] = None,
                              workgroup: Optional[str] = None,
                              encryption: Optional[str] = None,
                              kms_key: Optional[str] = None,
                              procs_cpu_bound: Optional[int] = None) -> pd.DataFrame:
        guid: str = pa.compat.guid()
        name: str = f"temp_table_{guid}"
        s3_output = s3_output[:-1] if s3_output[-1] == "/" else s3_output
        path: str = f"{s3_output}/{name}"
        query: str = f"CREATE TABLE {name}\n" \
                     f"WITH(\n" \
                     f"    format = 'Parquet',\n" \
                     f"    parquet_compression = 'SNAPPY',\n" \
                     f"    external_location = '{path}'\n" \
                     f") AS\n" \
                     f"{sql}"
        logger.debug(f"query: {query}")
        query_id: str = self._session.athena.run_query(query=query,
                                                       database=database,
                                                       s3_output=s3_output,
                                                       workgroup=workgroup,
                                                       encryption=encryption,
                                                       kms_key=kms_key)
        self._session.athena.wait_query(query_execution_id=query_id)
        self._session.glue.delete_table_if_exists(database=database, table=name)
        manifest_path: str = f"{s3_output}/tables/{query_id}-manifest.csv"
        paths: List[str] = self._session.athena.extract_manifest_paths(path=manifest_path)
        logger.debug(f"paths: {paths}")
        if not paths:
            df: pd.DataFrame = pd.DataFrame()
        else:
            df = self.read_parquet(path=paths,
                                   procs_cpu_bound=procs_cpu_bound,
                                   wait_objects=True,
                                   wait_objects_timeout=15.0)
        self._session.s3.delete_listed_objects(objects_paths=[manifest_path] + paths)
        return df

    def _read_sql_athena_regular(self,
                                 sql: str,
                                 s3_output: str,
                                 database: Optional[str] = None,
                                 workgroup: Optional[str] = None,
                                 encryption: Optional[str] = None,
                                 kms_key: Optional[str] = None,
                                 max_result_size: Optional[int] = None):
        query_execution_id: str = self._session.athena.run_query(query=sql,
                                                                 database=database,
                                                                 s3_output=s3_output,
                                                                 workgroup=workgroup,
                                                                 encryption=encryption,
                                                                 kms_key=kms_key)
        query_response: Dict = self._session.athena.wait_query(query_execution_id=query_execution_id)
        if query_response["QueryExecution"]["Status"]["State"] in ["FAILED", "CANCELLED"]:
            reason: str = query_response["QueryExecution"]["Status"]["StateChangeReason"]
            message_error: str = f"Query error: {reason}"
            raise AthenaQueryError(message_error)
        else:
            dtype, parse_timestamps, parse_dates, converters = self._get_query_dtype(
                query_execution_id=query_execution_id)
            path = f"{s3_output}{query_execution_id}.csv"
            ret = self.read_csv(path=path,
                                dtype=dtype,
                                parse_dates=parse_timestamps,
                                converters=converters,
                                quoting=csv.QUOTE_ALL,
                                max_result_size=max_result_size)
            if max_result_size is None:
                if len(ret.index) > 0:
                    for col in parse_dates:
                        if str(ret[col].dtype) == "object":
                            ret[col] = ret[col].apply(lambda x: date(*[int(y) for y in x.split("-")]))
                        else:
                            ret[col] = ret[col].dt.date.replace(to_replace={pd.NaT: None})
                return ret
            else:
                return Pandas._apply_dates_to_generator(generator=ret, parse_dates=parse_dates)

    @staticmethod
    def _apply_dates_to_generator(generator, parse_dates):
        for df in generator:
            if len(df.index) > 0:
                for col in parse_dates:
                    df[col] = df[col].dt.date.replace(to_replace={pd.NaT: None})
            yield df

    def to_csv(self,
               dataframe: pd.DataFrame,
               path: str,
               sep: str = ",",
               escapechar: Optional[str] = None,
               serde: str = "OpenCSVSerDe",
               database: Optional[str] = None,
               table: Optional[str] = None,
               partition_cols: Optional[List[str]] = None,
               preserve_index: bool = True,
               mode: str = "append",
               procs_cpu_bound: Optional[int] = None,
               procs_io_bound: Optional[int] = None,
               inplace=True,
               description: Optional[str] = None,
               parameters: Optional[Dict[str, str]] = None,
               columns_comments: Optional[Dict[str, str]] = None):
        """
        Write a Pandas Dataframe as CSV files on S3
        Optionally writes metadata on AWS Glue.

        :param dataframe: Pandas Dataframe
        :param path: AWS S3 path (E.g. s3://bucket-name/folder_name/
        :param sep: Same as pandas.to_csv()
        :param escapechar: Same as pandas.to_csv()
        :param serde: SerDe library name (e.g. OpenCSVSerDe, LazySimpleSerDe)
        :param database: AWS Glue Database name
        :param table: AWS Glue table name
        :param partition_cols: List of columns names that will be partitions on S3
        :param preserve_index: Should preserve index on S3?
        :param mode: "append", "overwrite", "overwrite_partitions"
        :param procs_cpu_bound: Number of cores used for CPU bound tasks
        :param procs_io_bound: Number of cores used for I/O bound tasks
        :param inplace: True is cheapest (CPU and Memory) but False leaves your DataFrame intact
        :param description: Table description
        :param parameters: Key/value pairs to tag the table (Optional[Dict[str, str]])
        :param columns_comments: Columns names and the related comments (Optional[Dict[str, str]])
        :return: List of objects written on S3
        """
        if serde not in Pandas.VALID_CSV_SERDES:
            raise InvalidSerDe(f"{serde} in not in the valid SerDe list ({Pandas.VALID_CSV_SERDES})")
        extra_args: Dict[str, Optional[str]] = {"sep": sep, "serde": serde, "escapechar": escapechar}
        return self.to_s3(dataframe=dataframe,
                          path=path,
                          file_format="csv",
                          database=database,
                          table=table,
                          partition_cols=partition_cols,
                          preserve_index=preserve_index,
                          mode=mode,
                          compression=None,
                          procs_cpu_bound=procs_cpu_bound,
                          procs_io_bound=procs_io_bound,
                          extra_args=extra_args,
                          inplace=inplace,
                          description=description,
                          parameters=parameters,
                          columns_comments=columns_comments)

    def to_parquet(self,
                   dataframe,
                   path,
                   database: Optional[str] = None,
                   table=None,
                   partition_cols=None,
                   preserve_index=True,
                   mode="append",
                   compression="snappy",
                   procs_cpu_bound=None,
                   procs_io_bound=None,
                   cast_columns=None,
                   inplace=True,
                   description: Optional[str] = None,
                   parameters: Optional[Dict[str, str]] = None,
                   columns_comments: Optional[Dict[str, str]] = None):
        """
        Write a Pandas Dataframe as parquet files on S3
        Optionally writes metadata on AWS Glue.

        :param dataframe: Pandas Dataframe
        :param path: AWS S3 path (E.g. s3://bucket-name/folder_name/)
        :param database: AWS Glue Database name
        :param table: AWS Glue table name
        :param partition_cols: List of columns names that will be partitions on S3
        :param preserve_index: Should preserve index on S3?
        :param mode: "append", "overwrite", "overwrite_partitions"
        :param compression: None, snappy, gzip, lzo
        :param procs_cpu_bound: Number of cores used for CPU bound tasks
        :param procs_io_bound: Number of cores used for I/O bound tasks
        :param cast_columns: Dictionary of columns names and Athena/Glue types to be casted (E.g. {"col name": "bigint", "col2 name": "int"})
        :param inplace: True is cheapest (CPU and Memory) but False leaves your DataFrame intact
        :param description: Table description
        :param parameters: Key/value pairs to tag the table (Optional[Dict[str, str]])
        :param columns_comments: Columns names and the related comments (Optional[Dict[str, str]])
        :return: List of objects written on S3
        """
        return self.to_s3(dataframe=dataframe,
                          path=path,
                          file_format="parquet",
                          database=database,
                          table=table,
                          partition_cols=partition_cols,
                          preserve_index=preserve_index,
                          mode=mode,
                          compression=compression,
                          procs_cpu_bound=procs_cpu_bound,
                          procs_io_bound=procs_io_bound,
                          cast_columns=cast_columns,
                          inplace=inplace,
                          description=description,
                          parameters=parameters,
                          columns_comments=columns_comments)

    def to_s3(self,
              dataframe: pd.DataFrame,
              path: str,
              file_format: str,
              database: Optional[str] = None,
              table: Optional[str] = None,
              partition_cols=None,
              preserve_index=True,
              mode: str = "append",
              compression=None,
              procs_cpu_bound=None,
              procs_io_bound=None,
              cast_columns=None,
              extra_args: Optional[Dict[str, Optional[str]]] = None,
              inplace: bool = True,
              description: Optional[str] = None,
              parameters: Optional[Dict[str, str]] = None,
              columns_comments: Optional[Dict[str, str]] = None) -> List[str]:
        """
        Write a Pandas Dataframe on S3
        Optionally writes metadata on AWS Glue.

        :param dataframe: Pandas Dataframe
        :param path: AWS S3 path (E.g. s3://bucket-name/folder_name/
        :param file_format: "csv" or "parquet"
        :param database: AWS Glue Database name
        :param table: AWS Glue table name
        :param partition_cols: List of columns names that will be partitions on S3
        :param preserve_index: Should preserve index on S3?
        :param mode: "append", "overwrite", "overwrite_partitions"
        :param compression: None, gzip, snappy, etc
        :param procs_cpu_bound: Number of cores used for CPU bound tasks
        :param procs_io_bound: Number of cores used for I/O bound tasks
        :param cast_columns: Dictionary of columns names and Athena/Glue types to be casted. (E.g. {"col name": "bigint", "col2 name": "int"}) (Only for "parquet" file_format)
        :param extra_args: Extra arguments specific for each file formats (E.g. "sep" for CSV)
        :param inplace: True is cheapest (CPU and Memory) but False leaves your DataFrame intact
        :param description: Table description
        :param parameters: Key/value pairs to tag the table (Optional[Dict[str, str]])
        :param columns_comments: Columns names and the related comments (Optional[Dict[str, str]])
        :return: List of objects written on S3
        """
        if partition_cols is None:
            partition_cols = []
        if cast_columns is None:
            cast_columns = {}
        dataframe = Pandas.normalize_columns_names_athena(dataframe, inplace=inplace)
        cast_columns = {Athena.normalize_column_name(k): v for k, v in cast_columns.items()}
        logger.debug(f"cast_columns: {cast_columns}")
        partition_cols = [Athena.normalize_column_name(x) for x in partition_cols]
        logger.debug(f"partition_cols: {partition_cols}")
        dataframe = Pandas.drop_duplicated_columns(dataframe=dataframe, inplace=inplace)
        if compression is not None:
            compression = compression.lower()
        file_format = file_format.lower()
        if file_format == "csv":
            if compression not in Pandas.VALID_CSV_COMPRESSIONS:
                raise InvalidCompression(
                    f"{compression} isn't a valid CSV compression. Try: {Pandas.VALID_CSV_COMPRESSIONS}")
        elif file_format == "parquet":
            if compression not in Pandas.VALID_PARQUET_COMPRESSIONS:
                raise InvalidCompression(
                    f"{compression} isn't a valid PARQUET compression. Try: {Pandas.VALID_PARQUET_COMPRESSIONS}")
        else:
            raise UnsupportedFileFormat(file_format)
        if dataframe.empty:
            raise EmptyDataframe()
        if ((mode == "overwrite") or ((mode == "overwrite_partitions") and  # noqa
                                      (not partition_cols))):
            self._session.s3.delete_objects(path=path)
        elif mode not in ["overwrite_partitions", "append"]:
            raise UnsupportedWriteMode(mode)
        objects_paths = self.data_to_s3(dataframe=dataframe,
                                        path=path,
                                        partition_cols=partition_cols,
                                        preserve_index=preserve_index,
                                        file_format=file_format,
                                        mode=mode,
                                        compression=compression,
                                        procs_cpu_bound=procs_cpu_bound,
                                        procs_io_bound=procs_io_bound,
                                        cast_columns=cast_columns,
                                        extra_args=extra_args)
        if database:
            self._session.glue.metadata_to_glue(dataframe=dataframe,
                                                path=path,
                                                objects_paths=objects_paths,
                                                database=database,
                                                table=table,
                                                partition_cols=partition_cols,
                                                preserve_index=preserve_index,
                                                file_format=file_format,
                                                mode=mode,
                                                compression=compression,
                                                cast_columns=cast_columns,
                                                extra_args=extra_args,
                                                description=description,
                                                parameters=parameters,
                                                columns_comments=columns_comments)
        return objects_paths

    def data_to_s3(self,
                   dataframe,
                   path,
                   file_format,
                   partition_cols=None,
                   preserve_index=True,
                   mode="append",
                   compression=None,
                   procs_cpu_bound=None,
                   procs_io_bound=None,
                   cast_columns=None,
                   extra_args=None):
        if procs_cpu_bound is None:
            procs_cpu_bound = self._session.procs_cpu_bound
        if procs_io_bound is None:
            procs_io_bound = self._session.procs_io_bound
        logger.debug(f"procs_cpu_bound: {procs_cpu_bound}")
        logger.debug(f"procs_io_bound: {procs_io_bound}")
        if path[-1] == "/":
            path = path[:-1]
        objects_paths = []
        if procs_cpu_bound > 1:
            bounders = _get_bounders(dataframe=dataframe, num_partitions=procs_cpu_bound)
            procs = []
            receive_pipes = []
            for bounder in bounders:
                receive_pipe, send_pipe = mp.Pipe()
                proc = mp.Process(
                    target=self._data_to_s3_dataset_writer_remote,
                    args=(send_pipe, dataframe.iloc[bounder[0]:bounder[1], :], path, partition_cols, preserve_index,
                          compression, self._session.primitives, file_format, cast_columns, extra_args),
                )
                proc.daemon = False
                proc.start()
                procs.append(proc)
                receive_pipes.append(receive_pipe)
            for i in range(len(procs)):
                objects_paths += receive_pipes[i].recv()
                procs[i].join()
                receive_pipes[i].close()
        else:
            objects_paths += self._data_to_s3_dataset_writer(dataframe=dataframe,
                                                             path=path,
                                                             partition_cols=partition_cols,
                                                             preserve_index=preserve_index,
                                                             compression=compression,
                                                             session_primitives=self._session.primitives,
                                                             file_format=file_format,
                                                             cast_columns=cast_columns,
                                                             extra_args=extra_args)
        if mode == "overwrite_partitions" and partition_cols:
            if procs_io_bound > procs_cpu_bound:
                num_procs = floor(float(procs_io_bound) / float(procs_cpu_bound))
            else:
                num_procs = 1
            logger.debug(f"num_procs for delete_not_listed_objects: {num_procs}")
            self._session.s3.delete_not_listed_objects(objects_paths=objects_paths, procs_io_bound=num_procs)
        return objects_paths

    @staticmethod
    def _data_to_s3_dataset_writer(dataframe: pd.DataFrame,
                                   path: str,
                                   partition_cols: Optional[List[str]],
                                   preserve_index: bool,
                                   compression,
                                   session_primitives: "SessionPrimitives",
                                   file_format: str,
                                   cast_columns=None,
                                   extra_args: Optional[Dict[str, Optional[str]]] = None,
                                   isolated_dataframe: bool = False):
        objects_paths: List[str] = []
        dataframe = Pandas._cast_pandas(dataframe=dataframe, cast_columns=cast_columns)
        if partition_cols is None:
            partition_cols = []
        cast_columns_materialized: Dict[str, str] = {c: t for c, t in cast_columns.items() if c not in partition_cols}
        if not partition_cols:
            object_path = Pandas._data_to_s3_object_writer(dataframe=dataframe,
                                                           path=path,
                                                           preserve_index=preserve_index,
                                                           compression=compression,
                                                           session_primitives=session_primitives,
                                                           file_format=file_format,
                                                           cast_columns=cast_columns_materialized,
                                                           extra_args=extra_args,
                                                           isolated_dataframe=isolated_dataframe)
            objects_paths.append(object_path)
        else:
            dataframe = Pandas._cast_pandas(dataframe=dataframe, cast_columns=cast_columns)
            for keys, subgroup in dataframe.groupby(partition_cols):
                subgroup = subgroup.drop(partition_cols, axis="columns")
                if not isinstance(keys, tuple):
                    keys = (keys, )
                subdir = "/".join([f"{name}={val}" for name, val in zip(partition_cols, keys)])
                prefix = "/".join([path, subdir])
                object_path = Pandas._data_to_s3_object_writer(dataframe=subgroup,
                                                               path=prefix,
                                                               preserve_index=preserve_index,
                                                               compression=compression,
                                                               session_primitives=session_primitives,
                                                               file_format=file_format,
                                                               cast_columns=cast_columns_materialized,
                                                               extra_args=extra_args,
                                                               isolated_dataframe=True)
                objects_paths.append(object_path)
        return objects_paths

    @staticmethod
    def _cast_pandas(dataframe: pd.DataFrame, cast_columns: Dict[str, str]) -> pd.DataFrame:
        for col, athena_type in cast_columns.items():
            pandas_type: str = data_types.athena2pandas(dtype=athena_type)
            if pandas_type == "datetime64":
                dataframe[col] = pd.to_datetime(dataframe[col])
            elif pandas_type == "date":
                dataframe[col] = pd.to_datetime(dataframe[col]).dt.date.replace(to_replace={pd.NaT: None})
            else:
                dataframe[col] = dataframe[col].astype(pandas_type, skipna=True)
        return dataframe

    @staticmethod
    def _data_to_s3_dataset_writer_remote(send_pipe,
                                          dataframe: pd.DataFrame,
                                          path: str,
                                          partition_cols,
                                          preserve_index,
                                          compression,
                                          session_primitives: "SessionPrimitives",
                                          file_format,
                                          cast_columns=None,
                                          extra_args: Optional[Dict[str, Optional[str]]] = None):
        send_pipe.send(
            Pandas._data_to_s3_dataset_writer(dataframe=dataframe,
                                              path=path,
                                              partition_cols=partition_cols,
                                              preserve_index=preserve_index,
                                              compression=compression,
                                              session_primitives=session_primitives,
                                              file_format=file_format,
                                              cast_columns=cast_columns,
                                              extra_args=extra_args,
                                              isolated_dataframe=True))
        send_pipe.close()

    @staticmethod
    def _data_to_s3_object_writer(dataframe: pd.DataFrame,
                                  path: str,
                                  preserve_index: bool,
                                  compression: str,
                                  session_primitives: "SessionPrimitives",
                                  file_format: str,
                                  cast_columns: Optional[Dict[str, str]] = None,
                                  extra_args: Optional[Dict[str, Optional[str]]] = None,
                                  isolated_dataframe=False) -> str:
        fs = get_fs(session_primitives=session_primitives)
        fs = pa.filesystem._ensure_filesystem(fs)
        mkdir_if_not_exists(fs, path)

        if compression is None:
            compression_extension: str = ""
        elif compression == "snappy":
            compression_extension = ".snappy"
        elif compression == "gzip":
            compression_extension = ".gz"
        else:
            raise InvalidCompression(compression)

        guid: str = pa.compat.guid()
        if file_format == "parquet":
            outfile: str = f"{guid}{compression_extension}.parquet"
        elif file_format == "csv":
            outfile = f"{guid}{compression_extension}.csv"
        else:
            raise UnsupportedFileFormat(file_format)
        object_path: str = "/".join([path, outfile])
        if file_format == "parquet":
            Pandas.write_parquet_dataframe(dataframe=dataframe,
                                           path=object_path,
                                           preserve_index=preserve_index,
                                           compression=compression,
                                           fs=fs,
                                           cast_columns=cast_columns,
                                           isolated_dataframe=isolated_dataframe)
        elif file_format == "csv":
            Pandas.write_csv_dataframe(dataframe=dataframe,
                                       path=object_path,
                                       preserve_index=preserve_index,
                                       compression=compression,
                                       fs=fs,
                                       extra_args=extra_args)
        return object_path

    @staticmethod
    def write_csv_dataframe(dataframe, path, preserve_index, compression, fs, extra_args=None):
        csv_extra_args = {}
        sep = extra_args.get("sep")
        if sep is not None:
            csv_extra_args["sep"] = sep

        serde = extra_args.get("serde")
        if serde is None:
            escapechar = extra_args.get("escapechar")
            if escapechar is not None:
                csv_extra_args["escapechar"] = escapechar
        else:
            if serde == "OpenCSVSerDe":
                csv_extra_args["quoting"] = csv.QUOTE_ALL
                csv_extra_args["escapechar"] = "\\"
            elif serde == "LazySimpleSerDe":
                csv_extra_args["quoting"] = csv.QUOTE_NONE
                csv_extra_args["escapechar"] = "\\"
        csv_buffer = bytes(
            dataframe.to_csv(None, header=False, index=preserve_index, compression=compression, **csv_extra_args),
            "utf-8")
        Pandas._write_csv_to_s3_retrying(fs=fs, path=path, buffer=csv_buffer)

    @staticmethod
    @tenacity.retry(retry=tenacity.retry_if_exception_type(exception_types=(ClientError, HTTPClientError)),
                    wait=tenacity.wait_random_exponential(multiplier=0.5),
                    stop=tenacity.stop_after_attempt(max_attempt_number=10),
                    reraise=True,
                    after=tenacity.after_log(logger, INFO))
    def _write_csv_to_s3_retrying(fs: Any, path: str, buffer: bytes) -> None:
        with fs.open(path, "wb") as f:
            f.write(buffer)

    @staticmethod
    def write_parquet_dataframe(dataframe, path, preserve_index, compression, fs, cast_columns, isolated_dataframe):
        if not cast_columns:
            cast_columns = {}

        # Casting on Pandas
        casted_in_pandas = []
        dtypes = copy.deepcopy(dataframe.dtypes.to_dict())
        for name, dtype in dtypes.items():
            if str(dtype) == "Int64":
                dataframe[name] = dataframe[name].astype("float64")
                casted_in_pandas.append(name)
                cast_columns[name] = "bigint"
                logger.debug(f"Casting column {name} Int64 to float64")

        # Converting Pandas Dataframe to Pyarrow's Table
        table = pa.Table.from_pandas(df=dataframe, preserve_index=preserve_index, safe=False)

        # Casting on Pyarrow
        if cast_columns:
            for col_name, dtype in cast_columns.items():
                col_index = table.column_names.index(col_name)
                pyarrow_dtype = data_types.athena2pyarrow(dtype)
                field = pa.field(name=col_name, type=pyarrow_dtype)
                table = table.set_column(col_index, field, table.column(col_name).cast(pyarrow_dtype))
                logger.debug(f"Casting column {col_name} ({col_index}) to {dtype} ({pyarrow_dtype})")

        # Persisting on S3
        Pandas._write_parquet_to_s3_retrying(fs=fs, path=path, table=table, compression=compression)

        # Casting back on Pandas if necessary
        if isolated_dataframe is False:
            for col in casted_in_pandas:
                dataframe[col] = dataframe[col].astype("Int64")

    @staticmethod
    @tenacity.retry(retry=tenacity.retry_if_exception_type(exception_types=(ClientError, HTTPClientError)),
                    wait=tenacity.wait_random_exponential(multiplier=0.5),
                    stop=tenacity.stop_after_attempt(max_attempt_number=10),
                    reraise=True,
                    after=tenacity.after_log(logger, INFO))
    def _write_parquet_to_s3_retrying(fs: Any, path: str, table: pa.Table, compression: str) -> None:
        with fs.open(path, "wb") as f:
            pq.write_table(table, f, compression=compression, coerce_timestamps="ms", flavor="spark")

    def to_redshift(
            self,
            dataframe: pd.DataFrame,
            path: str,
            connection: Any,
            schema: str,
            table: str,
            iam_role: str,
            diststyle: str = "AUTO",
            distkey: Optional[str] = None,
            sortstyle: str = "COMPOUND",
            sortkey: Optional[str] = None,
            primary_keys: Optional[List[str]] = None,
            preserve_index: bool = False,
            mode: str = "append",
            cast_columns: Optional[Dict[str, str]] = None,
    ) -> None:
        """
        Load Pandas Dataframe as a Table on Amazon Redshift

        :param dataframe: Pandas Dataframe
        :param path: S3 path to write temporary files (E.g. s3://BUCKET_NAME/ANY_NAME/)
        :param connection: A PEP 249 compatible connection (Can be generated with Redshift.generate_connection())
        :param schema: The Redshift Schema for the table
        :param table: The name of the desired Redshift table
        :param iam_role: AWS IAM role with the related permissions
        :param diststyle: Redshift distribution styles. Must be in ["AUTO", "EVEN", "ALL", "KEY"] (https://docs.aws.amazon.com/redshift/latest/dg/t_Distributing_data.html)
        :param distkey: Specifies a column name or positional number for the distribution key
        :param sortstyle: Sorting can be "COMPOUND" or "INTERLEAVED" (https://docs.aws.amazon.com/redshift/latest/dg/t_Sorting_data.html)
        :param sortkey: List of columns to be sorted
        :param primary_keys: Primary keys
        :param preserve_index: Should we preserve the Dataframe index?
        :param mode: append, overwrite or upsert
        :param cast_columns: Dictionary of columns names and Redshift types to be casted. (E.g. {"col name": "SMALLINT", "col2 name": "FLOAT4"})
        :return: None
        """
        if cast_columns is None:
            cast_columns = {}
            cast_columns_parquet: Dict = {}
        else:
            cast_columns_tuples: List[Tuple[str, str]] = [(k, v) for k, v in cast_columns.items()]
            cast_columns_parquet = data_types.convert_schema(func=data_types.redshift2athena,
                                                             schema=cast_columns_tuples)
        if path[-1] != "/":
            path += "/"
        self._session.s3.delete_objects(path=path)
        num_rows: int = len(dataframe.index)
        logger.debug(f"Number of rows: {num_rows}")
        if num_rows < MIN_NUMBER_OF_ROWS_TO_DISTRIBUTE:
            num_partitions: int = 1
        else:
            num_slices: int = self._session.redshift.get_number_of_slices(redshift_conn=connection)
            logger.debug(f"Number of slices on Redshift: {num_slices}")
            num_partitions = num_slices
        logger.debug(f"Number of partitions calculated: {num_partitions}")
        objects_paths: List[str] = self.to_parquet(dataframe=dataframe,
                                                   path=path,
                                                   preserve_index=preserve_index,
                                                   mode="append",
                                                   procs_cpu_bound=num_partitions,
                                                   cast_columns=cast_columns_parquet)
        manifest_path: str = f"{path}manifest.json"
        self._session.redshift.write_load_manifest(manifest_path=manifest_path, objects_paths=objects_paths)
        self._session.redshift.load_table(
            dataframe=dataframe,
            dataframe_type="pandas",
            manifest_path=manifest_path,
            schema_name=schema,
            table_name=table,
            redshift_conn=connection,
            preserve_index=preserve_index,
            num_files=num_partitions,
            iam_role=iam_role,
            diststyle=diststyle,
            distkey=distkey,
            sortstyle=sortstyle,
            sortkey=sortkey,
            primary_keys=primary_keys,
            mode=mode,
            cast_columns=cast_columns,
        )
        self._session.s3.delete_objects(path=path)

    def read_log_query(self,
                       query,
                       log_group_names,
                       start_time=datetime(year=1970, month=1, day=1),
                       end_time=datetime.utcnow(),
                       limit=None):
        """
        Run a query against AWS CloudWatchLogs Insights and convert the results to Pandas DataFrame

        :param query: The query string to use. https://docs.aws.amazon.com/AmazonCloudWatch/latest/logs/CWL_QuerySyntax.html
        :param log_group_names: The list of log groups to be queried. You can include up to 20 log groups.
        :param start_time: The beginning of the time range to query (datetime.datetime object)
        :param end_time: The end of the time range to query (datetime.datetime object)
        :param limit: The maximum number of log events to return in the query. If the query string uses the fields command, only the specified fields and their values are returned.
        :return: Results as a Pandas DataFrame
        """
        results = self._session.cloudwatchlogs.query(query=query,
                                                     log_group_names=log_group_names,
                                                     start_time=start_time,
                                                     end_time=end_time,
                                                     limit=limit)
        pre_df = []
        for row in results:
            new_row = {}
            for col in row:
                if col["field"].startswith("@"):
                    col_name = col["field"].replace("@", "", 1)
                else:
                    col_name = col["field"]
                new_row[col_name] = col["value"]
            pre_df.append(new_row)
        return pd.DataFrame(pre_df)

    @staticmethod
    def normalize_columns_names_athena(dataframe, inplace=True):
        if inplace is False:
            dataframe = dataframe.copy(deep=True)
        dataframe.columns = [Athena.normalize_column_name(x) for x in dataframe.columns]
        return dataframe

    @staticmethod
    def drop_duplicated_columns(dataframe: pd.DataFrame, inplace: bool = True) -> pd.DataFrame:
        if inplace is False:
            dataframe = dataframe.copy(deep=True)
        duplicated_cols = dataframe.columns.duplicated()
        duplicated_cols_names = list(dataframe.columns[duplicated_cols])
        if len(duplicated_cols_names) > 0:
            logger.warning(f"Dropping repeated columns: {duplicated_cols_names}")
        return dataframe.loc[:, ~duplicated_cols]

    def read_parquet(self,
                     path: Union[str, List[str]],
                     columns: Optional[List[str]] = None,
                     filters: Optional[Union[List[Tuple[Any]], List[List[Tuple[Any]]]]] = None,
                     procs_cpu_bound: Optional[int] = None,
                     wait_objects: bool = False,
                     wait_objects_timeout: Optional[float] = 10.0) -> pd.DataFrame:
        """
        Read parquet data from S3

        :param path: AWS S3 path or List of paths (E.g. s3://bucket-name/folder_name/)
        :param columns: Names of columns to read from the file
        :param filters: List of filters to apply, like ``[[('x', '=', 0), ...], ...]``.
        :param procs_cpu_bound: Number of cores used for CPU bound tasks
        :param wait_objects: Wait for all files exists (Not valid when path is a directory) (Useful for eventual consistency situations)
        :param wait_objects_timeout: Wait objects Timeout (seconds)
        :return: Pandas DataFrame
        """
        procs_cpu_bound = procs_cpu_bound if procs_cpu_bound is not None else self._session.procs_cpu_bound if self._session.procs_cpu_bound is not None else 1
        logger.debug(f"procs_cpu_bound: {procs_cpu_bound}")
        df: Optional[pd.DataFrame] = None
        session_primitives = self._session.primitives
        path = [path] if type(path) == str else path  # type: ignore
        bounders = calculate_bounders(len(path), procs_cpu_bound)
        logger.debug(f"len(bounders): {len(bounders)}")
        if len(bounders) == 1:
            df = Pandas._read_parquet_paths(session_primitives=session_primitives,
                                            path=path,
                                            columns=columns,
                                            filters=filters,
                                            procs_cpu_bound=procs_cpu_bound,
                                            wait_objects=wait_objects,
                                            wait_objects_timeout=wait_objects_timeout)
        else:
            procs = []
            receive_pipes = []
            for bounder in bounders:
                receive_pipe, send_pipe = mp.Pipe()
                logger.debug(f"bounder: {bounder}")
                proc = mp.Process(
                    target=self._read_parquet_paths_remote,
                    args=(
                        send_pipe,
                        session_primitives,
                        path[bounder[0]:bounder[1]],
                        columns,
                        filters,
                        1,  # procs_cpu_bound
                        wait_objects,
                        wait_objects_timeout),
                )
                proc.daemon = False
                proc.start()
                procs.append(proc)
                receive_pipes.append(receive_pipe)
            logger.debug(f"len(procs): {len(bounders)}")
            for i in range(len(procs)):
                logger.debug(f"Waiting pipe number: {i}")
                df_received = receive_pipes[i].recv()
                if df is None:
                    df = df_received
                else:
                    df = pd.concat(objs=[df, df_received], ignore_index=True)
                logger.debug(f"Waiting proc number: {i}")
                procs[i].join()
                logger.debug(f"Closing proc number: {i}")
                receive_pipes[i].close()
        return df

    @staticmethod
    def _read_parquet_paths_remote(send_pipe: mp.connection.Connection,
                                   session_primitives: Any,
                                   path: Union[str, List[str]],
                                   columns: Optional[List[str]] = None,
                                   filters: Optional[Union[List[Tuple[Any]], List[List[Tuple[Any]]]]] = None,
                                   procs_cpu_bound: Optional[int] = None,
                                   wait_objects: bool = False,
                                   wait_objects_timeout: Optional[float] = 10.0):
        df: pd.DataFrame = Pandas._read_parquet_paths(session_primitives=session_primitives,
                                                      path=path,
                                                      columns=columns,
                                                      filters=filters,
                                                      procs_cpu_bound=procs_cpu_bound,
                                                      wait_objects=wait_objects,
                                                      wait_objects_timeout=wait_objects_timeout)
        send_pipe.send(df)
        send_pipe.close()

    @staticmethod
    def _read_parquet_paths(session_primitives: Any,
                            path: Union[str, List[str]],
                            columns: Optional[List[str]] = None,
                            filters: Optional[Union[List[Tuple[Any]], List[List[Tuple[Any]]]]] = None,
                            procs_cpu_bound: Optional[int] = None,
                            wait_objects: bool = False,
                            wait_objects_timeout: Optional[float] = 10.0) -> pd.DataFrame:
        """
        Read parquet data from S3

        :param session_primitives: SessionPrimitives()
        :param path: AWS S3 path or List of paths (E.g. s3://bucket-name/folder_name/)
        :param columns: Names of columns to read from the file
        :param filters: List of filters to apply, like ``[[('x', '=', 0), ...], ...]``.
        :param procs_cpu_bound: Number of cores used for CPU bound tasks
        :param wait_objects: Wait for all files exists (Not valid when path is a directory) (Useful for eventual consistency situations)
        :param wait_objects: Wait objects Timeout (seconds)
        :return: Pandas DataFrame
        """
        df: pd.DataFrame
        if (type(path) == str) or (len(path) == 1):
            path = path[0] if type(path) == list else path  # type: ignore
            df = Pandas._read_parquet_path(
                session_primitives=session_primitives,
                path=path,  # type: ignore
                columns=columns,
                filters=filters,
                procs_cpu_bound=procs_cpu_bound,
                wait_objects=wait_objects,
                wait_objects_timeout=wait_objects_timeout)
        else:
            df = Pandas._read_parquet_path(session_primitives=session_primitives,
                                           path=path[0],
                                           columns=columns,
                                           filters=filters,
                                           procs_cpu_bound=procs_cpu_bound,
                                           wait_objects=wait_objects,
                                           wait_objects_timeout=wait_objects_timeout)
            for p in path[1:]:
                df_aux = Pandas._read_parquet_path(session_primitives=session_primitives,
                                                   path=p,
                                                   columns=columns,
                                                   filters=filters,
                                                   procs_cpu_bound=procs_cpu_bound,
                                                   wait_objects=wait_objects,
                                                   wait_objects_timeout=wait_objects_timeout)
                df = pd.concat(objs=[df, df_aux], ignore_index=True)
        return df

    @staticmethod
    def _read_parquet_path(session_primitives: "SessionPrimitives",
                           path: str,
                           columns: Optional[List[str]] = None,
                           filters: Optional[Union[List[Tuple[Any]], List[List[Tuple[Any]]]]] = None,
                           procs_cpu_bound: Optional[int] = None,
                           wait_objects: bool = False,
                           wait_objects_timeout: Optional[float] = 10.0) -> pd.DataFrame:
        """
        Read parquet data from S3

        :param session_primitives: SessionPrimitives()
        :param path: AWS S3 path (E.g. s3://bucket-name/folder_name/)
        :param columns: Names of columns to read from the file
        :param filters: List of filters to apply, like ``[[('x', '=', 0), ...], ...]``.
        :param procs_cpu_bound: Number of cores used for CPU bound tasks
        :param wait_objects: Wait for all files exists (Not valid when path is a directory) (Useful for eventual consistency situations)
        :param wait_objects: Wait objects Timeout (seconds)
        :return: Pandas DataFrame
        """
        session: Session = session_primitives.session
        if wait_objects is True:
            logger.debug(f"waiting {path}...")
            session.s3.wait_object_exists(path=path, timeout=wait_objects_timeout)
            is_file: bool = True
        else:
            logger.debug(f"checking if {path} exists...")
            is_file = session.s3.does_object_exists(path=path)
            if is_file is False:
                path = path[:-1] if path[-1] == "/" else path
        logger.debug(f"is_file: {is_file}")
        procs_cpu_bound = procs_cpu_bound if procs_cpu_bound is not None else session_primitives.procs_cpu_bound if session_primitives.procs_cpu_bound is not None else 1
        use_threads: bool = True if procs_cpu_bound > 1 else False
        logger.debug(f"Reading Parquet: {path}")
        if is_file is True:
            client_s3 = session.boto3_session.client(service_name="s3", use_ssl=True, config=session.botocore_config)
            bucket, key = path.replace("s3://", "").split("/", 1)
            obj = client_s3.get_object(Bucket=bucket, Key=key)
            table = pq.ParquetFile(source=BytesIO(obj["Body"].read())).read(columns=columns, use_threads=use_threads)
        else:
            fs: S3FileSystem = get_fs(session_primitives=session_primitives)
            fs = pa.filesystem._ensure_filesystem(fs)
            fs.invalidate_cache()
            table = pq.read_table(source=path, columns=columns, filters=filters, filesystem=fs, use_threads=use_threads)
        # Check if we lose some integer during the conversion (Happens when has some null value)
        integers = [field.name for field in table.schema if str(field.type).startswith("int")]
        logger.debug(f"Converting to Pandas: {path}")
        df = table.to_pandas(use_threads=use_threads, integer_object_nulls=True)
        for c in integers:
            if not str(df[c].dtype).startswith("int"):
                df[c] = df[c].astype("Int64")
        logger.debug(f"Done: {path}")
        return df

    def read_table(self,
                   database: str,
                   table: str,
                   columns: Optional[List[str]] = None,
                   filters: Optional[Union[List[Tuple[Any]], List[List[Tuple[Any]]]]] = None,
                   procs_cpu_bound: Optional[int] = None) -> pd.DataFrame:
        """
        Read PARQUET table from S3 using the Glue Catalog location skipping Athena's necessity

        :param database: Database name
        :param table: table name
        :param columns: Names of columns to read from the file
        :param filters: List of filters to apply, like ``[[('x', '=', 0), ...], ...]``.
        :param procs_cpu_bound: Number of cores used for CPU bound tasks
        """
        path: str = self._session.glue.get_table_location(database=database, table=table)
        return self.read_parquet(path=path, columns=columns, filters=filters, procs_cpu_bound=procs_cpu_bound)

    def read_sql_redshift(self,
                          sql: str,
                          iam_role: str,
                          connection: Any,
                          temp_s3_path: Optional[str] = None,
                          procs_cpu_bound: Optional[int] = None) -> pd.DataFrame:
        """
        Convert a query result in a Pandas Dataframe.

        :param sql: SQL Query
        :param iam_role: AWS IAM role with the related permissions
        :param connection: A PEP 249 compatible connection (Can be generated with Redshift.generate_connection())
        :param temp_s3_path: AWS S3 path to write temporary data (e.g. s3://...) (Default uses the Athena's results bucket)
        :param procs_cpu_bound: Number of cores used for CPU bound tasks
        """
        guid: str = pa.compat.guid()
        name: str = f"temp_redshift_{guid}"
        if temp_s3_path is None:
            if self._session.redshift_temp_s3_path is not None:
                temp_s3_path = self._session.redshift_temp_s3_path
            else:
                temp_s3_path = self._session.athena.create_athena_bucket()
        temp_s3_path = temp_s3_path[:-1] if temp_s3_path[-1] == "/" else temp_s3_path
        temp_s3_path = f"{temp_s3_path}/{name}"
        logger.debug(f"temp_s3_path: {temp_s3_path}")
        self._session.s3.delete_objects(path=temp_s3_path)
        paths: Optional[List[str]] = None
        try:
            paths = self._session.redshift.to_parquet(sql=sql,
                                                      path=temp_s3_path,
                                                      iam_role=iam_role,
                                                      connection=connection)
            logger.debug(f"paths: {paths}")
            df: pd.DataFrame = self.read_parquet(path=paths, procs_cpu_bound=procs_cpu_bound)  # type: ignore
            self._session.s3.delete_listed_objects(objects_paths=paths + [temp_s3_path + "/manifest"])  # type: ignore
            return df
        except Exception as e:
            if paths is not None:
                self._session.s3.delete_listed_objects(objects_paths=paths + [temp_s3_path + "/manifest"])
            else:
                self._session.s3.delete_objects(path=temp_s3_path)
            raise e

    def to_aurora(self,
                  dataframe: pd.DataFrame,
                  connection: Any,
                  schema: str,
                  table: str,
                  engine: str = "mysql",
                  temp_s3_path: Optional[str] = None,
                  preserve_index: bool = False,
                  mode: str = "append",
                  procs_cpu_bound: Optional[int] = None,
                  procs_io_bound: Optional[int] = None,
                  inplace=True) -> None:
        """
        Load Pandas Dataframe as a Table on Aurora

        :param dataframe: Pandas Dataframe
        :param connection: Glue connection name (str) OR a PEP 249 compatible connection (Can be generated with Redshift.generate_connection())
        :param schema: The Redshift Schema for the table
        :param table: The name of the desired Redshift table
        :param engine: "mysql" or "postgres"
        :param temp_s3_path: S3 path to write temporary files (E.g. s3://BUCKET_NAME/ANY_NAME/)
        :param preserve_index: Should we preserve the Dataframe index?
        :param mode: append or overwrite
        :param procs_cpu_bound: Number of cores used for CPU bound tasks
        :param procs_io_bound: Number of cores used for I/O bound tasks
        :param inplace: True is cheapest (CPU and Memory) but False leaves your DataFrame intact
        :return: None
        """
        if ("postgres" not in engine.lower()) and ("mysql" not in engine.lower()):
            raise InvalidEngine(f"{engine} is not a valid engine. Please use 'mysql' or 'postgres'!")
        generated_conn: bool = False
        if type(connection) == str:
            logger.debug("Glue connection (str) provided.")
            connection = self._session.glue.get_connection(name=connection)
            generated_conn = True
        try:
            if temp_s3_path is None:
                if self._session.aurora_temp_s3_path is not None:
                    temp_s3_path = self._session.aurora_temp_s3_path
                else:
                    guid: str = pa.compat.guid()
                    temp_directory = f"temp_aurora_{guid}"
                    temp_s3_path = self._session.athena.create_athena_bucket() + temp_directory + "/"
            temp_s3_path = temp_s3_path if temp_s3_path[-1] == "/" else temp_s3_path + "/"
            logger.debug(f"temp_s3_path: {temp_s3_path}")
            paths: List[str] = self.to_csv(dataframe=dataframe,
                                           path=temp_s3_path,
                                           sep=",",
                                           escapechar="\"",
                                           preserve_index=preserve_index,
                                           mode="overwrite",
                                           procs_cpu_bound=procs_cpu_bound,
                                           procs_io_bound=procs_io_bound,
                                           inplace=inplace)
            load_paths: List[str]
            region: str = "us-east-1"
            if "postgres" in engine.lower():
                load_paths = paths.copy()
                bucket, _ = Pandas._parse_path(path=load_paths[0])
                region = self._session.s3.get_bucket_region(bucket=bucket)
            elif "mysql" in engine.lower():
                manifest_path: str = f"{temp_s3_path}manifest_{pa.compat.guid()}.json"
                self._session.aurora.write_load_manifest(manifest_path=manifest_path, objects_paths=paths)
                load_paths = [manifest_path]
            logger.debug(f"load_paths: {load_paths}")
            Aurora.load_table(dataframe=dataframe,
                              dataframe_type="pandas",
                              load_paths=load_paths,
                              schema_name=schema,
                              table_name=table,
                              connection=connection,
                              num_files=len(paths),
                              mode=mode,
                              preserve_index=preserve_index,
                              engine=engine,
                              region=region)
            if "postgres" in engine.lower():
                self._session.s3.delete_listed_objects(objects_paths=load_paths, procs_io_bound=procs_io_bound)
            elif "mysql" in engine.lower():
                self._session.s3.delete_listed_objects(objects_paths=load_paths + [manifest_path],
                                                       procs_io_bound=procs_io_bound)
        except Exception as ex:
            connection.rollback()
            if generated_conn is True:
                connection.close()
            raise ex
        if generated_conn is True:
            connection.close()

    def read_sql_aurora(self,
                        sql: str,
                        connection: Any,
                        col_names: Optional[List[str]] = None,
                        temp_s3_path: Optional[str] = None,
                        engine: str = "mysql",
                        max_result_size: Optional[int] = None) -> Union[pd.DataFrame, Iterator[pd.DataFrame]]:
        """
        Convert a query result in a Pandas Dataframe.

        :param sql: SQL Query
        :param connection: Glue connection name (str) OR a PEP 249 compatible connection (Can be generated with Aurora.generate_connection())
        :param col_names: List of column names. Default (None) is use columns IDs as column names.
        :param temp_s3_path: AWS S3 path to write temporary data (e.g. s3://...) (Default uses the Athena's results bucket)
        :param engine: Only "mysql" by now
        :param max_result_size: Max number of bytes on each request to S3
        :return: Pandas Dataframe or Iterator of Pandas Dataframes if max_result_size != None
        """
        if "mysql" not in engine.lower():
            raise InvalidEngine(f"{engine} is not a valid engine. Please use 'mysql'!")
        generated_conn: bool = False
        if type(connection) == str:
            logger.debug("Glue connection (str) provided.")
            connection = self._session.glue.get_connection(name=connection)
            generated_conn = True
        try:
            guid: str = pa.compat.guid()
            name: str = f"temp_aurora_{guid}"
            if temp_s3_path is None:
                if self._session.aurora_temp_s3_path is not None:
                    temp_s3_path = self._session.aurora_temp_s3_path
                else:
                    temp_s3_path = self._session.athena.create_athena_bucket()
            temp_s3_path = temp_s3_path[:-1] if temp_s3_path[-1] == "/" else temp_s3_path
            temp_s3_path = f"{temp_s3_path}/{name}"
            logger.debug(f"temp_s3_path: {temp_s3_path}")
            manifest_path: str = self._session.aurora.to_s3(sql=sql,
                                                            path=temp_s3_path,
                                                            connection=connection,
                                                            engine=engine)
            paths: List[str] = self._session.aurora.extract_manifest_paths(path=manifest_path)
            logger.debug(f"paths: {paths}")
            ret: Union[pd.DataFrame, Iterator[pd.DataFrame]]
            ret = self.read_csv_list(paths=paths, max_result_size=max_result_size, header=None, names=col_names)
            self._session.s3.delete_listed_objects(objects_paths=paths + [manifest_path])
        except Exception as ex:
            connection.rollback()
            if generated_conn is True:
                connection.close()
            raise ex
        if generated_conn is True:
            connection.close()
        return ret

    def read_csv_list(
            self,
            paths: List[str],
            max_result_size=None,
            header: Optional[str] = "infer",
            names=None,
            usecols=None,
            dtype=None,
            sep=",",
            thousands=None,
            decimal=".",
            lineterminator="\n",
            quotechar='"',
            quoting=csv.QUOTE_MINIMAL,
            escapechar=None,
            parse_dates: Union[bool, Dict, List] = False,
            infer_datetime_format=False,
            encoding="utf-8",
            converters=None,
    ) -> Union[pd.DataFrame, Iterator[pd.DataFrame]]:
        """
        Read CSV files from AWS S3 using optimized strategies.
        Try to mimic as most as possible pandas.read_csv()
        https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.read_csv.html
        P.S. max_result_size != None tries to mimic the chunksize behaviour in pandas.read_sql()

        :param paths: AWS S3 paths (E.g. S3://BUCKET_NAME/KEY_NAME)
        :param max_result_size: Max number of bytes on each request to S3
        :param header: Same as pandas.read_csv()
        :param names: Same as pandas.read_csv()
        :param usecols: Same as pandas.read_csv()
        :param dtype: Same as pandas.read_csv()
        :param sep: Same as pandas.read_csv()
        :param thousands: Same as pandas.read_csv()
        :param decimal: Same as pandas.read_csv()
        :param lineterminator: Same as pandas.read_csv()
        :param quotechar: Same as pandas.read_csv()
        :param quoting: Same as pandas.read_csv()
        :param escapechar: Same as pandas.read_csv()
        :param parse_dates: Same as pandas.read_csv()
        :param infer_datetime_format: Same as pandas.read_csv()
        :param encoding: Same as pandas.read_csv()
        :param converters: Same as pandas.read_csv()
        :return: Pandas Dataframe or Iterator of Pandas Dataframes if max_result_size != None
        """
        if max_result_size is not None:
            return self._read_csv_list_iterator(paths=paths,
                                                max_result_size=max_result_size,
                                                header=header,
                                                names=names,
                                                usecols=usecols,
                                                dtype=dtype,
                                                sep=sep,
                                                thousands=thousands,
                                                decimal=decimal,
                                                lineterminator=lineterminator,
                                                quotechar=quotechar,
                                                quoting=quoting,
                                                escapechar=escapechar,
                                                parse_dates=parse_dates,
                                                infer_datetime_format=infer_datetime_format,
                                                encoding=encoding,
                                                converters=converters)
        else:
            df_full: Optional[pd.DataFrame] = None
            for path in paths:
                logger.debug(f"path: {path}")
                bucket_name, key_path = Pandas._parse_path(path)
                df = self._read_csv_once(bucket_name=bucket_name,
                                         key_path=key_path,
                                         header=header,
                                         names=names,
                                         usecols=usecols,
                                         dtype=dtype,
                                         sep=sep,
                                         thousands=thousands,
                                         decimal=decimal,
                                         lineterminator=lineterminator,
                                         quotechar=quotechar,
                                         quoting=quoting,
                                         escapechar=escapechar,
                                         parse_dates=parse_dates,
                                         infer_datetime_format=infer_datetime_format,
                                         encoding=encoding,
                                         converters=converters)
                if df_full is None:
                    df_full = df
                else:
                    df_full = pd.concat(objs=[df_full, df], ignore_index=True)
            return df_full

    def _read_csv_list_iterator(
        self,
        paths: List[str],
        max_result_size=None,
        header="infer",
        names=None,
        usecols=None,
        dtype=None,
        sep=",",
        thousands=None,
        decimal=".",
        lineterminator="\n",
        quotechar='"',
        quoting=csv.QUOTE_MINIMAL,
        escapechar=None,
        parse_dates: Union[bool, Dict, List] = False,
        infer_datetime_format=False,
        encoding="utf-8",
        converters=None,
    ):
        """
        Read CSV files from AWS S3 using optimized strategies.
        Try to mimic as most as possible pandas.read_csv()
        https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.read_csv.html
        P.S. Try to mimic the chunksize behaviour in pandas.read_sql()

        :param paths: AWS S3 paths (E.g. S3://BUCKET_NAME/KEY_NAME)
        :param max_result_size: Max number of bytes on each request to S3
        :param header: Same as pandas.read_csv()
        :param names: Same as pandas.read_csv()
        :param usecols: Same as pandas.read_csv()
        :param dtype: Same as pandas.read_csv()
        :param sep: Same as pandas.read_csv()
        :param thousands: Same as pandas.read_csv()
        :param decimal: Same as pandas.read_csv()
        :param lineterminator: Same as pandas.read_csv()
        :param quotechar: Same as pandas.read_csv()
        :param quoting: Same as pandas.read_csv()
        :param escapechar: Same as pandas.read_csv()
        :param parse_dates: Same as pandas.read_csv()
        :param infer_datetime_format: Same as pandas.read_csv()
        :param encoding: Same as pandas.read_csv()
        :param converters: Same as pandas.read_csv()
        :return: Iterator of iterators of Pandas Dataframes
        """
        for path in paths:
            logger.debug(f"path: {path}")
            bucket_name, key_path = Pandas._parse_path(path)
            yield from self._read_csv_iterator(bucket_name=bucket_name,
                                               key_path=key_path,
                                               max_result_size=max_result_size,
                                               header=header,
                                               names=names,
                                               usecols=usecols,
                                               dtype=dtype,
                                               sep=sep,
                                               thousands=thousands,
                                               decimal=decimal,
                                               lineterminator=lineterminator,
                                               quotechar=quotechar,
                                               quoting=quoting,
                                               escapechar=escapechar,
                                               parse_dates=parse_dates,
                                               infer_datetime_format=infer_datetime_format,
                                               encoding=encoding,
                                               converters=converters)

    def read_csv_prefix(
            self,
            path_prefix: str,
            max_result_size=None,
            header: Optional[str] = "infer",
            names=None,
            usecols=None,
            dtype=None,
            sep=",",
            thousands=None,
            decimal=".",
            lineterminator="\n",
            quotechar='"',
            quoting=csv.QUOTE_MINIMAL,
            escapechar=None,
            parse_dates: Union[bool, Dict, List] = False,
            infer_datetime_format=False,
            encoding="utf-8",
            converters=None,
    ) -> Union[pd.DataFrame, Iterator[pd.DataFrame]]:
        """
        Read CSV files from AWS S3 PREFIX using optimized strategies.
        Try to mimic as most as possible pandas.read_csv()
        https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.read_csv.html
        P.S. max_result_size != None tries to mimic the chunksize behaviour in pandas.read_sql()

        :param path_prefix: AWS S3 path prefix (E.g. S3://BUCKET_NAME/PREFIX)
        :param max_result_size: Max number of bytes on each request to S3
        :param header: Same as pandas.read_csv()
        :param names: Same as pandas.read_csv()
        :param usecols: Same as pandas.read_csv()
        :param dtype: Same as pandas.read_csv()
        :param sep: Same as pandas.read_csv()
        :param thousands: Same as pandas.read_csv()
        :param decimal: Same as pandas.read_csv()
        :param lineterminator: Same as pandas.read_csv()
        :param quotechar: Same as pandas.read_csv()
        :param quoting: Same as pandas.read_csv()
        :param escapechar: Same as pandas.read_csv()
        :param parse_dates: Same as pandas.read_csv()
        :param infer_datetime_format: Same as pandas.read_csv()
        :param encoding: Same as pandas.read_csv()
        :param converters: Same as pandas.read_csv()
        :return: Pandas Dataframe or Iterator of Pandas Dataframes if max_result_size != None
        """
        paths: List[str] = self._session.s3.list_objects(path=path_prefix)
        paths = [p for p in paths if not p.endswith("/")]
        return self.read_csv_list(paths=paths,
                                  max_result_size=max_result_size,
                                  header=header,
                                  names=names,
                                  usecols=usecols,
                                  dtype=dtype,
                                  sep=sep,
                                  thousands=thousands,
                                  decimal=decimal,
                                  lineterminator=lineterminator,
                                  quotechar=quotechar,
                                  quoting=quoting,
                                  escapechar=escapechar,
                                  parse_dates=parse_dates,
                                  infer_datetime_format=infer_datetime_format,
                                  encoding=encoding,
                                  converters=converters)
