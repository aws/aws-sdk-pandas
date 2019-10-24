from typing import Dict, List, Tuple, Optional, Any
from io import BytesIO, StringIO
import multiprocessing as mp
import logging
from math import floor
import copy
import csv
from datetime import datetime

import pandas as pd  # type: ignore
import pyarrow as pa  # type: ignore
from pyarrow import parquet as pq  # type: ignore

from awswrangler import data_types
from awswrangler.exceptions import (UnsupportedWriteMode, UnsupportedFileFormat, AthenaQueryError, EmptyS3Object,
                                    LineTerminatorNotFound, EmptyDataframe, InvalidSerDe, InvalidCompression)
from awswrangler.utils import calculate_bounders
from awswrangler import s3
from awswrangler.athena import Athena

logger = logging.getLogger(__name__)

MIN_NUMBER_OF_ROWS_TO_DISTRIBUTE = 1000


def _get_bounders(dataframe, num_partitions):
    num_rows = len(dataframe.index)
    return calculate_bounders(num_items=num_rows, num_groups=num_partitions)


class Pandas:

    VALID_CSV_SERDES = ["OpenCSVSerDe", "LazySimpleSerDe"]
    VALID_CSV_COMPRESSIONS = [None]
    VALID_PARQUET_COMPRESSIONS = [None, "snappy", "gzip"]

    def __init__(self, session):
        self._session = session

    @staticmethod
    def _parse_path(path):
        path2 = path.replace("s3://", "")
        parts = path2.partition("/")
        return parts[0], parts[2]

    def read_csv(
            self,
            path,
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
            parse_dates=False,
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
        client_s3 = self._session.boto3_session.client(service_name="s3",
                                                       use_ssl=True,
                                                       config=self._session.botocore_config)
        if max_result_size:
            ret = Pandas._read_csv_iterator(client_s3=client_s3,
                                            bucket_name=bucket_name,
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
            ret = Pandas._read_csv_once(client_s3=client_s3,
                                        bucket_name=bucket_name,
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

    @staticmethod
    def _read_csv_iterator(
            client_s3,
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
            parse_dates=False,
            infer_datetime_format=False,
            encoding="utf-8",
            converters=None,
    ):
        """
        Read CSV file from AWS S3 using optimized strategies.
        Try to mimic as most as possible pandas.read_csv()
        https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.read_csv.html

        :param client_s3: Boto3 S3 client object
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
        metadata = s3.S3.head_object_with_retry(client=client_s3, bucket=bucket_name, key=key_path)
        logger.debug(f"metadata: {metadata}")
        total_size = metadata["ContentLength"]
        logger.debug(f"total_size: {total_size}")
        if total_size <= 0:
            raise EmptyS3Object(metadata)
        elif total_size <= max_result_size:
            yield Pandas._read_csv_once(client_s3=client_s3,
                                        bucket_name=bucket_name,
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
            bounders_len = len(bounders)
            count = 0
            forgotten_bytes = 0
            for ini, end in bounders:
                count += 1

                ini -= forgotten_bytes
                end -= 1  # Range is inclusive, contrary from Python's List
                bytes_range = "bytes={}-{}".format(ini, end)
                logger.debug(f"bytes_range: {bytes_range}")
                body = client_s3.get_object(Bucket=bucket_name, Key=key_path, Range=bytes_range)["Body"].read()
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

    @staticmethod
    def _read_csv_once(
            client_s3,
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
            parse_dates=False,
            infer_datetime_format=False,
            encoding=None,
            converters=None,
    ):
        """
        Read CSV file from AWS S3 using optimized strategies.
        Try to mimic as most as possible pandas.read_csv()
        https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.read_csv.html

        :param client_s3: Boto3 S3 client object
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
        client_s3.download_fileobj(Bucket=bucket_name, Key=key_path, Fileobj=buff)
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

    def read_sql_athena(self, sql, database, s3_output=None, max_result_size=None):
        """
        Executes any SQL query on AWS Athena and return a Dataframe of the result.
        P.S. If max_result_size is passed, then a iterator of Dataframes is returned.

        :param sql: SQL Query
        :param database: Glue/Athena Database
        :param s3_output: AWS S3 path
        :param max_result_size: Max number of bytes on each request to S3
        :return: Pandas Dataframe or Iterator of Pandas Dataframes if max_result_size != None
        """
        if not s3_output:
            s3_output = self._session.athena.create_athena_bucket()
        query_execution_id = self._session.athena.run_query(query=sql, database=database, s3_output=s3_output)
        query_response = self._session.athena.wait_query(query_execution_id=query_execution_id)
        if query_response["QueryExecution"]["Status"]["State"] in ["FAILED", "CANCELLED"]:
            reason = query_response["QueryExecution"]["Status"]["StateChangeReason"]
            message_error = f"Query error: {reason}"
            raise AthenaQueryError(message_error)
        else:
            dtype, parse_timestamps, parse_dates, converters = self._session.athena.get_query_dtype(
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
                        ret[col] = ret[col].dt.date
                return ret
            else:
                return Pandas._apply_dates_to_generator(generator=ret, parse_dates=parse_dates)

    @staticmethod
    def _apply_dates_to_generator(generator, parse_dates):
        for df in generator:
            if len(df.index) > 0:
                for col in parse_dates:
                    df[col] = df[col].dt.date
            yield df

    def to_csv(
            self,
            dataframe,
            path,
            sep=",",
            serde="OpenCSVSerDe",
            database=None,
            table=None,
            partition_cols=None,
            preserve_index=True,
            mode="append",
            procs_cpu_bound=None,
            procs_io_bound=None,
            inplace=True,
    ):
        """
        Write a Pandas Dataframe as CSV files on S3
        Optionally writes metadata on AWS Glue.

        :param dataframe: Pandas Dataframe
        :param path: AWS S3 path (E.g. s3://bucket-name/folder_name/
        :param sep: Same as pandas.to_csv()
        :param serde: SerDe library name (e.g. OpenCSVSerDe, LazySimpleSerDe)
        :param database: AWS Glue Database name
        :param table: AWS Glue table name
        :param partition_cols: List of columns names that will be partitions on S3
        :param preserve_index: Should preserve index on S3?
        :param mode: "append", "overwrite", "overwrite_partitions"
        :param procs_cpu_bound: Number of cores used for CPU bound tasks
        :param procs_io_bound: Number of cores used for I/O bound tasks
        :param inplace: True is cheapest (CPU and Memory) but False leaves your DataFrame intact
        :return: List of objects written on S3
        """
        if serde not in Pandas.VALID_CSV_SERDES:
            raise InvalidSerDe(f"{serde} in not in the valid SerDe list ({Pandas.VALID_CSV_SERDES})")
        extra_args = {"sep": sep, "serde": serde}
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
                          inplace=inplace)

    def to_parquet(self,
                   dataframe,
                   path,
                   database=None,
                   table=None,
                   partition_cols=None,
                   preserve_index=True,
                   mode="append",
                   compression="snappy",
                   procs_cpu_bound=None,
                   procs_io_bound=None,
                   cast_columns=None,
                   inplace=True):
        """
        Write a Pandas Dataframe as parquet files on S3
        Optionally writes metadata on AWS Glue.

        :param dataframe: Pandas Dataframe
        :param path: AWS S3 path (E.g. s3://bucket-name/folder_name/
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
                          inplace=inplace)

    def to_s3(self,
              dataframe,
              path,
              file_format,
              database=None,
              table=None,
              partition_cols=None,
              preserve_index=True,
              mode="append",
              compression=None,
              procs_cpu_bound=None,
              procs_io_bound=None,
              cast_columns=None,
              extra_args=None,
              inplace=True):
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
        :return: List of objects written on S3
        """
        if not partition_cols:
            partition_cols = []
        if not cast_columns:
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
                                                extra_args=extra_args)
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
        if not procs_cpu_bound:
            procs_cpu_bound = self._session.procs_cpu_bound
        if not procs_io_bound:
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
    def _data_to_s3_dataset_writer(dataframe,
                                   path,
                                   partition_cols,
                                   preserve_index,
                                   compression,
                                   session_primitives,
                                   file_format,
                                   cast_columns=None,
                                   extra_args=None,
                                   isolated_dataframe=False):
        objects_paths = []
        if not partition_cols:
            object_path = Pandas._data_to_s3_object_writer(dataframe=dataframe,
                                                           path=path,
                                                           preserve_index=preserve_index,
                                                           compression=compression,
                                                           session_primitives=session_primitives,
                                                           file_format=file_format,
                                                           cast_columns=cast_columns,
                                                           extra_args=extra_args,
                                                           isolated_dataframe=isolated_dataframe)
            objects_paths.append(object_path)
        else:
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
                                                               cast_columns=cast_columns,
                                                               extra_args=extra_args,
                                                               isolated_dataframe=True)
                objects_paths.append(object_path)
        return objects_paths

    @staticmethod
    def _data_to_s3_dataset_writer_remote(send_pipe,
                                          dataframe,
                                          path,
                                          partition_cols,
                                          preserve_index,
                                          compression,
                                          session_primitives,
                                          file_format,
                                          cast_columns=None,
                                          extra_args=None):
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
    def _data_to_s3_object_writer(dataframe,
                                  path,
                                  preserve_index,
                                  compression,
                                  session_primitives,
                                  file_format,
                                  cast_columns=None,
                                  extra_args=None,
                                  isolated_dataframe=False):
        fs = s3.get_fs(session_primitives=session_primitives)
        fs = pa.filesystem._ensure_filesystem(fs)
        s3.mkdir_if_not_exists(fs, path)

        if compression is None:
            compression_end = ""
        elif compression == "snappy":
            compression_end = ".snappy"
        elif compression == "gzip":
            compression_end = ".gz"
        else:
            raise InvalidCompression(compression)

        guid = pa.compat.guid()
        if file_format == "parquet":
            outfile = f"{guid}.parquet{compression_end}"
        elif file_format == "csv":
            outfile = f"{guid}.csv{compression_end}"
        else:
            raise UnsupportedFileFormat(file_format)
        object_path = "/".join([path, outfile])
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
        if serde is not None:
            if serde == "OpenCSVSerDe":
                csv_extra_args["quoting"] = csv.QUOTE_ALL
                csv_extra_args["escapechar"] = "\\"
            elif serde == "LazySimpleSerDe":
                csv_extra_args["quoting"] = csv.QUOTE_NONE
                csv_extra_args["escapechar"] = "\\"
        csv_buffer = bytes(
            dataframe.to_csv(None, header=False, index=preserve_index, compression=compression, **csv_extra_args),
            "utf-8")
        with fs.open(path, "wb") as f:
            f.write(csv_buffer)

    @staticmethod
    def write_parquet_dataframe(dataframe, path, preserve_index, compression, fs, cast_columns, isolated_dataframe):
        if not cast_columns:
            cast_columns = {}

        # Casting on Pandas
        casted_in_pandas = []
        dtypes = copy.deepcopy(dataframe.dtypes.to_dict())
        for name, dtype in dtypes.items():
            if str(dtype) == "Int64":
                dataframe.loc[:, name] = dataframe[name].astype("float64")
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
                table = table.set_column(col_index, table.column(col_name).cast(pyarrow_dtype))
                logger.debug(f"Casting column {col_name} ({col_index}) to {dtype} ({pyarrow_dtype})")

        # Persisting on S3
        with fs.open(path, "wb") as f:
            pq.write_table(table, f, compression=compression, coerce_timestamps="ms", flavor="spark")

        # Casting back on Pandas if necessary
        if isolated_dataframe is False:
            for col in casted_in_pandas:
                dataframe[col] = dataframe[col].astype("Int64")

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
        :param preserve_index: Should we preserve the Dataframe index?
        :param mode: append or overwrite
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
        logger.warning(f"Dropping repeated columns: {list(dataframe.columns[duplicated_cols])}")
        return dataframe.loc[:, ~duplicated_cols]
