from io import BytesIO, StringIO
import multiprocessing as mp
import logging
from math import floor
import copy
import csv

import pandas
import pyarrow
from pyarrow import parquet

from awswrangler.exceptions import UnsupportedWriteMode, UnsupportedFileFormat, AthenaQueryError, EmptyS3Object, LineTerminatorNotFound
from awswrangler.utils import calculate_bounders
from awswrangler import s3

logger = logging.getLogger(__name__)

MIN_NUMBER_OF_ROWS_TO_DISTRIBUTE = 1000


def _get_bounders(dataframe, num_partitions):
    num_rows = len(dataframe.index)
    return calculate_bounders(num_items=num_rows, num_groups=num_partitions)


class Pandas:
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
            dtype=None,
            sep=",",
            lineterminator="\n",
            quotechar='"',
            quoting=csv.QUOTE_MINIMAL,
            escapechar=None,
            parse_dates=False,
            infer_datetime_format=False,
            encoding="utf-8",
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
        :param dtype: Same as pandas.read_csv()
        :param sep: Same as pandas.read_csv()
        :param lineterminator: Same as pandas.read_csv()
        :param quotechar: Same as pandas.read_csv()
        :param quoting: Same as pandas.read_csv()
        :param escapechar: Same as pandas.read_csv()
        :param parse_dates: Same as pandas.read_csv()
        :param infer_datetime_format: Same as pandas.read_csv()
        :param encoding: Same as pandas.read_csv()
        :return: Pandas Dataframe or Iterator of Pandas Dataframes if max_result_size != None
        """
        bucket_name, key_path = self._parse_path(path)
        client_s3 = self._session.boto3_session.client(
            service_name="s3",
            use_ssl=True,
            config=self._session.botocore_config)
        if max_result_size:
            ret = Pandas._read_csv_iterator(
                client_s3=client_s3,
                bucket_name=bucket_name,
                key_path=key_path,
                max_result_size=max_result_size,
                header=header,
                names=names,
                dtype=dtype,
                sep=sep,
                lineterminator=lineterminator,
                quotechar=quotechar,
                quoting=quoting,
                escapechar=escapechar,
                parse_dates=parse_dates,
                infer_datetime_format=infer_datetime_format,
                encoding=encoding)
        else:
            ret = Pandas._read_csv_once(
                client_s3=client_s3,
                bucket_name=bucket_name,
                key_path=key_path,
                header=header,
                names=names,
                dtype=dtype,
                sep=sep,
                lineterminator=lineterminator,
                quotechar=quotechar,
                quoting=quoting,
                escapechar=escapechar,
                parse_dates=parse_dates,
                infer_datetime_format=infer_datetime_format,
                encoding=encoding)
        return ret

    @staticmethod
    def _read_csv_iterator(
            client_s3,
            bucket_name,
            key_path,
            max_result_size=200_000_000,  # 200 MB
            header="infer",
            names=None,
            dtype=None,
            sep=",",
            lineterminator="\n",
            quotechar='"',
            quoting=csv.QUOTE_MINIMAL,
            escapechar=None,
            parse_dates=False,
            infer_datetime_format=False,
            encoding="utf-8",
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
        :param dtype: Same as pandas.read_csv()
        :param sep: Same as pandas.read_csv()
        :param lineterminator: Same as pandas.read_csv()
        :param quotechar: Same as pandas.read_csv()
        :param quoting: Same as pandas.read_csv()
        :param escapechar: Same as pandas.read_csv()
        :param parse_dates: Same as pandas.read_csv()
        :param infer_datetime_format: Same as pandas.read_csv()
        :param encoding: Same as pandas.read_csv()
        :return: Pandas Dataframe
        """
        metadata = s3.S3.head_object_with_retry(client=client_s3,
                                                bucket=bucket_name,
                                                key=key_path)
        logger.debug(f"metadata: {metadata}")
        total_size = metadata["ContentLength"]
        logger.debug(f"total_size: {total_size}")
        if total_size <= 0:
            raise EmptyS3Object(metadata)
        elif total_size <= max_result_size:
            yield Pandas._read_csv_once(
                client_s3=client_s3,
                bucket_name=bucket_name,
                key_path=key_path,
                header=header,
                names=names,
                dtype=dtype,
                sep=sep,
                lineterminator=lineterminator,
                quotechar=quotechar,
                quoting=quoting,
                escapechar=escapechar,
                parse_dates=parse_dates,
                infer_datetime_format=infer_datetime_format,
                encoding=encoding)
        else:
            bounders = calculate_bounders(num_items=total_size,
                                          max_size=max_result_size)
            logger.debug(f"bounders: {bounders}")
            bounders_len = len(bounders)
            count = 0
            forgotten_bytes = 0
            for ini, end in bounders:
                count += 1

                ini -= forgotten_bytes
                end -= 1  # Range is inclusive, contrary to Python's List
                bytes_range = "bytes={}-{}".format(ini, end)
                logger.debug(f"bytes_range: {bytes_range}")
                body = client_s3.get_object(Bucket=bucket_name,
                                            Key=key_path,
                                            Range=bytes_range)["Body"].read()
                chunk_size = len(body)
                logger.debug(f"chunk_size (bytes): {chunk_size}")

                if count == 1:  # first chunk
                    last_char = Pandas._find_terminator(
                        body=body,
                        sep=sep,
                        quoting=quoting,
                        quotechar=quotechar,
                        lineterminator=lineterminator)
                    forgotten_bytes = len(body[last_char:])
                elif count == bounders_len:  # Last chunk
                    last_char = chunk_size
                else:
                    last_char = Pandas._find_terminator(
                        body=body,
                        sep=sep,
                        quoting=quoting,
                        quotechar=quotechar,
                        lineterminator=lineterminator)
                    forgotten_bytes = len(body[last_char:])

                df = pandas.read_csv(
                    StringIO(body[:last_char].decode("utf-8")),
                    header=header,
                    names=names,
                    sep=sep,
                    quotechar=quotechar,
                    quoting=quoting,
                    escapechar=escapechar,
                    parse_dates=parse_dates,
                    infer_datetime_format=infer_datetime_format,
                    lineterminator=lineterminator,
                    dtype=dtype,
                    encoding=encoding,
                )
                yield df
                if count == 1:  # first chunk
                    names = df.columns
                    header = None

    @staticmethod
    def _extract_terminator_profile(body, sep, quotechar, lineterminator,
                                    last_index):
        """
        Backward parser for quoted CSV lines
        :param body: String
        :param sep: Same as pandas.read_csv()
        :param quotechar: Same as pandas.read_csv()
        :param lineterminator: Same as pandas.read_csv()
        :return: Dict with the profile
        """
        sep_int = int.from_bytes(bytes=sep.encode(encoding="utf-8"),
                                 byteorder="big")  # b"," -> 44
        quote_int = int.from_bytes(bytes=quotechar.encode(encoding="utf-8"),
                                   byteorder="big")  # b'"' -> 34
        terminator_int = int.from_bytes(
            bytes=lineterminator.encode(encoding="utf-8"),
            byteorder="big")  # b"\n" -> 10
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
        logger.debug(
            f"last_terminator_suspect_index: {last_terminator_suspect_index}")
        logger.debug(
            f"first_non_special_byte_index: {first_non_special_byte_index}")
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
                    profile = Pandas._extract_terminator_profile(
                        body=body,
                        sep=sep,
                        quotechar=quotechar,
                        lineterminator=lineterminator,
                        last_index=last_index)
                    if profile["last_terminator_suspect_index"] and profile[
                            "first_non_special_byte_index"]:
                        if profile["quote_counter"] % 2 == 0 or profile[
                                "quote_counter"] == 0:
                            last_index = profile[
                                "last_terminator_suspect_index"]
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
            dtype=None,
            sep=",",
            lineterminator="\n",
            quotechar='"',
            quoting=0,
            escapechar=None,
            parse_dates=False,
            infer_datetime_format=False,
            encoding=None,
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
        :param dtype: Same as pandas.read_csv()
        :param sep: Same as pandas.read_csv()
        :param lineterminator: Same as pandas.read_csv()
        :param quotechar: Same as pandas.read_csv()
        :param quoting: Same as pandas.read_csv()
        :param escapechar: Same as pandas.read_csv()
        :param parse_dates: Same as pandas.read_csv()
        :param infer_datetime_format: Same as pandas.read_csv()
        :param encoding: Same as pandas.read_csv()
        :return: Pandas Dataframe
        """
        buff = BytesIO()
        client_s3.download_fileobj(Bucket=bucket_name,
                                   Key=key_path,
                                   Fileobj=buff)
        buff.seek(0),
        dataframe = pandas.read_csv(
            buff,
            header=header,
            names=names,
            sep=sep,
            quotechar=quotechar,
            quoting=quoting,
            escapechar=escapechar,
            parse_dates=parse_dates,
            infer_datetime_format=infer_datetime_format,
            lineterminator=lineterminator,
            dtype=dtype,
            encoding=encoding,
        )
        buff.close()
        return dataframe

    def read_sql_athena(self,
                        sql,
                        database,
                        s3_output=None,
                        max_result_size=None):
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
            account_id = (self._session.boto3_session.client(
                service_name="sts", config=self._session.botocore_config).
                          get_caller_identity().get("Account"))
            session_region = self._session.boto3_session.region_name
            s3_output = f"s3://aws-athena-query-results-{account_id}-{session_region}/"
            s3_resource = self._session.boto3_session.resource("s3")
            s3_resource.Bucket(s3_output)
        query_execution_id = self._session.athena.run_query(
            query=sql, database=database, s3_output=s3_output)
        query_response = self._session.athena.wait_query(
            query_execution_id=query_execution_id)
        if query_response.get("QueryExecution").get("Status").get("State") in [
                "FAILED", "CANCELLED"
        ]:
            reason = (query_response.get("QueryExecution").get("Status").get(
                "StateChangeReason"))
            message_error = f"Query error: {reason}"
            raise AthenaQueryError(message_error)
        else:
            dtype, parse_dates = self._session.athena.get_query_dtype(
                query_execution_id=query_execution_id)
            path = f"{s3_output}{query_execution_id}.csv"
            ret = self.read_csv(path=path,
                                dtype=dtype,
                                parse_dates=parse_dates,
                                quoting=csv.QUOTE_ALL,
                                max_result_size=max_result_size)
        return ret

    def to_csv(
            self,
            dataframe,
            path,
            database=None,
            table=None,
            partition_cols=None,
            preserve_index=True,
            mode="append",
            procs_cpu_bound=None,
            procs_io_bound=None,
    ):
        """
        Write a Pandas Dataframe as CSV files on S3
        Optionally writes metadata on AWS Glue.
        :param dataframe: Pandas Dataframe
        :param path: AWS S3 path (E.g. s3://bucket-name/folder_name/
        :param database: AWS Glue Database name
        :param table: AWS Glue table name
        :param partition_cols: List of columns names that will be partitions on S3
        :param preserve_index: Should preserve index on S3?
        :param mode: "append", "overwrite", "overwrite_partitions"
        :param procs_cpu_bound: Number of cores used for CPU bound tasks
        :param procs_io_bound: Number of cores used for I/O bound tasks
        :return: List of objects written on S3
        """
        return self.to_s3(
            dataframe=dataframe,
            path=path,
            file_format="csv",
            database=database,
            table=table,
            partition_cols=partition_cols,
            preserve_index=preserve_index,
            mode=mode,
            procs_cpu_bound=procs_cpu_bound,
            procs_io_bound=procs_io_bound,
        )

    def to_parquet(self,
                   dataframe,
                   path,
                   database=None,
                   table=None,
                   partition_cols=None,
                   preserve_index=True,
                   mode="append",
                   procs_cpu_bound=None,
                   procs_io_bound=None,
                   cast_columns=None):
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
        :param procs_cpu_bound: Number of cores used for CPU bound tasks
        :param procs_io_bound: Number of cores used for I/O bound tasks
        :param cast_columns: Dictionary of columns names and Arrow types to be casted.
               E.g. {"col name": "int64", "col2 name": "int32"}
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
                          procs_cpu_bound=procs_cpu_bound,
                          procs_io_bound=procs_io_bound,
                          cast_columns=cast_columns)

    def to_s3(self,
              dataframe,
              path,
              file_format,
              database=None,
              table=None,
              partition_cols=None,
              preserve_index=True,
              mode="append",
              procs_cpu_bound=None,
              procs_io_bound=None,
              cast_columns=None):
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
        :param procs_cpu_bound: Number of cores used for CPU bound tasks
        :param procs_io_bound: Number of cores used for I/O bound tasks
        :param cast_columns: Dictionary of columns indexes and Arrow types to be casted.
               E.g. {2: "int64", 5: "int32"}
               Only for "parquet" file_format
        :return: List of objects written on S3
        """
        if not partition_cols:
            partition_cols = []
        if mode == "overwrite" or (mode == "overwrite_partitions"
                                   and not partition_cols):
            self._session.s3.delete_objects(path=path)
        elif mode not in ["overwrite_partitions", "append"]:
            raise UnsupportedWriteMode(mode)
        objects_paths = self.data_to_s3(dataframe=dataframe,
                                        path=path,
                                        partition_cols=partition_cols,
                                        preserve_index=preserve_index,
                                        file_format=file_format,
                                        mode=mode,
                                        procs_cpu_bound=procs_cpu_bound,
                                        procs_io_bound=procs_io_bound,
                                        cast_columns=cast_columns)
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
                                                cast_columns=cast_columns)
        return objects_paths

    def data_to_s3(self,
                   dataframe,
                   path,
                   file_format,
                   partition_cols=None,
                   preserve_index=True,
                   mode="append",
                   procs_cpu_bound=None,
                   procs_io_bound=None,
                   cast_columns=None):
        if not procs_cpu_bound:
            procs_cpu_bound = self._session.procs_cpu_bound
        if not procs_io_bound:
            procs_io_bound = self._session.procs_io_bound
        logger.debug(f"procs_cpu_bound: {procs_cpu_bound}")
        logger.debug(f"procs_io_bound: {procs_io_bound}")
        if path[-1] == "/":
            path = path[:-1]
        file_format = file_format.lower()
        if file_format not in ["parquet", "csv"]:
            raise UnsupportedFileFormat(file_format)
        objects_paths = []
        if procs_cpu_bound > 1:
            bounders = _get_bounders(dataframe=dataframe,
                                     num_partitions=procs_cpu_bound)
            procs = []
            receive_pipes = []
            for bounder in bounders:
                receive_pipe, send_pipe = mp.Pipe()
                proc = mp.Process(
                    target=self._data_to_s3_dataset_writer_remote,
                    args=(send_pipe, dataframe.iloc[bounder[0]:bounder[1], :],
                          path, partition_cols, preserve_index,
                          self._session.primitives, file_format, cast_columns),
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
            objects_paths += self._data_to_s3_dataset_writer(
                dataframe=dataframe,
                path=path,
                partition_cols=partition_cols,
                preserve_index=preserve_index,
                session_primitives=self._session.primitives,
                file_format=file_format,
                cast_columns=cast_columns)
        if mode == "overwrite_partitions" and partition_cols:
            if procs_io_bound > procs_cpu_bound:
                num_procs = floor(
                    float(procs_io_bound) / float(procs_cpu_bound))
            else:
                num_procs = 1
            logger.debug(
                f"num_procs for delete_not_listed_objects: {num_procs}")
            self._session.s3.delete_not_listed_objects(
                objects_paths=objects_paths, procs_io_bound=num_procs)
        return objects_paths

    @staticmethod
    def _data_to_s3_dataset_writer(dataframe,
                                   path,
                                   partition_cols,
                                   preserve_index,
                                   session_primitives,
                                   file_format,
                                   cast_columns=None):
        objects_paths = []
        if not partition_cols:
            object_path = Pandas._data_to_s3_object_writer(
                dataframe=dataframe,
                path=path,
                preserve_index=preserve_index,
                session_primitives=session_primitives,
                file_format=file_format,
                cast_columns=cast_columns)
            objects_paths.append(object_path)
        else:
            for keys, subgroup in dataframe.groupby(partition_cols):
                subgroup = subgroup.drop(partition_cols, axis="columns")
                if not isinstance(keys, tuple):
                    keys = (keys, )
                subdir = "/".join([
                    f"{name}={val}" for name, val in zip(partition_cols, keys)
                ])
                prefix = "/".join([path, subdir])
                object_path = Pandas._data_to_s3_object_writer(
                    dataframe=subgroup,
                    path=prefix,
                    preserve_index=preserve_index,
                    session_primitives=session_primitives,
                    file_format=file_format,
                    cast_columns=cast_columns)
                objects_paths.append(object_path)
        return objects_paths

    @staticmethod
    def _data_to_s3_dataset_writer_remote(
            send_pipe,
            dataframe,
            path,
            partition_cols,
            preserve_index,
            session_primitives,
            file_format,
            cast_columns=None,
    ):
        send_pipe.send(
            Pandas._data_to_s3_dataset_writer(
                dataframe=dataframe,
                path=path,
                partition_cols=partition_cols,
                preserve_index=preserve_index,
                session_primitives=session_primitives,
                file_format=file_format,
                cast_columns=cast_columns))
        send_pipe.close()

    @staticmethod
    def _data_to_s3_object_writer(dataframe,
                                  path,
                                  preserve_index,
                                  session_primitives,
                                  file_format,
                                  cast_columns=None):
        fs = s3.get_fs(session_primitives=session_primitives)
        fs = pyarrow.filesystem._ensure_filesystem(fs)
        s3.mkdir_if_not_exists(fs, path)
        if file_format == "parquet":
            outfile = pyarrow.compat.guid() + ".parquet"
        elif file_format == "csv":
            outfile = pyarrow.compat.guid() + ".csv"
        else:
            raise UnsupportedFileFormat(file_format)
        object_path = "/".join([path, outfile])
        if file_format == "parquet":
            Pandas.write_parquet_dataframe(dataframe=dataframe,
                                           path=object_path,
                                           preserve_index=preserve_index,
                                           fs=fs,
                                           cast_columns=cast_columns)
        elif file_format == "csv":
            Pandas.write_csv_dataframe(
                dataframe=dataframe,
                path=object_path,
                preserve_index=preserve_index,
                fs=fs,
            )
        return object_path

    @staticmethod
    def write_csv_dataframe(dataframe, path, preserve_index, fs):
        csv_buffer = bytes(
            dataframe.to_csv(None, header=False, index=preserve_index),
            "utf-8")
        with fs.open(path, "wb") as f:
            f.write(csv_buffer)

    @staticmethod
    def write_parquet_dataframe(dataframe, path, preserve_index, fs,
                                cast_columns):
        if not cast_columns:
            cast_columns = {}
        casted_in_pandas = []
        dtypes = copy.deepcopy(dataframe.dtypes.to_dict())
        for name, dtype in dtypes.items():
            if str(dtype) == "Int64":
                dataframe[name] = dataframe[name].astype("float64")
                casted_in_pandas.append(name)
                cast_columns[name] = "int64"
                logger.debug(f"Casting column {name} Int64 to float64")
        table = pyarrow.Table.from_pandas(df=dataframe,
                                          preserve_index=preserve_index,
                                          safe=False)
        if cast_columns:
            for col_name, dtype in cast_columns.items():
                col_index = table.column_names.index(col_name)
                table = table.set_column(col_index,
                                         table.column(col_name).cast(dtype))
                logger.debug(
                    f"Casting column {col_name} ({col_index}) to {dtype}")
        with fs.open(path, "wb") as f:
            parquet.write_table(table,
                                f,
                                coerce_timestamps="ms",
                                flavor="spark")
        for col in casted_in_pandas:
            dataframe[col] = dataframe[col].astype("Int64")

    def to_redshift(
            self,
            dataframe,
            path,
            connection,
            schema,
            table,
            iam_role,
            preserve_index=False,
            mode="append",
    ):
        self._session.s3.delete_objects(path=path)
        num_slices = self._session.redshift.get_number_of_slices(
            redshift_conn=connection)
        logger.debug(f"Number of slices on Redshift: {num_slices}")
        num_rows = len(dataframe.index)
        logger.info(f"Number of rows: {num_rows}")
        if num_rows < MIN_NUMBER_OF_ROWS_TO_DISTRIBUTE:
            num_partitions = 1
        else:
            num_partitions = num_slices
        logger.debug(f"Number of partitions calculated: {num_partitions}")
        objects_paths = self.to_parquet(
            dataframe=dataframe,
            path=path,
            preserve_index=preserve_index,
            mode="append",
            procs_cpu_bound=num_partitions,
        )
        if path[-1] != "/":
            path += "/"
        manifest_path = f"{path}manifest.json"
        self._session.redshift.write_load_manifest(manifest_path=manifest_path,
                                                   objects_paths=objects_paths)
        self._session.redshift.load_table(
            dataframe=dataframe,
            dataframe_type="pandas",
            manifest_path=manifest_path,
            schema_name=schema,
            table_name=table,
            redshift_conn=connection,
            preserve_index=False,
            num_files=num_partitions,
            iam_role=iam_role,
            mode=mode,
        )
        self._session.s3.delete_objects(path=path)
