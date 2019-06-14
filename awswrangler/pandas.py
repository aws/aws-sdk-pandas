from io import BytesIO
import multiprocessing as mp

import pandas
import pyarrow
from pyarrow import parquet

from awswrangler.exceptions import UnsupportedWriteMode, UnsupportedFileFormat
from awswrangler.utils import calculate_bounders
from awswrangler import s3


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
        bucket_name, key_path = self._parse_path(path)
        s3_client = self._session.boto3_session.client("s3", use_ssl=True)
        buff = BytesIO()
        s3_client.download_fileobj(bucket_name, key_path, buff)
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

    def read_sql_athena(self, sql, database, s3_output=None):
        if not s3_output:
            account_id = (
                self._session.boto3_session.client("sts")
                .get_caller_identity()
                .get("Account")
            )
            session_region = self._session.boto3_session.region_name
            s3_output = f"s3://aws-athena-query-results-{account_id}-{session_region}/"
            s3_resource = self._session.boto3_session.resource("s3")
            s3_resource.Bucket(s3_output)
        query_execution_id = self._session.athena.run_query(sql, database, s3_output)
        query_response = self._session.athena.wait_query(
            query_execution_id=query_execution_id
        )
        if query_response.get("QueryExecution").get("Status").get("State") == "FAILED":
            reason = (
                query_response.get("QueryExecution")
                .get("Status")
                .get("StateChangeReason")
            )
            message_error = f"Query error: {reason}"
            raise Exception(message_error)
        else:
            path = f"{s3_output}{query_execution_id}.csv"
            dataframe = self.read_csv(path=path)
        return dataframe

    def to_csv(
        self,
        dataframe,
        path,
        database=None,
        table=None,
        partition_cols=None,
        preserve_index=True,
        mode="append",
        num_procs=None,
        num_files=1,
    ):
        self.to_s3(
            dataframe=dataframe,
            path=path,
            file_format="csv",
            database=database,
            table=table,
            partition_cols=partition_cols,
            preserve_index=preserve_index,
            mode=mode,
            num_procs=num_procs,
            num_files=num_files,
        )

    def to_parquet(
        self,
        dataframe,
        path,
        database=None,
        table=None,
        partition_cols=None,
        preserve_index=True,
        mode="append",
        num_procs=None,
        num_files=1,
    ):
        self.to_s3(
            dataframe=dataframe,
            path=path,
            file_format="parquet",
            database=database,
            table=table,
            partition_cols=partition_cols,
            preserve_index=preserve_index,
            mode=mode,
            num_procs=num_procs,
            num_files=num_files,
        )

    def to_s3(
        self,
        dataframe,
        path,
        file_format,
        database=None,
        table=None,
        partition_cols=None,
        preserve_index=True,
        mode="append",
        num_procs=None,
        num_files=1,
    ):
        if not partition_cols:
            partition_cols = []
        if mode == "overwrite" or (
            mode == "overwrite_partitions" and not partition_cols
        ):
            self._session.s3.delete_objects(path)
        elif mode not in ["overwrite_partitions", "append"]:
            raise UnsupportedWriteMode(mode)
        partition_paths = self.data_to_s3(
            dataframe=dataframe,
            path=path,
            partition_cols=partition_cols,
            preserve_index=preserve_index,
            file_format=file_format,
            mode=mode,
            num_procs=num_procs,
            num_files=num_files,
        )
        if database:
            self._session.glue.metadata_to_glue(
                dataframe=dataframe,
                path=path,
                partition_paths=partition_paths,
                database=database,
                table=table,
                partition_cols=partition_cols,
                preserve_index=preserve_index,
                file_format=file_format,
                mode=mode,
            )

    def data_to_s3(
        self,
        dataframe,
        path,
        file_format,
        partition_cols=None,
        preserve_index=True,
        mode="append",
        num_procs=None,
        num_files=1,
    ):
        if not num_procs:
            num_procs = self._session.cpu_count
        if path[-1] == "/":
            path = path[:-1]
        file_format = file_format.lower()
        if file_format not in ["parquet", "csv"]:
            raise UnsupportedFileFormat(file_format)
        partition_paths = None
        if partition_cols is not None and len(partition_cols) > 0:
            partition_paths = self._data_to_s3_dataset_manager(
                dataframe=dataframe,
                path=path,
                partition_cols=partition_cols,
                preserve_index=preserve_index,
                file_format=file_format,
                mode=mode,
                num_procs=num_procs,
                num_files=num_files,
            )
        else:
            self._data_to_s3_files_manager(
                dataframe=dataframe,
                path=path,
                preserve_index=preserve_index,
                file_format=file_format,
                num_procs=num_procs,
                num_files=num_files,
            )
        return partition_paths

    def _data_to_s3_files_manager(
        self, dataframe, path, preserve_index, file_format, num_procs, num_files=2
    ):
        if num_procs > 1:
            bounders = _get_bounders(
                dataframe=dataframe, num_partitions=num_procs * num_files
            )
            procs = []
            for counter in range(num_files):
                for bounder in bounders[
                    counter * num_procs : (counter * num_procs) + num_procs
                ]:
                    proc = mp.Process(
                        target=Pandas._data_to_s3_files_writer,
                        args=(
                            dataframe.iloc[bounder[0] : bounder[1], :],
                            path,
                            preserve_index,
                            self._session.primitives,
                            file_format,
                        ),
                    )
                    proc.daemon = True
                    proc.start()
                    procs.append(proc)
                    procs = []
            for i in range(len(procs)):
                procs[i].join()
        else:
            Pandas._data_to_s3_files_writer(
                dataframe=dataframe,
                path=path,
                preserve_index=preserve_index,
                session_primitives=self._session.primitives,
                file_format=file_format,
            )

    def _data_to_s3_dataset_manager(
        self,
        dataframe,
        path,
        partition_cols,
        preserve_index,
        file_format,
        mode,
        num_procs,
        num_files=1,
    ):
        partition_paths = []
        if num_procs > 1:
            bounders = _get_bounders(
                dataframe=dataframe, num_partitions=num_procs * num_files
            )
            for counter in range(num_files):
                procs = []
                receive_pipes = []
                for bounder in bounders[
                    counter * num_procs : (counter * num_procs) + num_procs
                ]:
                    receive_pipe, send_pipe = mp.Pipe()
                    proc = mp.Process(
                        target=self._data_to_s3_dataset_writer_remote,
                        args=(
                            send_pipe,
                            dataframe.iloc[bounder[0] : bounder[1], :],
                            path,
                            partition_cols,
                            preserve_index,
                            self._session.primitives,
                            file_format,
                            mode,
                        ),
                    )
                    proc.daemon = True
                    proc.start()
                    procs.append(proc)
                    receive_pipes.append(receive_pipe)
                for i in range(len(procs)):
                    partition_paths += receive_pipes[i].recv()
                    procs[i].join()
                    receive_pipes[i].close()
        else:
            partition_paths += self._data_to_s3_dataset_writer(
                dataframe=dataframe,
                path=path,
                partition_cols=partition_cols,
                preserve_index=preserve_index,
                session_primitives=self._session.primitives,
                file_format=file_format,
                mode=mode,
            )
        return partition_paths

    @staticmethod
    def _data_to_s3_dataset_writer(
        dataframe,
        path,
        partition_cols,
        preserve_index,
        session_primitives,
        file_format,
        mode,
    ):
        session = session_primitives.session
        partition_paths = []
        dead_keys = []
        for keys, subgroup in dataframe.groupby(partition_cols):
            subgroup = subgroup.drop(partition_cols, axis="columns")
            if not isinstance(keys, tuple):
                keys = (keys,)
            subdir = "/".join(
                [f"{name}={val}" for name, val in zip(partition_cols, keys)]
            )
            prefix = "/".join([path, subdir])
            if mode == "overwrite_partitions":
                dead_keys += session.s3.list_objects(path=prefix)
            full_path = Pandas._data_to_s3_files_writer(
                dataframe=subgroup,
                path=prefix,
                preserve_index=preserve_index,
                session_primitives=session_primitives,
                file_format=file_format,
            )
            partition_path = full_path.rpartition("/")[0] + "/"
            keys_str = [str(x) for x in keys]
            partition_paths.append((partition_path, keys_str))
        if mode == "overwrite_partitions" and dead_keys:
            bucket = path.replace("s3://", "").split("/", 1)[0]
            session.s3.delete_listed_objects(bucket=bucket, batch=dead_keys)
        return partition_paths

    @staticmethod
    def _data_to_s3_dataset_writer_remote(
        send_pipe,
        dataframe,
        path,
        partition_cols,
        preserve_index,
        session_primitives,
        file_format,
        mode,
    ):
        send_pipe.send(
            Pandas._data_to_s3_dataset_writer(
                dataframe=dataframe,
                path=path,
                partition_cols=partition_cols,
                preserve_index=preserve_index,
                session_primitives=session_primitives,
                file_format=file_format,
                mode=mode,
            )
        )
        send_pipe.close()

    @staticmethod
    def _data_to_s3_files_writer(
        dataframe, path, preserve_index, session_primitives, file_format
    ):
        fs = s3.get_fs(session_primitives=session_primitives)
        fs = pyarrow.filesystem._ensure_filesystem(fs)
        s3.mkdir_if_not_exists(fs, path)
        if file_format == "parquet":
            outfile = pyarrow.compat.guid() + ".parquet"
        elif file_format == "csv":
            outfile = pyarrow.compat.guid() + ".csv"
        else:
            raise UnsupportedFileFormat(file_format)
        full_path = "/".join([path, outfile])
        if file_format == "parquet":
            Pandas.write_parquet_dataframe(
                dataframe=dataframe,
                path=full_path,
                preserve_index=preserve_index,
                fs=fs,
            )
        elif file_format == "csv":
            Pandas.write_csv_dataframe(
                dataframe=dataframe,
                path=full_path,
                preserve_index=preserve_index,
                fs=fs,
            )
        return full_path

    @staticmethod
    def write_csv_dataframe(dataframe, path, preserve_index, fs):
        csv_buffer = bytes(
            dataframe.to_csv(None, header=False, index=preserve_index), "utf-8"
        )
        with fs.open(path, "wb") as f:
            f.write(csv_buffer)

    @staticmethod
    def write_parquet_dataframe(dataframe, path, preserve_index, fs):
        table = pyarrow.Table.from_pandas(
            dataframe, preserve_index=preserve_index, safe=False
        )
        with fs.open(path, "wb") as f:
            parquet.write_table(table, f, coerce_timestamps="ms")
