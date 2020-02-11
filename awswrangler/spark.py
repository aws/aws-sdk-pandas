"""Apache Spark Module."""

import os
from logging import Logger, getLogger
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Tuple

import pandas as pd  # type: ignore
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import PandasUDFType, pandas_udf, spark_partition_id
from pyspark.sql.types import TimestampType

from awswrangler.exceptions import MissingBatchDetected, UnsupportedFileFormat

if TYPE_CHECKING:
    from awswrangler.session import Session

logger: Logger = getLogger(__name__)

MIN_NUMBER_OF_ROWS_TO_DISTRIBUTE: int = 1000


class Spark:
    """Apache Spark Class."""
    def __init__(self, session: "Session"):
        """
        Apache Spark Class Constructor.

        Don't use it directly, call through a Session().
        e.g. wr.redshift.your_method()

        :param session: awswrangler.Session()
        """
        self._session: "Session" = session
        self._procs_io_bound: int = 1
        logger.info(f"_procs_io_bound: {self._procs_io_bound}")

    def read_csv(self, **args) -> DataFrame:
        """
        Read CSV.

        :param args: All arguments supported by spark.read.csv()
        :return: PySpark DataDataframe
        """
        spark: SparkSession = self._session.spark_session
        return spark.read.csv(**args)

    @staticmethod
    def _extract_casts(dtypes: List[Tuple[str, str]]) -> Dict[str, str]:
        casts: Dict[str, str] = {}
        name: str
        dtype: str
        for name, dtype in dtypes:
            if dtype in ["smallint", "int", "bigint"]:
                casts[name] = "bigint"
            elif dtype == "date":
                casts[name] = "date"
        logger.debug(f"casts: {casts}")
        return casts

    @staticmethod
    def date2timestamp(dataframe: DataFrame) -> DataFrame:
        """
        Convert all Date columns to Timestamp.

        :param dataframe: PySpark DataFrame
        :return: New converted DataFrame
        """
        name: str
        dtype: str
        for name, dtype in dataframe.dtypes:
            if dtype == "date":
                dataframe = dataframe.withColumn(name, dataframe[name].cast(TimestampType()))
                logger.warning(f"Casting column {name} from date to timestamp!")
        return dataframe

    def to_redshift(self,
                    dataframe: DataFrame,
                    path: str,
                    connection: Any,
                    schema: str,
                    table: str,
                    iam_role: str,
                    diststyle: str = "AUTO",
                    distkey: Optional[str] = None,
                    sortstyle: str = "COMPOUND",
                    sortkey: Optional[str] = None,
                    min_num_partitions: int = 200,
                    mode: str = "append",
                    varchar_default_length: int = 256,
                    varchar_lengths: Optional[Dict[str, int]] = None) -> None:
        """
        Load Spark Dataframe as a Table on Amazon Redshift.

        :param dataframe: Pandas Dataframe
        :param path: S3 path to write temporary files (E.g. s3://BUCKET_NAME/ANY_NAME/)
        :param connection: Glue connection name (str) OR a PEP 249 compatible connection (Can be generated with Redshift.generate_connection())
        :param schema: The Redshift Schema for the table
        :param table: The name of the desired Redshift table
        :param iam_role: AWS IAM role with the related permissions
        :param diststyle: Redshift distribution styles. Must be in ["AUTO", "EVEN", "ALL", "KEY"] (https://docs.aws.amazon.com/redshift/latest/dg/t_Distributing_data.html)
        :param distkey: Specifies a column name or positional number for the distribution key
        :param sortstyle: Sorting can be "COMPOUND" or "INTERLEAVED" (https://docs.aws.amazon.com/redshift/latest/dg/t_Sorting_data.html)
        :param sortkey: List of columns to be sorted
        :param min_num_partitions: Minimal number of partitions
        :param mode: append or overwrite
        :param varchar_default_length: The size that will be set for all VARCHAR columns not specified with varchar_lengths
        :param varchar_lengths: Dict of VARCHAR length by columns. (e.g. {"col1": 10, "col5": 200})
        :return: None
        """
        logger.debug(f"Minimum number of partitions : {min_num_partitions}")
        if path[-1] != "/":
            path += "/"
        self._session.s3.delete_objects(path=path, procs_io_bound=self._procs_io_bound)
        spark: SparkSession = self._session.spark_session
        casts: Dict[str, str] = Spark._extract_casts(dataframe.dtypes)
        dataframe = Spark.date2timestamp(dataframe)
        dataframe.cache()
        num_rows: int = dataframe.count()
        logger.info(f"Number of rows: {num_rows}")

        generated_conn: bool = False
        if type(connection) == str:
            logger.debug("Glue connection (str) provided.")
            connection = self._session.glue.get_connection(name=connection)
            generated_conn = True

        try:
            num_partitions: int
            if num_rows < MIN_NUMBER_OF_ROWS_TO_DISTRIBUTE:
                num_partitions = 1
            else:
                num_slices: int = self._session.redshift.get_number_of_slices(redshift_conn=connection)
                logger.debug(f"Number of slices on Redshift: {num_slices}")
                num_partitions = num_slices
                while num_partitions < min_num_partitions:
                    num_partitions += num_slices
            logger.debug(f"Number of partitions calculated: {num_partitions}")
            spark.conf.set("spark.sql.execution.arrow.enabled", "true")
            session_primitives = self._session.primitives
            par_col_name: str = "aws_data_wrangler_internal_partition_id"

            @pandas_udf(returnType="objects_paths string", functionType=PandasUDFType.GROUPED_MAP)
            def write(pandas_dataframe: pd.DataFrame) -> pd.DataFrame:
                # Exporting ARROW_PRE_0_15_IPC_FORMAT environment variable for
                # a temporary workaround while waiting for Apache Arrow updates
                # https://stackoverflow.com/questions/58273063/pandasudf-and-pyarrow-0-15-0
                os.environ["ARROW_PRE_0_15_IPC_FORMAT"] = "1"

                del pandas_dataframe[par_col_name]
                paths: List[str] = session_primitives.session.pandas.to_parquet(dataframe=pandas_dataframe,
                                                                                path=path,
                                                                                preserve_index=False,
                                                                                mode="append",
                                                                                procs_cpu_bound=1,
                                                                                procs_io_bound=1,
                                                                                cast_columns=casts)
                return pd.DataFrame.from_dict({"objects_paths": paths})

            df_objects_paths: DataFrame = dataframe.repartition(numPartitions=num_partitions)  # type: ignore
            df_objects_paths = df_objects_paths.withColumn(par_col_name, spark_partition_id())  # type: ignore
            df_objects_paths = df_objects_paths.groupby(par_col_name).apply(write)  # type: ignore

            objects_paths: List[str] = list(df_objects_paths.toPandas()["objects_paths"])
            dataframe.unpersist()
            num_files_returned: int = len(objects_paths)
            if num_files_returned != num_partitions:
                raise MissingBatchDetected(f"{num_files_returned} files returned. {num_partitions} expected.")
            logger.debug(f"List of objects returned: {objects_paths}")
            logger.debug(f"Number of objects returned from UDF: {num_files_returned}")
            manifest_path: str = f"{path}manifest.json"
            self._session.redshift.write_load_manifest(manifest_path=manifest_path,
                                                       objects_paths=objects_paths,
                                                       procs_io_bound=self._procs_io_bound)
            self._session.redshift.load_table(dataframe=dataframe,
                                              dataframe_type="spark",
                                              manifest_path=manifest_path,
                                              schema_name=schema,
                                              table_name=table,
                                              redshift_conn=connection,
                                              preserve_index=False,
                                              num_files=num_partitions,
                                              iam_role=iam_role,
                                              diststyle=diststyle,
                                              distkey=distkey,
                                              sortstyle=sortstyle,
                                              sortkey=sortkey,
                                              mode=mode,
                                              cast_columns=casts,
                                              varchar_default_length=varchar_default_length,
                                              varchar_lengths=varchar_lengths)
            self._session.s3.delete_objects(path=path, procs_io_bound=self._procs_io_bound)
        except Exception as ex:
            connection.rollback()
            if generated_conn is True:
                connection.close()
            raise ex
        if generated_conn is True:
            connection.close()

    def create_glue_table(self,
                          database,
                          path,
                          dataframe,
                          file_format,
                          compression,
                          table=None,
                          serde=None,
                          sep=",",
                          partition_by=None,
                          load_partitions=True,
                          replace_if_exists=True,
                          description: Optional[str] = None,
                          parameters: Optional[Dict[str, str]] = None,
                          columns_comments: Optional[Dict[str, str]] = None):
        """
        Create a Glue metadata table pointing for some dataset stored on AWS S3.

        :param dataframe: PySpark Dataframe
        :param file_format: File format (E.g. "parquet", "csv")
        :param partition_by: Columns used for partitioning
        :param path: AWS S3 path
        :param compression: Compression (e.g. gzip, snappy, lzo, etc)
        :param sep: Separator token for CSV formats (e.g. ",", ";", "|")
        :param serde: Serializer/Deserializer (e.g. "OpenCSVSerDe", "LazySimpleSerDe")
        :param database: Glue database name
        :param table: Glue table name. If not passed, extracted from the path
        :param load_partitions: Load partitions after the table creation
        :param replace_if_exists: Drop table and recreates that if already exists
        :param description: Table description
        :param parameters: Key/value pairs to tag the table (Optional[Dict[str, str]])
        :param columns_comments: Columns names and the related comments (Optional[Dict[str, str]])
        :return: None
        """
        file_format = file_format.lower()
        if file_format not in ["parquet", "csv"]:
            raise UnsupportedFileFormat(file_format)
        table = table if table else self._session.glue._parse_table_name(path)
        table = table.lower().replace(".", "_")
        logger.debug(f"table: {table}")
        full_schema = dataframe.dtypes
        if partition_by is None:
            partition_by = []
        schema = [x for x in full_schema if x[0] not in partition_by]
        partitions_schema_tmp = {x[0]: x[1] for x in full_schema if x[0] in partition_by}
        partitions_schema = [(x, partitions_schema_tmp[x]) for x in partition_by]
        logger.debug(f"schema: {schema}")
        logger.debug(f"partitions_schema: {partitions_schema}")
        if replace_if_exists is not None:
            self._session.glue.delete_table_if_exists(database=database, table=table)
        extra_args = {}
        if file_format == "csv":
            extra_args["sep"] = sep
            if serde is None:
                serde = "OpenCSVSerDe"
            extra_args["serde"] = serde
        self._session.glue.create_table(database=database,
                                        table=table,
                                        schema=schema,
                                        partition_cols_schema=partitions_schema,
                                        path=path,
                                        file_format=file_format,
                                        compression=compression,
                                        extra_args=extra_args,
                                        description=description,
                                        parameters=parameters,
                                        columns_comments=columns_comments)
        if load_partitions:
            self._session.athena.repair_table(database=database, table=table)

    @staticmethod
    def _is_struct(dtype: str) -> bool:
        return True if dtype.startswith("struct") else False

    @staticmethod
    def _is_array(dtype: str) -> bool:
        return True if dtype.startswith("array") else False

    @staticmethod
    def _is_map(dtype: str) -> bool:
        return True if dtype.startswith("map") else False

    @staticmethod
    def _is_array_or_map(dtype: str) -> bool:
        return True if (dtype.startswith("array") or dtype.startswith("map")) else False

    @staticmethod
    def _parse_aux(path: str, aux: str) -> Tuple[str, str]:
        path_child: str
        dtype: str
        if ":" in aux:
            path_child, dtype = aux.split(sep=":", maxsplit=1)
        else:
            path_child = "element"
            dtype = aux
        return f"{path}.{path_child}", dtype

    @staticmethod
    def _flatten_struct_column(path: str, dtype: str) -> List[Tuple[str, str]]:
        dtype = dtype[7:-1]  # Cutting off "struct<" and ">"
        cols: List[Tuple[str, str]] = []
        struct_acc: int = 0
        path_child: str
        dtype_child: str
        aux: str = ""
        for c, i in zip(dtype, range(len(dtype), 0, -1)):  # Zipping a descendant ID for each letter
            if ((c == ",") and (struct_acc == 0)) or (i == 1):
                if i == 1:
                    aux += c
                path_child, dtype_child = Spark._parse_aux(path=path, aux=aux)
                if Spark._is_struct(dtype=dtype_child):
                    cols += Spark._flatten_struct_column(path=path_child, dtype=dtype_child)  # Recursion
                elif Spark._is_array(dtype=dtype):
                    cols.append((path, "array"))
                else:
                    cols.append((path_child, dtype_child))
                aux = ""
            elif c == "<":
                aux += c
                struct_acc += 1
            elif c == ">":
                aux += c
                struct_acc -= 1
            else:
                aux += c
        return cols

    @staticmethod
    def _flatten_struct_dataframe(df: DataFrame,
                                  explode_outer: bool = True,
                                  explode_pos: bool = True) -> List[Tuple[str, str, str]]:
        explode: str = "EXPLODE_OUTER" if explode_outer is True else "EXPLODE"
        explode = f"POS{explode}" if explode_pos is True else explode
        cols: List[Tuple[str, str]] = []
        for path, dtype in df.dtypes:
            if Spark._is_struct(dtype=dtype):
                cols += Spark._flatten_struct_column(path=path, dtype=dtype)
            elif Spark._is_array(dtype=dtype):
                cols.append((path, "array"))
            elif Spark._is_map(dtype=dtype):
                cols.append((path, "map"))
            else:
                cols.append((path, dtype))
        cols_exprs: List[Tuple[str, str, str]] = []
        expr: str
        for path, dtype in cols:
            path_under = path.replace('.', '_')
            if Spark._is_array(dtype):
                if explode_pos:
                    expr = f"{explode}({path}) AS ({path_under}_pos, {path_under})"
                else:
                    expr = f"{explode}({path}) AS {path_under}"
            elif Spark._is_map(dtype):
                if explode_pos:
                    expr = f"{explode}({path}) AS ({path_under}_pos, {path_under}_key, {path_under}_value)"
                else:
                    expr = f"{explode}({path}) AS ({path_under}_key, {path_under}_value)"
            else:
                expr = f"{path} AS {path.replace('.', '_')}"
            cols_exprs.append((path, dtype, expr))
        return cols_exprs

    @staticmethod
    def _build_name(name: str, expr: str) -> str:
        suffix: str = expr[expr.find("(") + 1:expr.find(")")]
        return f"{name}_{suffix}".replace(".", "_")

    @staticmethod
    def flatten(dataframe: DataFrame,
                explode_outer: bool = True,
                explode_pos: bool = True,
                name: str = "root") -> Dict[str, DataFrame]:
        """
        Convert a complex nested DataFrame in one (or many) flat DataFrames.

        If a columns is a struct it is flatten directly.
        If a columns is an array or map, then child DataFrames are created in different granularities.

        :param dataframe: Spark DataFrame
        :param explode_outer: Should we preserve the null values on arrays?
        :param explode_pos: Create columns with the index of the ex-array
        :param name: The name of the root Dataframe
        :return: A dictionary with the names as Keys and the DataFrames as Values
        """
        cols_exprs: List[Tuple[str, str, str]] = Spark._flatten_struct_dataframe(df=dataframe,
                                                                                 explode_outer=explode_outer,
                                                                                 explode_pos=explode_pos)
        exprs_arr: List[str] = [x[2] for x in cols_exprs if Spark._is_array_or_map(x[1])]
        exprs: List[str] = [x[2] for x in cols_exprs if not Spark._is_array_or_map(x[1])]
        dfs: Dict[str, DataFrame] = {name: dataframe.selectExpr(exprs)}
        exprs = [x[2] for x in cols_exprs if not Spark._is_array_or_map(x[1]) and not x[0].endswith("_pos")]
        for expr in exprs_arr:
            df_arr = dataframe.selectExpr(exprs + [expr])
            name_new: str = Spark._build_name(name=name, expr=expr)
            dfs_new = Spark.flatten(dataframe=df_arr,
                                    explode_outer=explode_outer,
                                    explode_pos=explode_pos,
                                    name=name_new)
            dfs = {**dfs, **dfs_new}
        return dfs
