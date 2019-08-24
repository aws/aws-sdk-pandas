import logging

import pandas

from pyspark.sql.functions import pandas_udf, PandasUDFType
from pyspark.sql.functions import floor, rand

from awswrangler.exceptions import MissingBatchDetected

logger = logging.getLogger(__name__)

MIN_NUMBER_OF_ROWS_TO_DISTRIBUTE = 1000


class Spark:
    def __init__(self, session):
        self._session = session

    def read_csv(self, path):
        spark = self._session.spark_session
        return spark.read.csv(path=path, header=True)

    def to_redshift(
            self,
            dataframe,
            path,
            connection,
            schema,
            table,
            iam_role,
            diststyle="AUTO",
            distkey=None,
            sortstyle="COMPOUND",
            sortkey=None,
            min_num_partitions=200,
            mode="append",
    ):
        """
        Load Spark Dataframe as a Table on Amazon Redshift

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
        :param min_num_partitions: Minimal number of partitions
        :param mode: append or overwrite
        :return: None
        """
        logger.debug(f"Minimum number of partitions : {min_num_partitions}")
        if path[-1] != "/":
            path += "/"
        self._session.s3.delete_objects(path=path)
        spark = self._session.spark_session
        dataframe.cache()
        num_rows = dataframe.count()
        logger.info(f"Number of rows: {num_rows}")
        if num_rows < MIN_NUMBER_OF_ROWS_TO_DISTRIBUTE:
            num_partitions = 1
        else:
            num_slices = self._session.redshift.get_number_of_slices(
                redshift_conn=connection)
            logger.debug(f"Number of slices on Redshift: {num_slices}")
            num_partitions = num_slices
            while num_partitions < min_num_partitions:
                num_partitions += num_slices
        logger.debug(f"Number of partitions calculated: {num_partitions}")
        spark.conf.set("spark.sql.execution.arrow.enabled", "true")
        session_primitives = self._session.primitives

        @pandas_udf(returnType="objects_paths string",
                    functionType=PandasUDFType.GROUPED_MAP)
        def write(pandas_dataframe):
            del pandas_dataframe["partition_index"]
            paths = session_primitives.session.pandas.to_parquet(
                dataframe=pandas_dataframe,
                path=path,
                preserve_index=False,
                mode="append",
                procs_cpu_bound=1,
            )
            return pandas.DataFrame.from_dict({"objects_paths": paths})

        df_objects_paths = (dataframe.withColumn(
            "partition_index", floor(rand() * num_partitions)).repartition(
                "partition_index").groupby("partition_index").apply(write))
        objects_paths = list(df_objects_paths.toPandas()["objects_paths"])
        num_files_returned = len(objects_paths)
        if num_files_returned != num_partitions:
            raise MissingBatchDetected(
                f"{num_files_returned} files returned. {num_partitions} expected."
            )
        logger.debug(f"List of objects returned: {objects_paths}")
        logger.debug(
            f"Number of objects returned from UDF: {num_files_returned}")
        manifest_path = f"{path}manifest.json"
        self._session.redshift.write_load_manifest(manifest_path=manifest_path,
                                                   objects_paths=objects_paths)
        self._session.redshift.load_table(
            dataframe=dataframe,
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
        )
        dataframe.unpersist()
        self._session.s3.delete_objects(path=path)
