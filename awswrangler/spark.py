import pandas

from pyspark.sql.functions import pandas_udf, PandasUDFType
from pyspark.sql.functions import floor, rand


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
        mode="append",
    ):
        spark = self._session.spark_session
        self._session.s3.delete_objects(path=path)
        num_slices = self._session.redshift.get_number_of_slices(
            redshift_conn=connection
        )
        spark.conf.set("spark.sql.execution.arrow.enabled", "true")
        session_primitives = self._session.primitives
        if path[-1] != "/":
            path += "/"

        @pandas_udf(returnType="objects_paths string", functionType=PandasUDFType.GROUPED_MAP)
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

        df_objects_paths = dataframe.withColumn("partition_index", floor(rand() * num_slices))\
            .groupby("partition_index")\
            .apply(write)
        objects_paths = list(df_objects_paths.toPandas()["objects_paths"])
        manifest_path = f"{path}manifest.json"
        self._session.redshift.write_load_manifest(
            manifest_path=manifest_path, objects_paths=objects_paths
        )
        self._session.redshift.load_table(
            dataframe=dataframe,
            dataframe_type="spark",
            manifest_path=manifest_path,
            schema_name=schema,
            table_name=table,
            redshift_conn=connection,
            preserve_index=False,
            num_files=num_slices,
            iam_role=iam_role,
            mode=mode,
        )
        self._session.s3.delete_objects(path=path)
