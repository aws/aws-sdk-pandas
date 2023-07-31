import csv
import logging

import boto3
import pyarrow as pa
import pytest

import awswrangler as wr
import awswrangler.pandas as pd

from .._utils import ensure_data_types_csv, get_df_csv, is_ray_modin

logging.getLogger("awswrangler").setLevel(logging.DEBUG)

pytestmark = pytest.mark.distributed


@pytest.mark.parametrize("use_threads", [True, False])
@pytest.mark.parametrize("concurrent_partitioning", [True, False])
def test_to_csv_modes(glue_database, glue_table, path, use_threads, concurrent_partitioning):
    # Round 1 - Warm up
    df = pd.DataFrame({"c0": [0, 1]}, dtype="Int64")
    wr.s3.to_csv(
        df=df,
        path=path,
        dataset=True,
        mode="overwrite",
        database=glue_database,
        table=glue_table,
        glue_table_settings=wr.typing.GlueTableSettings(
            description="c0",
            parameters={"num_cols": str(len(df.columns)), "num_rows": str(len(df.index))},
            columns_comments={"c0": "0"},
        ),
        use_threads=use_threads,
        concurrent_partitioning=concurrent_partitioning,
        index=False,
    )
    df2 = wr.athena.read_sql_table(glue_table, glue_database, use_threads=use_threads)
    assert df.shape == df2.shape
    assert df.c0.sum() == df2.c0.sum()
    parameters = wr.catalog.get_table_parameters(glue_database, glue_table)
    assert len(parameters) >= 5
    assert parameters["num_cols"] == str(len(df2.columns))
    assert parameters["num_rows"] == str(len(df2.index))
    assert wr.catalog.get_table_description(glue_database, glue_table) == "c0"
    comments = wr.catalog.get_columns_comments(glue_database, glue_table)
    assert len(comments) == len(df.columns)
    assert comments["c0"] == "0"

    # Round 2 - Overwrite
    df = pd.DataFrame({"c1": [0, 1, 2]}, dtype="Int16")
    wr.s3.to_csv(
        df=df,
        dataset=True,
        mode="overwrite",
        database=glue_database,
        table=glue_table,
        glue_table_settings=wr.typing.GlueTableSettings(
            description="c1",
            parameters={"num_cols": str(len(df.columns)), "num_rows": str(len(df.index))},
            columns_comments={"c1": "1"},
        ),
        use_threads=use_threads,
        concurrent_partitioning=concurrent_partitioning,
        index=False,
    )
    df2 = wr.athena.read_sql_table(glue_table, glue_database, use_threads=use_threads)
    assert df.shape == df2.shape
    assert df.c1.sum() == df2.c1.sum()
    parameters = wr.catalog.get_table_parameters(glue_database, glue_table)
    assert len(parameters) >= 5
    assert parameters["num_cols"] == str(len(df2.columns))
    assert parameters["num_rows"] == str(len(df2.index))
    assert wr.catalog.get_table_description(glue_database, glue_table) == "c1"
    comments = wr.catalog.get_columns_comments(glue_database, glue_table)
    assert len(comments) == len(df.columns)
    assert comments["c1"] == "1"

    # Round 3 - Append
    df = pd.DataFrame({"c1": [0, 1, 2]}, dtype="Int8")
    wr.s3.to_csv(
        df=df,
        path=path,
        dataset=True,
        mode="append",
        database=glue_database,
        table=glue_table,
        glue_table_settings=wr.typing.GlueTableSettings(
            description="c1",
            parameters={"num_cols": str(len(df.columns)), "num_rows": str(len(df.index) * 2)},
            columns_comments={"c1": "1"},
        ),
        use_threads=use_threads,
        concurrent_partitioning=concurrent_partitioning,
        index=False,
    )
    df2 = wr.athena.read_sql_table(glue_table, glue_database, use_threads=use_threads)
    assert len(df.columns) == len(df2.columns)
    assert len(df.index) * 2 == len(df2.index)
    assert df.c1.sum() + 3 == df2.c1.sum()
    parameters = wr.catalog.get_table_parameters(glue_database, glue_table)
    assert len(parameters) >= 5
    assert parameters["num_cols"] == str(len(df2.columns))
    assert parameters["num_rows"] == str(len(df2.index))
    assert wr.catalog.get_table_description(glue_database, glue_table) == "c1"
    comments = wr.catalog.get_columns_comments(glue_database, glue_table)
    assert len(comments) == len(df.columns)
    assert comments["c1"] == "1"

    # Round 4 - Overwrite Partitioned
    df = pd.DataFrame({"c0": ["foo", "boo"], "c1": [0, 1]})
    wr.s3.to_csv(
        df=df,
        dataset=True,
        mode="overwrite",
        database=glue_database,
        table=glue_table,
        glue_table_settings=wr.typing.GlueTableSettings(
            description="c0+c1",
            parameters={"num_cols": "2", "num_rows": "2"},
            columns_comments={"c0": "zero", "c1": "one"},
        ),
        partition_cols=["c1"],
        use_threads=use_threads,
        concurrent_partitioning=concurrent_partitioning,
        index=False,
    )
    df2 = wr.athena.read_sql_table(glue_table, glue_database, use_threads=use_threads)
    assert df.shape == df2.shape
    assert df.c1.sum() == df2.c1.sum()
    parameters = wr.catalog.get_table_parameters(glue_database, glue_table)
    assert len(parameters) >= 5
    assert parameters["num_cols"] == "2"
    assert parameters["num_rows"] == "2"
    assert wr.catalog.get_table_description(glue_database, glue_table) == "c0+c1"
    comments = wr.catalog.get_columns_comments(glue_database, glue_table)
    assert len(comments) == len(df.columns)
    assert comments["c0"] == "zero"
    assert comments["c1"] == "one"

    # Round 5 - Overwrite Partitions
    df = pd.DataFrame({"c0": ["bar", "abc"], "c1": [0, 2]})
    wr.s3.to_csv(
        df=df,
        path=path,
        dataset=True,
        mode="overwrite_partitions",
        database=glue_database,
        table=glue_table,
        glue_table_settings=wr.typing.GlueTableSettings(
            description="c0+c1",
            parameters={"num_cols": "2", "num_rows": "3"},
            columns_comments={"c0": "zero", "c1": "one"},
        ),
        partition_cols=["c1"],
        concurrent_partitioning=concurrent_partitioning,
        use_threads=use_threads,
        index=False,
    )
    df2 = wr.athena.read_sql_table(glue_table, glue_database, use_threads=use_threads)
    assert len(df2.columns) == 2
    assert len(df2.index) == 3
    assert df2.c1.sum() == 3
    parameters = wr.catalog.get_table_parameters(glue_database, glue_table)
    assert len(parameters) >= 5
    assert parameters["num_cols"] == "2"
    assert parameters["num_rows"] == "3"
    assert wr.catalog.get_table_description(glue_database, glue_table) == "c0+c1"
    comments = wr.catalog.get_columns_comments(glue_database, glue_table)
    assert len(comments) == len(df.columns)
    assert comments["c0"] == "zero"
    assert comments["c1"] == "one"


@pytest.mark.parametrize("use_threads", [True, False])
def test_csv_overwrite_several_partitions(path, glue_database, glue_table, use_threads):
    df0 = pd.DataFrame({"id": list(range(27)), "par": list(range(27))})
    df1 = pd.DataFrame({"id": list(range(26)), "par": list(range(26))})
    for df in (df0, df1):
        wr.s3.to_csv(
            df=df,
            path=path,
            index=False,
            use_threads=use_threads,
            dataset=True,
            partition_cols=["par"],
            mode="overwrite",
            database=glue_database,
            table=glue_table,
            concurrent_partitioning=True,
        )
        df2 = wr.athena.read_sql_table(glue_table, glue_database, use_threads=use_threads)
        assert df2.shape == df.shape
        assert df2["id"].sum() == df["id"].sum()
        assert df2["par"].sum() == df["par"].sum()


@pytest.mark.xfail(
    is_ray_modin, raises=wr.exceptions.InvalidArgumentCombination, reason="Ray can't load frame with no header"
)
def test_csv_dataset(path, glue_database):
    with pytest.raises(wr.exceptions.UndetectedType):
        wr.s3.to_csv(
            pd.DataFrame({"A": [None]}),
            path,
            dataset=True,
            database=glue_database,
            table="test_csv_dataset",
        )
    df = get_df_csv()
    with pytest.raises(wr.exceptions.InvalidArgumentCombination):
        wr.s3.to_csv(
            df,
            path + "0",
            dataset=False,
            mode="overwrite",
            database=glue_database,
            table="test_csv_dataset",
        )
    with pytest.raises(wr.exceptions.InvalidArgumentCombination):
        wr.s3.to_csv(
            df,
            path + "0",
            dataset=False,
            database=None,
            table="test_csv_dataset",
        )
    with pytest.raises(wr.exceptions.InvalidArgumentCombination):
        wr.s3.to_csv(df=df, path=path + "0", mode="append")
    with pytest.raises(wr.exceptions.InvalidArgumentCombination):
        wr.s3.to_csv(df=df, path=path + "0", partition_cols=["col2"])
    with pytest.raises(wr.exceptions.InvalidArgumentCombination):
        wr.s3.to_csv(
            df=df,
            path=path + "0",
            database=None,
            table=None,
            glue_table_settings=wr.typing.GlueTableSettings(description="foo"),
        )
    with pytest.raises(wr.exceptions.InvalidArgumentValue):
        wr.s3.to_csv(df=df, path=path + "0", partition_cols=["col2"], dataset=True, mode="WRONG")
    paths = wr.s3.to_csv(
        df=df,
        path=path,
        sep="|",
        index=False,
        use_threads=True,
        boto3_session=None,
        s3_additional_kwargs=None,
        dataset=True,
        partition_cols=["par0", "par1"],
        mode="overwrite",
        header=False,
    )["paths"]
    df2 = wr.s3.read_csv(path=paths, sep="|", header=None)
    assert len(df2.index) == 3
    assert len(df2.columns) == 8
    assert df2[0].sum() == 6
    wr.s3.delete_objects(path=paths)


@pytest.mark.parametrize("use_threads", [True, False])
@pytest.mark.parametrize("concurrent_partitioning", [True, False])
def test_csv_catalog(path, glue_table, glue_database, use_threads, concurrent_partitioning):
    df = get_df_csv()
    wr.s3.to_csv(
        df=df,
        path=path,
        sep="\t",
        index=True,
        use_threads=use_threads,
        boto3_session=None,
        s3_additional_kwargs=None,
        dataset=True,
        partition_cols=["par0", "par1"],
        mode="overwrite",
        table=glue_table,
        database=glue_database,
        concurrent_partitioning=concurrent_partitioning,
    )
    df2 = wr.athena.read_sql_table(glue_table, glue_database)
    assert len(df2.index) == 3
    assert len(df2.columns) == 11
    assert df2["id"].sum() == 6
    ensure_data_types_csv(df2)


@pytest.mark.parametrize("use_threads", [True, False])
@pytest.mark.parametrize("concurrent_partitioning", [True, False])
def test_csv_catalog_columns(path, glue_database, glue_table, use_threads, concurrent_partitioning):
    wr.s3.to_csv(
        df=get_df_csv(),
        path=path,
        sep="|",
        columns=["id", "date", "timestamp", "par0", "par1"],
        index=False,
        use_threads=use_threads,
        boto3_session=None,
        s3_additional_kwargs=None,
        dataset=True,
        partition_cols=["par0", "par1"],
        mode="overwrite",
        table=glue_table,
        database=glue_database,
        concurrent_partitioning=concurrent_partitioning,
    )
    df2 = wr.athena.read_sql_table(glue_table, glue_database, use_threads=use_threads)
    assert len(df2.index) == 3
    assert len(df2.columns) == 5
    assert df2["id"].sum() == 6
    ensure_data_types_csv(df2)

    wr.s3.to_csv(
        df=pd.DataFrame({"id": [4], "date": [None], "timestamp": [None], "par0": [1], "par1": ["a"]}),
        path=path,
        sep="|",
        index=False,
        use_threads=use_threads,
        boto3_session=None,
        s3_additional_kwargs=None,
        dataset=True,
        partition_cols=["par0", "par1"],
        mode="overwrite_partitions",
        table=glue_table,
        database=glue_database,
        concurrent_partitioning=concurrent_partitioning,
    )
    df2 = wr.athena.read_sql_table(glue_table, glue_database, use_threads=use_threads)
    assert len(df2.index) == 3
    assert len(df2.columns) == 5
    assert df2["id"].sum() == 9
    ensure_data_types_csv(df2)


def test_athena_csv_types(path, glue_database, glue_table):
    df = get_df_csv()
    wr.s3.to_csv(
        df=df,
        path=path,
        sep=",",
        index=False,
        use_threads=True,
        boto3_session=None,
        s3_additional_kwargs=None,
        dataset=True,
        header=False,
        partition_cols=["par0", "par1"],
        mode="overwrite",
    )
    columns_types, partitions_types = wr.catalog.extract_athena_types(
        df=df, index=False, partition_cols=["par0", "par1"], file_format="csv"
    )
    wr.catalog.create_csv_table(
        table=glue_table,
        database=glue_database,
        path=path,
        partitions_types=partitions_types,
        columns_types=columns_types,
    )
    columns_types["col0"] = "string"
    with pytest.raises(wr.exceptions.InvalidArgumentValue):
        wr.catalog.create_csv_table(
            database=glue_database, table=glue_table, path=path, columns_types=columns_types, mode="append"
        )
    wr.athena.repair_table(glue_table, glue_database)
    assert len(wr.catalog.get_csv_partitions(glue_database, glue_table)) == 3
    df2 = wr.athena.read_sql_table(glue_table, glue_database)
    assert len(df2.index) == 3
    assert len(df2.columns) == 10
    assert df2["id"].sum() == 6
    ensure_data_types_csv(df2)


@pytest.mark.modin_index
@pytest.mark.parametrize("use_threads", [True, False])
@pytest.mark.parametrize("ctas_approach", [True, False])
@pytest.mark.parametrize("line_count", [1, 2])
def test_skip_header(path, glue_database, glue_table, use_threads, ctas_approach, line_count):
    df = pd.DataFrame({"c0": [1, 2], "c1": [3.3, 4.4], "c2": ["foo", "boo"]})
    df["c0"] = df["c0"].astype("Int64")
    df["c2"] = df["c2"].astype("string")
    wr.s3.to_csv(df=df, path=f"{path}0.csv", sep=",", index=False, header=True, use_threads=use_threads)
    wr.catalog.create_csv_table(
        database=glue_database,
        table=glue_table,
        path=path,
        columns_types={"c0": "bigint", "c1": "double", "c2": "string"},
        skip_header_line_count=line_count,
    )
    df2 = wr.athena.read_sql_table(glue_table, glue_database, use_threads=use_threads, ctas_approach=ctas_approach)
    assert df.iloc[line_count - 1 :].reset_index(drop=True).equals(df2.reset_index(drop=True))


@pytest.mark.parametrize("use_threads", [True, False])
def test_empty_column(path, glue_table, glue_database, use_threads):
    df = pd.DataFrame({"c0": [1, 2, 3], "c1": [None, None, None], "par": ["a", "b", "c"]})
    df["c0"] = df["c0"].astype("Int64")
    df["par"] = df["par"].astype("string")
    with pytest.raises(wr.exceptions.UndetectedType):
        wr.s3.to_csv(
            df,
            path,
            index=False,
            dataset=True,
            use_threads=use_threads,
            table=glue_table,
            database=glue_database,
            partition_cols=["par"],
        )


@pytest.mark.parametrize("use_threads", [True, False])
def test_mixed_types_column(path, glue_table, glue_database, use_threads):
    df = pd.DataFrame({"c0": [1, 2, 3], "c1": [1, 2, "foo"], "par": ["a", "b", "c"]})
    df["c0"] = df["c0"].astype("Int64")
    df["par"] = df["par"].astype("string")
    with pytest.raises(pa.ArrowInvalid):
        wr.s3.to_csv(
            df,
            path,
            use_threads=use_threads,
            index=False,
            dataset=True,
            table=glue_table,
            database=glue_database,
            partition_cols=["par"],
        )


@pytest.mark.parametrize("use_threads", [True, False])
def test_failing_catalog(path, glue_table, use_threads):
    df = pd.DataFrame({"c0": [1, 2, 3]})
    try:
        wr.s3.to_csv(
            df,
            path,
            use_threads=use_threads,
            dataset=True,
            table=glue_table,
            database="foo",
        )
    except boto3.client("glue").exceptions.EntityNotFoundException:
        pass
    assert len(wr.s3.list_objects(path)) == 0


@pytest.mark.parametrize("use_threads", [True, False])
@pytest.mark.parametrize("concurrent_partitioning", [True, False])
@pytest.mark.parametrize("compression", ["gzip", "bz2", None])
def test_csv_compressed(path, glue_table, glue_database, use_threads, concurrent_partitioning, compression):
    wr.s3.to_csv(
        df=get_df_csv(),
        path=path,
        sep="\t",
        index=True,
        use_threads=use_threads,
        boto3_session=None,
        s3_additional_kwargs=None,
        dataset=True,
        partition_cols=["par0", "par1"],
        mode="overwrite",
        table=glue_table,
        database=glue_database,
        concurrent_partitioning=concurrent_partitioning,
        compression=compression,
    )
    df2 = wr.athena.read_sql_table(glue_table, glue_database)
    assert df2.shape == (3, 11)
    assert df2["id"].sum() == 6
    ensure_data_types_csv(df2)


@pytest.mark.xfail(is_ray_modin, raises=TypeError, reason="Broken sort_values in Modin")
@pytest.mark.parametrize("use_threads", [True, False])
@pytest.mark.parametrize("ctas_approach", [True, False])
def test_opencsv_serde(path, glue_table, glue_database, use_threads, ctas_approach):
    df = pd.DataFrame({"col": ["1", "2", "3"], "col2": ["A", "A", "B"]})
    response = wr.s3.to_csv(
        df=df,
        path=path,
        dataset=True,
        partition_cols=["col2"],
        sep=",",
        index=False,
        header=False,
        use_threads=use_threads,
        quoting=csv.QUOTE_NONE,
    )
    wr.catalog.create_csv_table(
        database=glue_database,
        table=glue_table,
        path=path,
        columns_types={"col": "string"},
        partitions_types={"col2": "string"},
        serde_library="org.apache.hadoop.hive.serde2.OpenCSVSerde",
        serde_parameters={"separatorChar": ",", "quoteChar": '"', "escapeChar": "\\"},
    )
    wr.catalog.add_csv_partitions(
        database=glue_database,
        table=glue_table,
        partitions_values=response["partitions_values"],
        serde_library="org.apache.hadoop.hive.serde2.OpenCSVSerde",
        serde_parameters={"separatorChar": ",", "quoteChar": '"', "escapeChar": "\\"},
    )
    df2 = wr.athena.read_sql_table(
        table=glue_table, database=glue_database, use_threads=use_threads, ctas_approach=ctas_approach
    )
    df = df.applymap(lambda x: x.replace('"', "")).convert_dtypes()
    assert df.equals(df2.sort_values(by=list(df2)).reset_index(drop=True))
