import logging

import boto3
import pytest

import awswrangler as wr
import awswrangler.pandas as pd

from .._utils import is_pandas_2_x, is_ray_modin

logging.getLogger("awswrangler").setLevel(logging.DEBUG)

pytestmark = pytest.mark.distributed


@pytest.mark.parametrize(
    "encoding,strings,wrong_encoding,exception",
    [
        ("utf-8", ["漢字", "ãóú", "г, д, ж, з, к, л"], "ISO-8859-1", AssertionError),
        ("ISO-8859-1", ["Ö, ö, Ü, ü", "ãóú", "øe"], "utf-8", UnicodeDecodeError),
        ("ISO-8859-1", ["Ö, ö, Ü, ü", "ãóú", "øe"], None, UnicodeDecodeError),
    ],
)
@pytest.mark.parametrize("use_threads", [True, False, 2])
@pytest.mark.parametrize("chunksize", [None, 2])
@pytest.mark.parametrize("line_terminator", ["\n", "\r"])
def test_csv_encoding(path, encoding, strings, wrong_encoding, exception, line_terminator, chunksize, use_threads):
    file_path = f"{path}0.csv"
    df = pd.DataFrame({"c0": [1, 2, 3], "c1": strings})
    wr.s3.to_csv(df, file_path, index=False, encoding=encoding, lineterminator=line_terminator, use_threads=use_threads)
    df2 = wr.s3.read_csv(
        file_path, encoding=encoding, lineterminator=line_terminator, use_threads=use_threads, chunksize=chunksize
    )
    if isinstance(df2, pd.DataFrame) is False:
        df2 = pd.concat(df2, ignore_index=True)
    assert df.equals(df2)
    with pytest.raises(exception):
        df2 = wr.s3.read_csv(file_path, encoding=wrong_encoding, use_threads=use_threads, chunksize=chunksize)
        if isinstance(df2, pd.DataFrame) is False:
            df2 = pd.concat(df2, ignore_index=True)
        assert df.equals(df2)


@pytest.mark.parametrize(
    "encoding,strings,wrong_encoding",
    [
        ("utf-8", ["漢字", "ãóú", "г, д, ж, з, к, л"], "ascii"),
        ("ISO-8859-15", ["Ö, ö, Ü, ü", "ãóú", "øe"], "ascii"),
    ],
)
def test_csv_ignore_encoding_errors(path, encoding, strings, wrong_encoding):
    file_path = f"{path}0.csv"
    df = pd.DataFrame({"c0": [1, 2, 3], "c1": strings})
    wr.s3.to_csv(df, file_path, index=False, encoding=encoding)
    with pytest.raises(UnicodeDecodeError):
        df2 = wr.s3.read_csv(file_path, encoding=wrong_encoding)
    df2 = wr.s3.read_csv(file_path, encoding=wrong_encoding, encoding_errors="ignore")
    if isinstance(df2, pd.DataFrame) is False:
        df2 = pd.concat(df2, ignore_index=True)
        assert df2.shape == (3, 4)


@pytest.mark.parametrize("use_threads", [True, False, 2])
@pytest.mark.parametrize("chunksize", [None, 1])
def test_read_partitioned_json_paths(path, use_threads, chunksize):
    df = pd.DataFrame({"c0": [0, 1], "c1": ["foo", "boo"]})
    paths = [f"{path}year={y}/month={m}/0.json" for y, m in [(2020, 1), (2020, 2), (2021, 1)]]
    for p in paths:
        wr.s3.to_json(df, path=p, orient="records", lines=True)
    df2 = wr.s3.read_json(path, dataset=True, use_threads=use_threads, chunksize=chunksize)
    if chunksize is None:
        assert df2.shape == (6, 4)
        assert df2.c0.sum() == 3
    else:
        for d in df2:
            assert d.shape == (1, 4)


@pytest.mark.parametrize("use_threads", [True, False, 2])
@pytest.mark.parametrize("chunksize", [None, 1])
def test_read_partitioned_csv_paths(path, use_threads, chunksize):
    df = pd.DataFrame({"c0": [0, 1], "c1": ["foo", "boo"]})
    paths = [f"{path}year={y}/month={m}/0.csv" for y, m in [(2020, 1), (2020, 2), (2021, 1)]]
    for p in paths:
        wr.s3.to_csv(df, p, index=False)
    df2 = wr.s3.read_csv(path, dataset=True, use_threads=use_threads, chunksize=chunksize)
    if chunksize is None:
        assert df2.shape == (6, 4)
        assert df2.c0.sum() == 3
    else:
        for d in df2:
            assert d.shape == (1, 4)


@pytest.mark.parametrize("use_threads", [True, False, 2])
@pytest.mark.parametrize("chunksize", [None, 1])
def test_read_partitioned_fwf(path, use_threads, chunksize):
    text = "0foo\n1boo"
    client_s3 = boto3.client("s3")
    paths = [f"{path}year={y}/month={m}/0.csv" for y, m in [(2020, 1), (2020, 2), (2021, 1)]]
    for p in paths:
        bucket, key = wr._utils.parse_path(p)
        client_s3.put_object(Body=text, Bucket=bucket, Key=key)
    df2 = wr.s3.read_fwf(
        path, dataset=True, use_threads=use_threads, chunksize=chunksize, widths=[1, 3], names=["c0", "c1"]
    )
    if chunksize is None:
        assert df2.shape == (6, 4)
        assert df2.c0.sum() == 3
    else:
        for d in df2:
            assert d.shape == (1, 4)


@pytest.mark.xfail(
    is_ray_modin, raises=wr.exceptions.InvalidArgument, reason="kwargs not supported in distributed mode"
)
def test_csv(path):
    session = boto3.Session()
    df = pd.DataFrame({"id": [1, 2, 3]})
    path0 = f"{path}test_csv0.csv"
    path1 = f"{path}test_csv1.csv"
    path2 = f"{path}test_csv2.csv"
    wr.s3.to_csv(df=df, path=path0, index=False)
    assert wr.s3.does_object_exist(path=path0) is True
    assert wr.s3.size_objects(path=[path0], use_threads=False)[path0] == 9
    assert wr.s3.size_objects(path=[path0], use_threads=True)[path0] == 9
    wr.s3.to_csv(df=df, path=path1, index=False, boto3_session=None)
    wr.s3.to_csv(df=df, path=path2, index=False, boto3_session=session)
    assert df.equals(wr.s3.read_csv(path=path0, use_threads=False))
    assert df.equals(wr.s3.read_csv(path=path0, use_threads=True))
    assert df.equals(wr.s3.read_csv(path=path0, use_threads=False, boto3_session=session))
    assert df.equals(wr.s3.read_csv(path=path0, use_threads=True, boto3_session=session))
    paths = [path0, path1, path2]
    df2 = pd.concat(objs=[df, df, df], sort=False, ignore_index=True)
    assert df2.equals(wr.s3.read_csv(path=paths, use_threads=False))
    assert df2.equals(wr.s3.read_csv(path=paths, use_threads=True))
    assert df2.equals(wr.s3.read_csv(path=paths, use_threads=False, boto3_session=session))
    assert df2.equals(wr.s3.read_csv(path=paths, use_threads=True, boto3_session=session))
    with pytest.raises(wr.exceptions.InvalidArgumentType):
        wr.s3.read_csv(path=1)
    with pytest.raises(wr.exceptions.InvalidArgument):
        wr.s3.read_csv(path=paths, iterator=True)


@pytest.mark.parametrize("header", [True, ["identifier"]])
def test_csv_dataset_header(path, header, glue_database, glue_table):
    path0 = f"{path}test_csv_dataset0.csv"
    df0 = pd.DataFrame({"id": [1, 2, 3]})
    wr.s3.to_csv(
        df=df0,
        path=path0,
        dataset=True,
        database=glue_database,
        table=glue_table,
        index=False,
        header=header,
    )
    df1 = wr.s3.read_csv(path=path0)
    if isinstance(header, list):
        df0.columns = header
    assert df0.equals(df1)


@pytest.mark.parametrize("mode", ["append", "overwrite"])
def test_csv_dataset_header_modes(path, mode, glue_database, glue_table):
    path0 = f"{path}test_csv_dataset0.csv"
    dfs = [
        pd.DataFrame({"id": [1, 2, 3]}),
        pd.DataFrame({"id": [4, 5, 6]}),
    ]
    for df in dfs:
        wr.s3.to_csv(
            df=df,
            path=path0,
            dataset=True,
            database=glue_database,
            table=glue_table,
            mode=mode,
            index=False,
            header=True,
        )
    df_res = wr.s3.read_csv(path=path0)

    if mode == "append":
        assert len(df_res) == sum([len(df) for df in dfs])
    else:
        assert df_res.equals(dfs[-1])


@pytest.mark.modin_index
@pytest.mark.xfail(
    raises=AssertionError,
    reason="https://github.com/ray-project/ray/issues/37771",
    condition=is_ray_modin,
)
def test_json(path):
    df0 = pd.DataFrame({"id": [1, 2, 3]})
    path0 = f"{path}test_json0.json"
    path1 = f"{path}test_json1.json"
    wr.s3.to_json(df=df0, path=path0)
    wr.s3.to_json(df=df0, path=path1)
    assert df0.equals(wr.s3.read_json(path=path0, use_threads=False))
    df1 = pd.concat(objs=[df0, df0], sort=False, ignore_index=False)
    assert df1.equals(wr.s3.read_json(path=[path0, path1], use_threads=True))


def test_json_lines(path):
    df0 = pd.DataFrame({"id": [1, 2, 3]})
    path0 = f"{path}test_json0.json"
    path1 = f"{path}test_json1.json"
    wr.s3.to_json(df=df0, path=path0, orient="records", lines=True)
    wr.s3.to_json(df=df0, path=path1, orient="records", lines=True)
    assert df0.equals(wr.s3.read_json(path=path0, use_threads=False, orient="records", lines=True))
    df1 = pd.concat(objs=[df0, df0], sort=False, ignore_index=True)
    assert df1.equals(wr.s3.read_json(path=[path0, path1], use_threads=True, orient="records", lines=True))


def test_to_json_partitioned(path, glue_database, glue_table):
    df = pd.DataFrame({"c0": [0, 1, 2], "c1": [3, 4, 5], "c2": [6, 7, 8]})
    partitions = wr.s3.to_json(
        df,
        path,
        dataset=True,
        database=glue_database,
        table=glue_table,
        partition_cols=["c0"],
    )
    assert len(partitions["paths"]) == 3
    assert len(partitions["partitions_values"]) == 3


@pytest.mark.parametrize("filename_prefix", [None, "my_prefix"])
@pytest.mark.parametrize("use_threads", [True, False])
def test_to_text_filename_prefix(compare_filename_prefix, path, filename_prefix, use_threads):
    test_prefix = "my_prefix"
    df = pd.DataFrame({"col": [1, 2, 3], "col2": ["A", "A", "B"]})

    # If Dataset is False, csv/json file should never start with prefix
    file_path = f"{path}0.json"
    filename = wr.s3.to_json(df=df, path=file_path, use_threads=use_threads)["paths"][0].split("/")[-1]
    assert not filename.startswith(test_prefix)
    file_path = f"{path}0.csv"
    filename = wr.s3.to_csv(
        df=df, path=file_path, dataset=False, filename_prefix=filename_prefix, use_threads=use_threads
    )["paths"][0].split("/")[-1]
    assert not filename.startswith(test_prefix)

    # If Dataset is True, csv file starts with prefix if one is supplied
    filename = wr.s3.to_csv(df=df, path=path, dataset=True, filename_prefix=filename_prefix, use_threads=use_threads)[
        "paths"
    ][0].split("/")[-1]
    compare_filename_prefix(filename, filename_prefix, test_prefix)

    # Partitioned
    filename = wr.s3.to_csv(
        df=df,
        path=path,
        dataset=True,
        filename_prefix=filename_prefix,
        partition_cols=["col2"],
        use_threads=use_threads,
    )["paths"][0].split("/")[-1]
    compare_filename_prefix(filename, filename_prefix, test_prefix)

    # Bucketing
    filename = wr.s3.to_csv(
        df=df,
        path=path,
        dataset=True,
        filename_prefix=filename_prefix,
        bucketing_info=(["col2"], 2),
        use_threads=use_threads,
    )["paths"][0].split("/")[-1]
    compare_filename_prefix(filename, filename_prefix, test_prefix)
    assert filename.endswith("bucket-00000.csv")


def test_fwf(path):
    text = "1 Herfelingen27-12-18\n2   Lambusart14-06-18\n3Spormaggiore15-04-18"
    client_s3 = boto3.client("s3")
    path0 = f"{path}0.txt"
    bucket, key = wr._utils.parse_path(path0)
    client_s3.put_object(Body=text, Bucket=bucket, Key=key)
    path1 = f"{path}1.txt"
    bucket, key = wr._utils.parse_path(path1)
    client_s3.put_object(Body=text, Bucket=bucket, Key=key)
    df = wr.s3.read_fwf(path=path0, use_threads=False, widths=[1, 12, 8], names=["id", "name", "date"])
    assert df.shape == (3, 3)
    df = wr.s3.read_fwf(path=[path0, path1], use_threads=True, widths=[1, 12, 8], names=["id", "name", "date"])
    assert df.shape == (6, 3)


def test_json_chunksize(path):
    num_files = 10
    df = pd.DataFrame({"id": [1, 2, 3], "value": ["foo", "boo", "bar"]})
    paths = [f"{path}{i}.json" for i in range(num_files)]
    for p in paths:
        wr.s3.to_json(df, p, orient="records", lines=True)
    dfs = list(wr.s3.read_json(paths, lines=True, chunksize=1))
    assert len(dfs) == (3 * num_files)
    for d in dfs:
        assert len(d.columns) == 2
        assert d.id.iloc[0] in (1, 2, 3)
        assert d.value.iloc[0] in ("foo", "boo", "bar")


def test_read_csv_index(path):
    df0 = pd.DataFrame({"id": [4, 5, 6], "value": ["foo", "boo", "bar"]})
    df1 = pd.DataFrame({"id": [1, 2, 3], "value": ["a", "b", "c"]})
    path0 = f"{path}test_csv0.csv"
    path1 = f"{path}test_csv1.csv"
    paths = [path0, path1]
    wr.s3.to_csv(df=df0, path=path0, index=False)
    wr.s3.to_csv(df=df1, path=path1, index=False)
    df = wr.s3.read_csv(paths, index_col=["id"])
    assert df.shape == (6, 1)


def test_read_json_index(path):
    df0 = pd.DataFrame({"id": [4, 5, 6], "value": ["foo", "boo", "bar"]})
    df1 = pd.DataFrame({"id": [1, 2, 3], "value": ["a", "b", "c"]})
    path0 = f"{path}test0.csv"
    path1 = f"{path}test1.csv"
    paths = [path0, path1]
    wr.s3.to_json(df=df0, path=path0, orient="index")
    wr.s3.to_json(df=df1, path=path1, orient="index")
    df = wr.s3.read_json(paths, orient="index")
    assert df.shape == (6, 2)


@pytest.mark.parametrize("use_threads", [True, False, 2])
@pytest.mark.parametrize(
    "s3_additional_kwargs",
    [None, {"ServerSideEncryption": "AES256"}, {"ServerSideEncryption": "aws:kms", "SSEKMSKeyId": None}],
)
def test_csv_additional_kwargs(path, kms_key_id, s3_additional_kwargs, use_threads):
    if s3_additional_kwargs is not None and "SSEKMSKeyId" in s3_additional_kwargs:
        s3_additional_kwargs["SSEKMSKeyId"] = kms_key_id
    path = f"{path}0.txt"
    df = pd.DataFrame({"c0": [0, 1, 2], "c1": [3, 4, 5]})
    wr.s3.to_csv(df, path, index=False, s3_additional_kwargs=s3_additional_kwargs)
    assert df.equals(wr.s3.read_csv([path]))
    desc = wr.s3.describe_objects([path])[path]
    if s3_additional_kwargs is None:
        # SSE enabled by default
        assert desc.get("ServerSideEncryption") == "AES256"
    elif s3_additional_kwargs["ServerSideEncryption"] == "aws:kms":
        assert desc.get("ServerSideEncryption") == "aws:kms"
    elif s3_additional_kwargs["ServerSideEncryption"] == "AES256":
        assert desc.get("ServerSideEncryption") == "AES256"


@pytest.mark.parametrize("line_terminator", ["\n", "\r", "\r\n"])
def test_csv_line_terminator(path, line_terminator):
    file_path = f"{path}0.csv"
    df = pd.DataFrame(data={"reading": ["col1", "col2"], "timestamp": [1601379427618, 1601379427625], "value": [1, 2]})
    wr.s3.to_csv(df=df, path=file_path, index=False, lineterminator=line_terminator)
    df2 = wr.s3.read_csv(file_path)
    assert df.equals(df2)


@pytest.mark.modin_index
def test_read_json_versioned(path) -> None:
    path_file = f"{path}0.json"
    dfs = [
        pd.DataFrame({"id": [4, 5, 6], "value": ["foo", "boo", "bar"]}),
        pd.DataFrame({"id": [1, 2, 3], "value": ["a", "b", "c"]}),
    ]
    version_ids = []

    for df in dfs:
        wr.s3.to_json(df=df, path=path_file)
        version_id = wr.s3.describe_objects(path=path_file)[path_file]["VersionId"]
        version_ids.append(version_id)

    for df, version_id in zip(dfs, version_ids):
        df_temp = wr.s3.read_json(path_file, version_id=version_id).reset_index(drop=True)
        assert df_temp.equals(df)
        assert version_id == wr.s3.describe_objects(path=path_file, version_id=version_id)[path_file]["VersionId"]


def test_read_csv_versioned(path) -> None:
    path_file = f"{path}0.csv"
    dfs = [
        pd.DataFrame({"c0": [0, 1, 2], "c1": [3, 4, 5]}),
        pd.DataFrame({"c0": [3, 4, 5], "c1": [6, 7, 8]}),
    ]
    version_ids = []

    for df in dfs:
        wr.s3.to_csv(df=df, path=path_file, index=False)
        version_id = wr.s3.describe_objects(path=path_file)[path_file]["VersionId"]
        version_ids.append(version_id)

    for df, version_id in zip(dfs, version_ids):
        df_temp = wr.s3.read_csv(path_file, version_id=version_id)
        assert df_temp.equals(df)
        assert version_id == wr.s3.describe_objects(path=path_file, version_id=version_id)[path_file]["VersionId"]


@pytest.mark.parametrize("mode", ["append", "overwrite"])
def test_to_csv_schema_evolution(path, glue_database, glue_table, mode) -> None:
    df = pd.DataFrame({"c0": [0, 1, 2], "c1": [3, 4, 5]})
    wr.s3.to_csv(df=df, path=path, dataset=True, database=glue_database, table=glue_table, index=False)

    df["c2"] = [6, 7, 8]
    wr.s3.to_csv(
        df=df,
        path=path,
        dataset=True,
        database=glue_database,
        table=glue_table,
        mode=mode,
        schema_evolution=True,
        index=False,
    )

    column_types = wr.catalog.get_table_types(glue_database, glue_table)
    assert len(column_types) == len(df.columns)

    df["c3"] = [9, 10, 11]
    with pytest.raises(wr.exceptions.InvalidArgumentValue):
        wr.s3.to_csv(df=df, path=path, dataset=True, database=glue_database, table=glue_table, schema_evolution=False)


@pytest.mark.parametrize("schema_evolution", [False, True])
def test_to_csv_schema_evolution_out_of_order(path, glue_database, glue_table, schema_evolution) -> None:
    df = pd.DataFrame({"c0": [0, 1, 2], "c1": [3, 4, 5]})
    wr.s3.to_csv(df=df, path=path, dataset=True, database=glue_database, table=glue_table, index=False)

    df["c2"] = [6, 7, 8]
    df = df[["c0", "c2", "c1"]]

    with pytest.raises(wr.exceptions.InvalidArgumentValue):
        wr.s3.to_csv(
            df=df,
            path=path,
            dataset=True,
            database=glue_database,
            table=glue_table,
            mode="append",
            schema_evolution=schema_evolution,
            index=False,
        )


@pytest.mark.parametrize("mode", ["append", "overwrite"])
def test_to_json_schema_evolution(path, glue_database, glue_table, mode) -> None:
    df = pd.DataFrame({"c0": [0, 1, 2], "c1": [3, 4, 5]})
    wr.s3.to_json(
        df=df,
        path=path,
        dataset=True,
        database=glue_database,
        table=glue_table,
        orient="split",
        index=False,
    )

    df["c2"] = [6, 7, 8]
    wr.s3.to_json(
        df=df,
        path=path,
        dataset=True,
        database=glue_database,
        table=glue_table,
        mode=mode,
        schema_evolution=True,
        orient="split",
        index=False,
    )

    column_types = wr.catalog.get_table_types(glue_database, glue_table)
    assert len(column_types) == len(df.columns)

    df["c3"] = [9, 10, 11]
    with pytest.raises(wr.exceptions.InvalidArgumentValue):
        wr.s3.to_json(df=df, path=path, dataset=True, database=glue_database, table=glue_table, schema_evolution=False)


def test_exceptions(path):
    df = pd.DataFrame({"c0": [1, 2], "c1": ["a", "b"]})
    with pytest.raises(wr.exceptions.InvalidArgument):
        wr.s3.to_csv(df=df, path=path, pandas_kwargs={})

    with pytest.raises(wr.exceptions.InvalidArgument):
        wr.s3.to_json(df=df, path=path, pandas_kwargs={})

    with pytest.raises(wr.exceptions.InvalidArgumentCombination):
        wr.s3.to_json(df=df, path=f"{path}test.pq", dataset=False, bucketing_info=(["c0"], 2))

    with pytest.raises(wr.exceptions.InvalidArgumentValue):
        wr.s3.to_csv(df=df, path=f"{path}test.pq", dataset=True, bucketing_info=(["c0"], -1))

    with pytest.raises(wr.exceptions.InvalidArgumentCombination):
        wr.s3.to_json(df=df, path=f"{path}test.pq", dataset=True, database=None, table="test")

    with pytest.raises(wr.exceptions.InvalidArgumentCombination):
        wr.s3.to_csv(df=df, dataset=True)


@pytest.mark.parametrize(
    "format,write_function,read_function",
    [
        ("csv", wr.s3.to_csv, wr.s3.read_csv),
        ("json", wr.s3.to_json, wr.s3.read_json),
        ("excel", wr.s3.to_excel, wr.s3.read_excel),
    ],
)
@pytest.mark.skipif(condition=not is_pandas_2_x, reason="not pandas 2.x")
def test_s3_text_pyarrow_dtype_backend_roundtrip(path, format, write_function, read_function):
    s3_path = f"{path}test.{format}"

    df = pd.DataFrame(
        {
            "col0": [1, None, 3],
            "col1": [0.0, None, 2.2],
            "col2": [True, None, False],
            "col3": ["Washington", None, "Seattle"],
        }
    )
    # Cast to pyarrow backend types
    df = df.convert_dtypes(dtype_backend="pyarrow")

    write_function(
        df,
        path=s3_path,
        index=False,
    )
    df1 = read_function(s3_path, dtype_backend="pyarrow")

    assert df.equals(df1)
