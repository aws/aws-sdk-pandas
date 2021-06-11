import logging

import pandas as pd
import pytest

import awswrangler as wr

logging.getLogger("awswrangler").setLevel(logging.DEBUG)


@pytest.mark.parametrize("use_threads", [True, False, 2])
def test_full_table(path, use_threads):
    df = pd.DataFrame({"c0": [1, 2, 3], "c1": ["foo", "boo", "bar"], "c2": [4.0, 5.0, 6.0]})

    # Parquet
    file_path = f"{path}test_parquet_file.snappy.parquet"
    wr.s3.to_parquet(df, file_path, compression="snappy")
    df2 = wr.s3.select_query(
        sql="select * from s3object",
        path=file_path,
        input_serialization="Parquet",
        input_serialization_params={},
        use_threads=use_threads,
    )
    assert df.equals(df2)

    # CSV
    file_path = f"{path}test_csv_file.csv"
    wr.s3.to_csv(df, file_path, index=False)
    df3 = wr.s3.select_query(
        sql="select * from s3object",
        path=file_path,
        input_serialization="CSV",
        input_serialization_params={"FileHeaderInfo": "Use", "RecordDelimiter": "\n"},
        use_threads=use_threads,
    )
    assert len(df.index) == len(df3.index)
    assert list(df.columns) == list(df3.columns)
    assert df.shape == df3.shape

    # JSON
    file_path = f"{path}test_json_file.json"
    wr.s3.to_json(df, file_path, orient="records")
    df4 = wr.s3.select_query(
        sql="select * from s3object[*][*]",
        path=file_path,
        input_serialization="JSON",
        input_serialization_params={"Type": "Document"},
        use_threads=use_threads,
    )
    assert df.equals(df4)


@pytest.mark.parametrize("use_threads", [True, False, 2])
def test_push_down(path, use_threads):
    df = pd.DataFrame({"c0": [1, 2, 3], "c1": ["foo", "boo", "bar"], "c2": [4.0, 5.0, 6.0]})

    file_path = f"{path}test_parquet_file.snappy.parquet"
    wr.s3.to_parquet(df, file_path, compression="snappy")
    df2 = wr.s3.select_query(
        sql='select * from s3object s where s."c0" = 1',
        path=file_path,
        input_serialization="Parquet",
        input_serialization_params={},
        use_threads=use_threads,
    )
    assert df2.shape == (1, 3)
    assert df2.c0.sum() == 1

    file_path = f"{path}test_parquet_file.gzip.parquet"
    wr.s3.to_parquet(df, file_path, compression="gzip")
    df2 = wr.s3.select_query(
        sql='select * from s3object s where s."c0" = 99',
        path=file_path,
        input_serialization="Parquet",
        input_serialization_params={},
        use_threads=use_threads,
    )
    assert df2.shape == (0, 0)

    file_path = f"{path}test_csv_file.csv"
    wr.s3.to_csv(df, file_path, header=False, index=False)
    df3 = wr.s3.select_query(
        sql='select s."_1" from s3object s limit 2',
        path=file_path,
        input_serialization="CSV",
        input_serialization_params={"FileHeaderInfo": "None", "RecordDelimiter": "\n"},
        use_threads=use_threads,
    )
    assert df3.shape == (2, 1)

    file_path = f"{path}test_json_file.json"
    wr.s3.to_json(df, file_path, orient="records")
    df4 = wr.s3.select_query(
        sql="select count(*) from s3object[*][*]",
        path=file_path,
        input_serialization="JSON",
        input_serialization_params={"Type": "Document"},
        use_threads=use_threads,
    )
    assert df4.shape == (1, 1)
    assert df4._1.sum() == 3


@pytest.mark.parametrize("compression", ["gzip", "bz2"])
def test_compression(path, compression):
    df = pd.DataFrame({"c0": [1, 2, 3], "c1": ["foo", "boo", "bar"], "c2": [4.0, 5.0, 6.0]})

    # CSV
    file_path = f"{path}test_csv_file.csv"
    wr.s3.to_csv(df, file_path, index=False, compression=compression)
    df2 = wr.s3.select_query(
        sql="select * from s3object",
        path=file_path,
        input_serialization="CSV",
        input_serialization_params={"FileHeaderInfo": "Use", "RecordDelimiter": "\n"},
        compression="bzip2" if compression == "bz2" else compression,
        use_threads=False,
    )
    assert len(df.index) == len(df2.index)
    assert list(df.columns) == list(df2.columns)
    assert df.shape == df2.shape

    # JSON
    file_path = f"{path}test_json_file.json"
    wr.s3.to_json(df, file_path, orient="records", compression=compression)
    df3 = wr.s3.select_query(
        sql="select * from s3object[*][*]",
        path=file_path,
        input_serialization="JSON",
        input_serialization_params={"Type": "Document"},
        compression="bzip2" if compression == "bz2" else compression,
        use_threads=False,
    )
    assert df.equals(df3)


@pytest.mark.parametrize(
    "s3_additional_kwargs",
    [None, {"ServerSideEncryption": "AES256"}, {"ServerSideEncryption": "aws:kms", "SSEKMSKeyId": None}],
)
def test_encryption(path, kms_key_id, s3_additional_kwargs):
    if s3_additional_kwargs is not None and "SSEKMSKeyId" in s3_additional_kwargs:
        s3_additional_kwargs["SSEKMSKeyId"] = kms_key_id

    df = pd.DataFrame({"c0": [1, 2, 3], "c1": ["foo", "boo", "bar"], "c2": [4.0, 5.0, 6.0]})
    file_path = f"{path}test_parquet_file.snappy.parquet"
    wr.s3.to_parquet(
        df,
        file_path,
        compression="snappy",
        s3_additional_kwargs=s3_additional_kwargs,
    )
    df2 = wr.s3.select_query(
        sql="select * from s3object",
        path=file_path,
        input_serialization="Parquet",
        input_serialization_params={},
        use_threads=False,
    )
    assert df.equals(df2)
