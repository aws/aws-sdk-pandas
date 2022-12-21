import logging

import botocore
import pandas as pd
import pytest

import awswrangler as wr

logging.getLogger("awswrangler").setLevel(logging.DEBUG)


@pytest.mark.parametrize("use_threads", [True, False])
def test_wait_object_exists_single_file(path: str, use_threads: bool) -> None:
    df = pd.DataFrame({"FooBoo": [1, 2, 3]})
    file_path = f"{path}data.csv"

    wr.s3.to_csv(df, file_path)

    wr.s3.wait_objects_exist(paths=[file_path], use_threads=use_threads)


@pytest.mark.parametrize("use_threads", [True, False])
def test_wait_object_exists_multiple_files(path: str, use_threads: bool) -> None:
    df = pd.DataFrame({"FooBoo": [1, 2, 3]})

    file_paths = [f"{path}data.csv", f"{path}data2.csv", f"{path}data3.csv"]
    for file_path in file_paths:
        wr.s3.to_csv(df, file_path)

    wr.s3.wait_objects_exist(paths=file_paths, use_threads=use_threads)


@pytest.mark.parametrize("use_threads", [True, False])
def test_wait_object_not_exists(path: str, use_threads: bool) -> None:
    wr.s3.wait_objects_not_exist(paths=[path], use_threads=use_threads)


@pytest.mark.parametrize("use_threads", [True, False])
@pytest.mark.timeout(5)
def test_wait_object_timeout(path: str, use_threads: bool) -> None:
    with pytest.raises(botocore.exceptions.WaiterError):
        wr.s3.wait_objects_exist(
            paths=[path],
            use_threads=use_threads,
            delay=0.5,
            max_attempts=3,
        )


@pytest.mark.parametrize("use_threads", [True, False])
def test_wait_object_exists_empty_list(use_threads: bool) -> None:
    wr.s3.wait_objects_exist(paths=[])
