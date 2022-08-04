import pytest  # type: ignore

from ._utils import extract_cloudformation_outputs, path_generator


@pytest.fixture(scope="session")
def cloudformation_outputs():
    return extract_cloudformation_outputs()


@pytest.fixture(scope="session")
def bucket(cloudformation_outputs):
    return cloudformation_outputs["BucketName"]


@pytest.fixture(scope="function")
def path(bucket):
    yield from path_generator(bucket)


@pytest.fixture(scope="function")
def path2(bucket):
    yield from path_generator(bucket)


@pytest.fixture(scope="function")
def path3(bucket):
    yield from path_generator(bucket)
