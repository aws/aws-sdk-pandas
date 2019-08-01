import os
from io import open
from setuptools import setup, find_packages

here = os.path.abspath(os.path.dirname(__file__))
about = {}
path = os.path.join(here, "awswrangler", "__version__.py")
with open(file=path, mode="r", encoding="utf-8") as f:
    exec(f.read(), about)

with open("README.md", "r") as fh:
    long_description = fh.read()

setup(
    name=about["__title__"],
    version=about["__version__"],
    description=about["__description__"],
    long_description=long_description,
    long_description_content_type="text/markdown",
    license=about["__license__"],
    packages=find_packages(include=["awswrangler", "awswrangler.*"],
                           exclude=["tests"]),
    python_requires=">=3.6",
    install_requires=[
        "pyarrow>=0.14.0",
        "pandas>=0.24.2",
        "boto3>=1.9.130",
        "s3fs>=0.3.1",
        "tenacity>=5.0.4",
        "pg8000>=1.13.2",
    ],
)
