import os
from io import open
from typing import Dict

from setuptools import find_packages, setup

here = os.path.abspath(os.path.dirname(__file__))
about: Dict[str, str] = {}
path = os.path.join(here, "awswrangler", "__metadata__.py")
with open(file=path, mode="r", encoding="utf-8") as f:
    exec(f.read(), about)

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setup(
    author="Igor Tavares",
    url="https://github.com/awslabs/aws-data-wrangler",
    name=about["__title__"],
    version=about["__version__"],
    description=about["__description__"],
    long_description=long_description,
    long_description_content_type="text/markdown",
    license=about["__license__"],
    packages=find_packages(exclude=["tests"]),
    include_package_data=True,
    python_requires=">=3.6, <3.10",
    install_requires=open("requirements.txt").read().strip().split("\n"),
    classifiers=[
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
    ],
    extras_require={"sqlserver": ["pyodbc~=4.0.30"]},
)
