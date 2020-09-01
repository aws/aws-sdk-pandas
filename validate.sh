#!/usr/bin/env bash
set -ex

isort awswrangler tests
black --line-length 120 --target-version py36 awswrangler tests
pydocstyle awswrangler/ --convention=numpy
mypy awswrangler
flake8 setup.py awswrangler tests
pylint -j 0 awswrangler
