#!/usr/bin/env bash
set -ex

isort --check .
black --check .
yes y | mypy --install-types awswrangler
flake8 .
pylint -j 0 awswrangler
pydocstyle awswrangler/ --convention=numpy
doc8 --ignore D005,D002 --max-line-length 120 docs/source
