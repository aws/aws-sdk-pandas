#!/usr/bin/env bash
set -ex

isort -rc awswrangler tests
black --line-length 120 --target-version py36 awswrangler tests
pydocstyle awswrangler/ --add-ignore=D204,D403
mypy awswrangler
flake8 setup.py awswrangler tests
pylint -j 0 awswrangler
