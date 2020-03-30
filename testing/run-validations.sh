#!/usr/bin/env bash
set -ex

pushd ..
black --line-length 120 --target-version py36 awswrangler testing/test_awswrangler
isort -rc --line-width 120 awswrangler testing/test_awswrangler
pydocstyle awswrangler/ --add-ignore=D204
mypy awswrangler
flake8 setup.py awswrangler testing/test_awswrangler
pylint awswrangler
