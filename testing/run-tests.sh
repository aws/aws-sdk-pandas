#!/usr/bin/env bash
set -ex

pushd ..
yapf --in-place --recursive --parallel setup.py awswrangler testing/test_awswrangler
isort -rc --line-width 120 awswrangler testing/test_awswrangler
pydocstyle awswrangler/ --add-ignore=D204
mypy awswrangler
flake8 setup.py awswrangler testing/test_awswrangler
pip install --upgrade -e .
pytest --cov=awswrangler testing/test_awswrangler
coverage html --directory testing/coverage
rm -rf .coverage*
