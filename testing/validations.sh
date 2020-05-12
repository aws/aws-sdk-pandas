#!/usr/bin/env bash
set -ex

cfn-lint -t cloudformation.yaml
rm -rf temp.yaml
cfn-flip -c -l -n cloudformation.yaml temp.yaml
cfn-lint -t temp.yaml
mv temp.yaml cloudformation.yaml
pushd ..
black --line-length 120 --target-version py36 awswrangler testing/test_awswrangler
isort -rc --line-width 120 awswrangler testing/test_awswrangler
pydocstyle awswrangler/ --add-ignore=D204,D403
mypy awswrangler
flake8 setup.py awswrangler testing/test_awswrangler
pylint -j 0 awswrangler
