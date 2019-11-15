#!/usr/bin/env bash
set -e

cd ..
yapf --in-place --recursive setup.py awswrangler testing/test_awswrangler
mypy awswrangler
flake8 setup.py awswrangler testing/test_awswrangler
pip install --upgrade -e .
pytest --cov=awswrangler testing/test_awswrangler
cd testing
