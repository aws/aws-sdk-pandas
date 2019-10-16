#!/bin/bash

set -e

cd ..
pip install -e .
yapf --in-place --recursive setup.py awswrangler testing/test_awswrangler
mypy awswrangler
flake8 setup.py awswrangler testing/test_awswrangler
pytest --cov=awswrangler testing/test_awswrangler
cd testing
