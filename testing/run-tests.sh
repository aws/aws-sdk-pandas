#!/bin/bash

cd ..
rm -rf *.pytest_cache
yapf --in-place  --recursive setup.py awswrangler testing/test_awswrangler
flake8 setup.py awswrangler testing/test_awswrangler
pip install -e .
pytest testing/test_awswrangler awswrangler
rm -rf *.pytest_cache
cd tests
