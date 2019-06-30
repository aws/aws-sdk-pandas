#!/bin/bash

cd ..
black awswrangler tests
flake8 awswrangler tests
pip install -e .
pytest tests awswrangler
rm -rf .pytest_cache
cd tests
