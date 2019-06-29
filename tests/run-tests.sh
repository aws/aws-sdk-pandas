#!/bin/bash

cd ..
black awswrangler tests
flake8 awswrangler tests
pip install -e .
pytest tests --cov awswrangler --cov-report=term-missing
coverage html
rm -rf .tox .pytest_cache .coverage .coverage.*
cd tests
