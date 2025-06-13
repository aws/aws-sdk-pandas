#!/usr/bin/env bash
set -ex

ruff format --check .
ruff check .
mypy --install-types --non-interactive awswrangler
doc8 --ignore-path docs/source/stubs --max-line-length 120 docs/source
uv lock --check
