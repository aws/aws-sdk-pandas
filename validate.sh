#!/usr/bin/env bash
set -ex

black --check .
ruff . --ignore "PL" --ignore "D"
ruff awswrangler
mypy --install-types --non-interactive awswrangler
pylint -j 0 --disable=all --enable=R0913,R0915 awswrangler
doc8 --ignore D005,D002 --max-line-length 120 docs/source
