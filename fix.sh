#!/usr/bin/env bash
set -ex

black .
ruff --fix --select "I001" --select "I002" awswrangler