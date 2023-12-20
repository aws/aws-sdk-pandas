#!/usr/bin/env bash
set -ex

black .
ruff --fix awswrangler