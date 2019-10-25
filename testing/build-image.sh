#!/usr/bin/env bash
set -e

cp ../requirements.txt .
cp ../requirements-dev.txt .
docker build -t awswrangler-testing .
