#!/usr/bin/env bash
set -ex

cp ../requirements.txt .
cp ../requirements-dev.txt .
docker build -t awswrangler-testing .
rm -rf requirements*
