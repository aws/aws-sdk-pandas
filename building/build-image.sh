#!/usr/bin/env bash
set -ex

cp ../requirements.txt .
docker build -t awswrangler-building .
rm -rf requirements*
