#!/bin/bash
set -e

cp ../requirements.txt .
cp ../requirements-dev.txt .
docker build -t awswrangler-building .
