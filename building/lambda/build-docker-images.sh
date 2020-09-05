#!/usr/bin/env bash
set -ex

cp ../../requirements.txt .
cp ../../requirements-dev.txt .

# Python 3.6
docker build \
  --no-cache \
  --pull \
  --tag awswrangler-build-py36 \
  --build-arg base_image=lambci/lambda:build-python3.6 \
  --build-arg py_dev=python36-devel \
  .

# Python 3.7
docker build \
  --no-cache \
  --pull \
  --tag awswrangler-build-py37 \
  --build-arg base_image=lambci/lambda:build-python3.7 \
  --build-arg py_dev=python37-devel \
  .

# Python 3.8
docker build \
  --no-cache \
  --pull \
  --tag awswrangler-build-py38 \
  --build-arg base_image=lambci/lambda:build-python3.8 \
  --build-arg py_dev=python38-devel \
  .

rm -rf requirements*
