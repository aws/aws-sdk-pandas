#!/usr/bin/env bash
set -ex

cp ../../pyproject.toml .
cp ../../poetry.lock .

# Python 3.6
docker build \
  --pull \
  --tag awswrangler-build-py36 \
  --build-arg base_image=lambci/lambda:build-python3.6 \
  --build-arg py_dev=python36-devel \
  .

# Python 3.7
docker build \
  --pull \
  --tag awswrangler-build-py37 \
  --build-arg base_image=lambci/lambda:build-python3.7 \
  --build-arg py_dev=python37-devel \
  .

# Python 3.8
docker build \
  --pull \
  --tag awswrangler-build-py38 \
  --build-arg base_image=lambci/lambda:build-python3.8 \
  --build-arg py_dev=python38-devel \
  .

# Python 3.9
docker build \
  --pull \
  --tag awswrangler-build-py39 \
  --build-arg base_image=public.ecr.aws/sam/build-python3.9:latest \
  --build-arg py_dev=python39-devel \
  .

rm -rf pyproject.toml poetry.lock
