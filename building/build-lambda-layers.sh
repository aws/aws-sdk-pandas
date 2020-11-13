#!/usr/bin/env bash
set -ex

VERSION=$(python -c "import awswrangler as wr; print(wr.__version__)")
DIR_NAME=$(dirname "$PWD")

echo "Building Lambda Layers for AWS Data Wrangler ${VERSION}"

pushd lambda

# Building all related docker images
./build-docker-images.sh

# Python 3.6
docker run \
 --volume "$DIR_NAME":/aws-data-wrangler/ \
 --workdir /aws-data-wrangler/building/lambda \
 --rm \
 -it \
 awswrangler-build-py36 \
 build-lambda-layer.sh "${VERSION}-py3.6" "ninja"

# Python 3.7
docker run \
 --volume "$DIR_NAME":/aws-data-wrangler/ \
 --workdir /aws-data-wrangler/building/lambda \
 --rm \
 -it \
 awswrangler-build-py37 \
 build-lambda-layer.sh "${VERSION}-py3.7" "ninja"

# Python 3.8
docker run \
 --volume "$DIR_NAME":/aws-data-wrangler/ \
 --workdir /aws-data-wrangler/building/lambda \
 --rm \
 -it \
 awswrangler-build-py38 \
 build-lambda-layer.sh "${VERSION}-py3.8" "ninja-build"
