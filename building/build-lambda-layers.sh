#!/usr/bin/env bash
set -ex

VERSION=$(python -c "import awswrangler as wr; print(wr.__version__)")
DIR_NAME=$(dirname "$PWD")

ARCH=$(arch)
[ "${ARCH}" = "aarch64" ] && ARCH_SUFFIX="-arm64" # AWS Lambda, the name arm64 is used instead of aarch64

echo "Building Lambda Layers for AWS Data Wrangler ${VERSION}"

pushd lambda

# Building all related docker images
./build-docker-images.sh

if [ "${ARCH}" != "aarch64" ]; then # https://docs.aws.amazon.com/lambda/latest/dg/lambda-runtimes.html
  # Python 3.7
  docker run \
    --volume "$DIR_NAME":/aws-data-wrangler/ \
    --workdir /aws-data-wrangler/building/lambda \
    --rm \
    awswrangler-build-py37 \
    build-lambda-layer.sh "${VERSION}-py3.7" "ninja-build"
fi

# Python 3.8
docker run \
  --volume "$DIR_NAME":/aws-data-wrangler/ \
  --workdir /aws-data-wrangler/building/lambda \
  --rm \
  awswrangler-build-py38 \
  build-lambda-layer.sh "${VERSION}-py3.8${ARCH_SUFFIX}" "ninja-build"

# Python 3.9
docker run \
  --volume "$DIR_NAME":/aws-data-wrangler/ \
  --workdir /aws-data-wrangler/building/lambda \
  --rm \
  awswrangler-build-py39 \
  build-lambda-layer.sh "${VERSION}-py3.9${ARCH_SUFFIX}" "ninja-build"
