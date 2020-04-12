#!/usr/bin/env bash
set -ex

rm -rf  /aws-data-wrangler/dist/pyarrow_files

pushd arrow/python

export ARROW_PRE_0_15_IPC_FORMAT=0
export PYARROW_WITH_HDFS=0
export PYARROW_WITH_FLIGHT=0
export PYARROW_WITH_GANDIVA=0
export PYARROW_WITH_ORC=0
export PYARROW_WITH_CUDA=0
export PYARROW_WITH_PARQUET=1

python setup.py build_ext \
  --build-type=release \
  --bundle-arrow-cpp \
  bdist_wheel

pip install dist/pyarrow-*.whl -t /aws-data-wrangler/dist/pyarrow_files

rm -rf dist
