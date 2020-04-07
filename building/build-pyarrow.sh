#!/usr/bin/env bash
set -ex

# Go to home
rm -rf \
  arrow \
  dist \
  /aws-data-wrangler/dist/pyarrow_wheels \
  /aws-data-wrangler/dist/pyarrow_files \

# Clone desired Arrow version
git clone \
  --branch apache-arrow-0.16.0 \
  --single-branch \
  https://github.com/apache/arrow.git

# Build Arrow
export ARROW_HOME=$(pwd)/dist
export LD_LIBRARY_PATH=$(pwd)/dist/lib:$LD_LIBRARY_PATH
mkdir dist
mkdir arrow/cpp/build
pushd arrow/cpp/build
cmake \
    -DCMAKE_INSTALL_PREFIX=$ARROW_HOME \
    -DCMAKE_INSTALL_LIBDIR=lib \
    -DARROW_FLIGHT=OFF \
    -DARROW_GANDIVA=OFF \
    -DARROW_ORC=OFF \
    -DARROW_WITH_SNAPPY=ON \
    -DARROW_WITH_ZLIB=ON \
    -DARROW_PARQUET=ON \
    -DARROW_CSV=OFF \
    -DARROW_PYTHON=ON \
    -DARROW_PLASMA=OFF \
    -DARROW_BUILD_TESTS=OFF \
    ..
make -j4
make install
popd

# Build Pyarrow
export ARROW_PRE_0_15_IPC_FORMAT=0
export PYARROW_WITH_HDFS=0
export PYARROW_WITH_FLIGHT=0
export PYARROW_WITH_GANDIVA=0
export PYARROW_WITH_ORC=0
export PYARROW_WITH_CUDA=0
export PYARROW_WITH_PARQUET=1
pushd arrow/python
python setup.py build_ext \
    --build-type=release \
    --bundle-arrow-cpp \
    bdist_wheel
mkdir -p /aws-data-wrangler/dist/pyarrow_wheels
cp -r dist/pyarrow-*.whl /aws-data-wrangler/dist/pyarrow_wheels/
popd

# Extracting files
pip install /aws-data-wrangler/dist/pyarrow_wheels/pyarrow-*whl -t /aws-data-wrangler/dist/pyarrow_files

# Cleaning Up
rm -rf arrow dist
