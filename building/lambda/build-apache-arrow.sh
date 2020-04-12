#!/usr/bin/env bash
set -ex

rm -rf arrow dist

git clone \
  --branch apache-arrow-0.16.0 \
  --single-branch \
  https://github.com/apache/arrow.git

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
make -j
make install

popd

rm -rf arrow
