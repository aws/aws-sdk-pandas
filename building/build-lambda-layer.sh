#!/bin/bash

# Go to home
cd ~

# Clone desired Arrow version
rm -rf arrow dist pyarrow*
git clone \
    --branch apache-arrow-0.14.0 \
    --single-branch \
    https://github.com/apache/arrow.git

# Install dependencies
yum install -y \
    boost-devel \
    jemalloc-devel \
    bison \
    flex \
    autoconf \
    python36-devel
pip install six numpy pandas cython pytest cmake wheel

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
    -DARROW_PARQUET=ON \
    -DARROW_PYTHON=ON \
    -DARROW_PLASMA=OFF \
    -DARROW_BUILD_TESTS=ON \
    ..
make -j4
make install
popd

# Build Pyarrow
export PYARROW_WITH_FLIGHT=0
export PYARROW_WITH_GANDIVA=0
export PYARROW_WITH_ORC=0
export PYARROW_WITH_PARQUET=1
pushd arrow/python
python setup.py build_ext \
    --build-type=release \
    --bundle-arrow-cpp \
    bdist_wheel
cp dist/pyarrow-*.whl ~
popd

# Extracting files
pip install pyarrow-*whl  -t pyarrow_files

# Go back to AWSWRANGLER directory
cd /aws-data-wrangler/

# Preparing directories
mkdir -p dist
rm -rf python
rm -f "awswrangler_layer.zip"
rm -f "dist/awswrangler_layer.zip"

# Building
pip install . -t ./python
rm -rf python/pyarrow*
cp -r ~/pyarrow_files/pyarrow* python/
zip -r "awswrangler_layer.zip" ./python
mv "awswrangler_layer.zip" dist/

# # Cleaning up the directory again
rm -rf python

cd building
