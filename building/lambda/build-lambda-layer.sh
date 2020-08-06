#!/usr/bin/env bash
set -ex

FILENAME="awswrangler-layer-${1}.zip"
NINJA=${2}

pushd /aws-data-wrangler
rm -rf python dist/pyarrow_files "dist/${FILENAME}" "${FILENAME}"
popd

rm -rf dist arrow

export ARROW_HOME=$(pwd)/dist
export LD_LIBRARY_PATH=$(pwd)/dist/lib:$LD_LIBRARY_PATH

git clone \
  --branch apache-arrow-1.0.0 \
  --single-branch \
  https://github.com/apache/arrow.git

mkdir dist
mkdir arrow/cpp/build
pushd arrow/cpp/build

cmake \
    -DCMAKE_INSTALL_PREFIX=$ARROW_HOME \
    -DCMAKE_INSTALL_LIBDIR=lib \
    -DARROW_PYTHON=ON \
    -DARROW_PARQUET=ON \
    -DARROW_WITH_SNAPPY=ON \
    -DARROW_WITH_ZLIB=ON \
    -DARROW_FLIGHT=OFF \
    -DARROW_GANDIVA=OFF \
    -DARROW_ORC=OFF \
    -DARROW_CSV=OFF \
    -DARROW_PLASMA=OFF \
    -DARROW_WITH_BZ2=OFF \
    -DARROW_WITH_ZSTD=OFF \
    -DARROW_WITH_LZ4=OFF \
    -DARROW_WITH_BROTLI=OFF \
    -DARROW_BUILD_TESTS=OFF \
    -GNinja \
    ..

eval $NINJA
eval "${NINJA} install"

popd

pushd arrow/python

export ARROW_PRE_0_15_IPC_FORMAT=0
export PYARROW_WITH_HDFS=0
export PYARROW_WITH_FLIGHT=0
export PYARROW_WITH_GANDIVA=0
export PYARROW_WITH_ORC=0
export PYARROW_WITH_CUDA=0
export PYARROW_WITH_PLASMA=0
export PYARROW_WITH_PARQUET=1

python setup.py build_ext \
  --build-type=release \
  --bundle-arrow-cpp \
  bdist_wheel

pip install dist/pyarrow-*.whl -t /aws-data-wrangler/dist/pyarrow_files

popd

pushd /aws-data-wrangler

pip install . -t ./python

rm -rf python/pyarrow*
rm -rf python/boto*

rm -f /aws-data-wrangler/dist/pyarrow_files/pyarrow/libarrow.so
rm -f /aws-data-wrangler/dist/pyarrow_files/pyarrow/libparquet.so
rm -f /aws-data-wrangler/dist/pyarrow_files/pyarrow/libarrow_python.so

cp -r /aws-data-wrangler/dist/pyarrow_files/pyarrow* python/

find python -wholename "*/tests/*" -type f -delete

zip -r9 "${FILENAME}" ./python
mv "${FILENAME}" dist/

rm -rf python dist/pyarrow_files "${FILENAME}"

popd

rm -rf dist arrow
