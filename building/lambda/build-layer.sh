#!/usr/bin/env bash
set -ex

FILENAME="awswrangler-layer-${1}.zip"

pushd /aws-data-wrangler

mkdir -p dist
rm -rf python "${FILENAME}" "dist/${FILENAME}"

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

rm -rf python
