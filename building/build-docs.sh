#!/usr/bin/env bash
set -ex

pushd ..
rm -rf docs/build docs/source/stubs
for notebook in tutorials/*.ipynb; do
  filename=${notebook##*/}
  basename=${filename%.ipynb}
  echo $pwd
  echo "{\"path\": \"../../../tutorials/${filename}\",\"extra-media\": [\"../../../tutorials/_static\"]}" > "docs/source/tutorials/${basename}.nblink"
done
make -C docs/ html
doc8 --ignore D005,D002 --max-line-length 120 docs/source
