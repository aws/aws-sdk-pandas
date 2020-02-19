#!/usr/bin/env bash
set -ex

pushd ..
make -C docs/ html
doc8 --ignore D001,D002,D005 docs/source
echo DONE!
