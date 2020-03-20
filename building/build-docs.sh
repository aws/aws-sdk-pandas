#!/usr/bin/env bash
set -ex

pushd ..
make -C docs/ html
doc8 --ignore D005 docs/source
