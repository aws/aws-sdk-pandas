#!/usr/bin/env bash
set -ex

pushd ..
rm -rf dist/*.whl
poetry build -f wheel
