#!/usr/bin/env bash
set -ex

pushd ..
rm -rf dist/*.whl
uv build --wheel --sdist
uv publish dist/*.whl
