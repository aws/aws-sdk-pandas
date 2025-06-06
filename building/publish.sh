#!/usr/bin/env bash
set -ex

pushd ..
rm -rf dist/*.whl
uv build --wheel
uv publish dist/*.whl
