#!/usr/bin/env bash
set -ex

pushd ..
rm -rf *.egg-info build dist/*.whl
python3.6 setup.py bdist_wheel
rm -rf *.egg-info build
