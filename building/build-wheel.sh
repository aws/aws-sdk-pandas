#!/usr/bin/env bash
set -ex

pushd ..
rm -rf *.egg-info build dist/*.whl
python setup.py bdist_wheel
rm -rf *.egg-info build
