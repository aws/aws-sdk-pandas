#!/usr/bin/env bash
set -e

pushd ..
rm -rf *.egg-info build dist/*.egg
python3.6 setup.py bdist_egg
rm -rf *.egg-info build
