#!/bin/bash
set -e

cd ..
rm -rf *.egg-info build dist/*.whl
python3.6 setup.py bdist_wheel
rm -rf *.egg-info build
cd building
