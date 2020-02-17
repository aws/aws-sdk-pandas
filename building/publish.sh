#!/usr/bin/env bash
set -ex

cd ..
rm -fr build dist .egg awswrangler.egg-info
python3.6 setup.py bdist_egg
python3.6 setup.py bdist_wheel
python3.6 setup.py sdist
twine upload dist/*
rm -fr build dist .egg awswrangler.egg-info
