#!/bin/bash
set -e

cd ..
rm -fr build dist .egg awswrangler.egg-info
python setup.py sdist
twine upload dist/*
rm -fr build dist .egg awswrangler.egg-info
