#!/bin/bash

cd ..
rm -fr build dist .egg awswrangler.egg-info
python setup.py sdist bdist_wheel
twine upload dist/*
rm -fr build dist .egg awswrangler.egg-info
