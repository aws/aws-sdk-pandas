#!/bin/bash

cd ..
rm -fr build dist .egg requests.egg-info
python setup.py sdist bdist_wheel
twine upload dist/*
rm -fr build dist .egg requests.egg-info
