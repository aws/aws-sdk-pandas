#!/usr/bin/env bash
set -ex

pushd ..
rm -fr dist
poetry publish --build
rm -fr dist
