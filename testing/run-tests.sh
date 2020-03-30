#!/usr/bin/env bash
set -ex

microtime() {
    python -c 'import time; print(time.time())'
}

START=$(microtime)

./run-validations.sh
pushd ..
tox -e py36
coverage html --directory testing/coverage
rm -rf .coverage* testing/Running Running

echo "Time elapsed: $(echo "scale=1; ($(microtime) - $START) / 60" | bc) minutes"