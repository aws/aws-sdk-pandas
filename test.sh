#!/usr/bin/env bash
set -ex

microtime() {
    python -c 'import time; print(time.time())'
}

START=$(microtime)

./validate.sh
tox -e ALL
coverage html --directory coverage
rm -rf .coverage* Running

echo "Time elapsed: $(echo "scale=1; ($(microtime) - $START) / 60" | bc) minutes"