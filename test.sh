#!/usr/bin/env bash
versions=${1:-ALL}
posargs=${2:-32}
set -e

microtime() {
    python -c 'import time; print(time.time())'
}

START=$(microtime)

./validate.sh
mkdir -p test-reports
tox -e ${versions} -- ${posargs}
coverage html --directory test-reports/coverage
rm -rf .coverage* Running

echo "Time elapsed: $(echo "scale=1; ($(microtime) - $START) / 60" | bc) minutes"