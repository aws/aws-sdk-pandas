#!/usr/bin/env bash
set -e

cd ..
sphinx-apidoc --separate -f -H "API Reference" -o docs/source/api awswrangler/
make -C docs/ html
cd building
echo DONE!
