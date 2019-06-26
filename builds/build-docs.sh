#!/usr/bin/env bash

cd ..
sphinx-apidoc -f -H "API Reference" -o docs/source/api awswrangler/
make -C docs/ html
cd build
echo DONE!