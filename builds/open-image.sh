#!/usr/bin/env bash

docker run \
    --workdir /aws-data-wrangler/builds \
    -v $(dirname $PWD):/aws-data-wrangler/ \
    -it \
    --entrypoint /bin/bash \
    awswrangler-builds