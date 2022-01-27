#!/usr/bin/env bash

pushd ..

# define stacks to validate
stacks=( base lakeformation databases opensearch )

# run cdk-nag against each stack
for stack in "${stacks[@]}"
do
  rm -rf cdk.out || true
	echo "[*] running cfn-nag for stack: $stack"
  cdk synth aws-data-wrangler-${stack} | cfn_nag
done

popd
