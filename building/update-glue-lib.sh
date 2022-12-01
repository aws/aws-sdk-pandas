#!/usr/bin/env bash
set -ex

pushd ..
rm -fr dist
poetry build -f sdist

pushd dist

tar_file=$(ls | head -n 1)
tar xf $tar_file
rm $tar_file

dir_name=$(ls | head -n 1)
zip -r awswrangler.zip $dir_name

s3_location=$(aws cloudformation describe-stacks --stack-name aws-sdk-pandas-glueray --query "Stacks[0].Outputs[?ExportName=='WranglerZipLocation'].OutputValue" --output text)
aws s3 cp awswrangler.zip $s3_location

popd

rm -fr dist
