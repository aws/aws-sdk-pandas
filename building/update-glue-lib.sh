#!/usr/bin/env bash
set -ex

pushd ..
rm -fr dist

# Poetry will package the library into a TAR file (we need ZIP)
poetry build -f sdist

pushd dist

# Unzip the TAR archive
tar_file=$(ls | head -n 1)
tar xf $tar_file
rm $tar_file

# Re-archive the library as a ZIP file
dir_name=$(ls | head -n 1)
zip -r awswrangler.zip $dir_name

# Upload the ZIP file
s3_location=$(aws cloudformation describe-stacks --stack-name aws-sdk-pandas-glueray --query "Stacks[0].Outputs[?OutputKey=='AWSSDKforpandasZIPLocation'].OutputValue" --output text)
aws s3 cp awswrangler.zip $s3_location

popd

rm -fr dist
