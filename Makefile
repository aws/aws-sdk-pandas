.PHONY: sam-deploy init clean format lint build docs
.DEFAULT_GOAL := build

# Input variables for deploy the SAM infrastructure used to test (sam-deploy)
# Also can be passed as parameters on the make command
Bucket := BUCKET_FOR_ARTIFACTS
VpcId := VPC_ID_FOR REDSHIFT
SubnetId := SUBNET_ID_FOR REDSHIFT
Password := PASSWORD_FOR_REDSHIFT

init:
	pip install --upgrade pip
	pip install pipenv --upgrade
	pipenv install --dev
	pipenv update

clean:
	rm -rf .tox *.egg-info .pytest_cache build dist htmlcov .coverage docs/build

format:
	black awswrangler tests benchmarks

lint:
	flake8 awswrangler tests benchmarks

tox:
	tox

test:
	pytest tests --cov awswrangler --cov-report=term-missing

coverage:
	coverage report --show-missing

coverage-html:
	coverage html

artifacts: format lint generate-glue-egg generate-layer-3.6 generate-layer-3.7

generate-glue-egg:
	python3.6 setup.py bdist_egg

generate-layer-3.6:
	docker run -v $(PWD):/var/task -it lambci/lambda:build-python3.6 /bin/bash ./build-lambda-layer.sh 3.6

generate-layer-3.7:
	docker run -v $(PWD):/var/task -it lambci/lambda:build-python3.6 /bin/bash ./build-lambda-layer.sh 3.7

docs:
	sphinx-apidoc -f -H "API Reference" -o docs/source/api awswrangler/
	make -C docs/ html

build:
	rm -fr build dist .egg requests.egg-info
	python setup.py sdist bdist_wheel

publish:
	twine upload dist/*
	rm -fr build dist .egg requests.egg-info

sam-deploy:
	rm -rf .aws-sam
	sam build --template tests/sam/template.yaml
	sam package --output-template-file .aws-sam/packaged.yaml --s3-bucket $(Bucket)
	sam deploy --template-file .aws-sam/packaged.yaml --stack-name aws-data-wrangler-test-arena \
	--capabilities CAPABILITY_IAM \
	--parameter-overrides VpcId=$(VpcId) SubnetId=$(SubnetId) Password=$(Password)
