.PHONY: init clean format lint build docs
.DEFAULT_GOAL := build

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
