.PHONY: init format lint build
.DEFAULT_GOAL := build

init:
	pip install --upgrade pip
	pip install pipenv --upgrade
	pipenv install --dev
	pipenv update

format:
	black awswrangler tests benchmarks

lint:
	flake8 awswrangler tests benchmarks

test:
	tox

coverage:
	pytest tests --cov awswrangler --cov-report=term-missing

coverage-report:
	coverage report --show-missing

coverage-report-html:
	coverage html

artifacts: format lint generate-glue-eggs generate-layers-3.7 generate-layers-3.6

generate-glue-eggs:
	python3.6 setup.py bdist_egg
	python3.7 setup.py bdist_egg

generate-layers-3.7:
	mkdir -p dist
	rm -rf python
	docker run -v $(PWD):/var/task -it lambci/lambda:build-python3.7 /bin/bash -c "pip install . -t ./python"
	rm -f awswrangler_layer_3.7.zip
	zip -r awswrangler_layer_3.7.zip ./python
	mv awswrangler_layer_3.7.zip dist/
	rm -rf python

generate-layers-3.6:
	mkdir -p dist
	rm -rf python
	docker run -v $(PWD):/var/task -it lambci/lambda:build-python3.6 /bin/bash -c "pip install . -t ./python"
	rm -f awswrangler_layer_3.6.zip
	zip -r awswrangler_layer_3.6.zip ./python
	mv awswrangler_layer_3.6.zip dist/
	rm -rf python

build: format lint test doc
	rm -fr build dist .egg requests.egg-info
	python setup.py sdist bdist_wheel

publish:
	twine upload dist/*
	rm -fr build dist .egg requests.egg-info

doc:
	sphinx-apidoc -f -H "API Reference" -o docs/source/api awswrangler/
	make -C docs/ html