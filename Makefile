.PHONY: test

init:
	pip install pipenv --upgrade
	pipenv install --dev

test:
	tox

coverage:
	coverage report

coverage-html:
	coverage html

format:
	black awswrangler tests benchmarks

lint:
	flake8 awswrangler tests benchmarks

doc:
	sphinx-apidoc -f -H "API Reference" -o docs/source/api awswrangler/
	make -C docs/ html

artifacts: format generate-glue-eggs generate-layers-3.7 generate-layers-3.6 generate-layers-2.7

generate-glue-eggs:
	python2.7 setup.py bdist_egg
	python3.6 setup.py bdist_egg
	python3.7 setup.py bdist_egg

generate-layers-3.7:
	mkdir -p dist
	docker run -v $(PWD):/var/task -it lambci/lambda:build-python3.7 /bin/bash -c "pip install . -t ./python"
	zip -r awswrangler_layer_3.7.zip ./python
	mv awswrangler_layer_3.7.zip dist/
	rm -rf python

generate-layers-3.6:
	mkdir -p dist
	docker run -v $(PWD):/var/task -it lambci/lambda:build-python3.6 /bin/bash -c "pip install . -t ./python"
	zip -r awswrangler_layer_3.6.zip ./python
	mv awswrangler_layer_3.6.zip dist/
	rm -rf python

generate-layers-2.7:
	mkdir -p dist
	docker run -v $(PWD):/var/task -it lambci/lambda:build-python2.7 /bin/bash -c "pip install . -t ./python"
	zip -r awswrangler_layer_2.7.zip ./python
	mv awswrangler_layer_2.7.zip dist/
	rm -rf python

build: format test doc
	rm -fr build dist .egg requests.egg-info
	python setup.py sdist bdist_wheel

publish: build
	twine upload dist/*
	rm -fr build dist .egg requests.egg-info