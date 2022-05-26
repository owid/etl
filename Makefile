#
#  Makefile
#

.PHONY: etl

include default.mk

SRC = etl backport tests

help:
	@echo 'Available commands:'
	@echo
	@echo '  make etl       Fetch data and run all transformations for garden'
	@echo '  make full      Fetch all data and run full transformations'
	@echo '  make lab       Start a Jupyter Lab server'
	@echo '  make test      Run all linting and unit tests'
	@echo '  make publish   Publish the generated catalog to S3'
	@echo '  make grapher   Publish supported datasets to Grapher'
	@echo '  make dot       Build a visual graph of the dependencies'
	@echo '  make watch     Run all tests, watching for changes'
	@echo '  make clean     Delete all non-reference data in the data/ folder'
	@echo '  make clobber   Delete non-reference data and .venv'
	@echo


watch-all:
	.venv/bin/watchmedo shell-command -c 'clear; make unittest; (cd vendor/owid-catalog-py && make unittest); (cd vendor/walden && make unittest)' --recursive --drop .

test-all: test
	cd vendor/owid-catalog-py && make test
	cd vendor/walden && make test

watch: .venv
	.venv/bin/watchmedo shell-command -c 'clear; make check-formatting lint check-typing coverage' --recursive --drop .

.submodule-init:
	@echo '==> Initialising submodules'
	git submodule update --init
	touch $@

.venv: pyproject.toml poetry.toml poetry.lock .submodule-init
	@echo '==> Installing packages'
	poetry install
	touch $@

check-typing: .venv
	@echo '==> Checking types'
	.venv/bin/mypy --exclude=etl/steps $(SRC)
	@./scripts/typecheck_steps.sh

coverage: .venv
	@echo '==> Unit testing with coverage'
	.venv/bin/pytest --cov=etl --cov-report=term-missing tests

etl: .venv
	@echo '==> Running etl on garden'
	.venv/bin/etl garden

full: .venv
	@echo '==> Running full etl'
	.venv/bin/etl

clean:
	@echo '==> Cleaning data/ folder'
	rm -rf $$(ls -d data/* | grep -v reference)

clobber: clean
	find . -name .venv | xargs rm -rf
	find . -name .mypy_cache | xargs rm -rf

lab: .venv
	@echo '==> Starting Jupyter server'
	poetry run jupyter lab

publish: etl reindex
	@echo '==> Publishing the catalog'
	.venv/bin/publish --private

reindex: .venv
	@echo '==> Creating a catalog index'
	.venv/bin/reindex

grapher: .venv
	@echo '==> Running full etl with grapher upsert'
	.venv/bin/etl --grapher

dot: dependencies.pdf

dependencies.pdf: .venv dag.yml etl/to_graphviz.py
	.venv/bin/python etl/to_graphviz.py dependencies.dot
	dot -Tpdf dependencies.dot >$@.tmp
	mv -f $@.tmp $@
