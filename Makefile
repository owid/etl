#
#  Makefile
#

.PHONY: etl help


include default.mk

SRC = etl tests

help:
	@echo 'Available commands:'
	@echo
	@echo '  make etl       Fetch data and run all transformations'
	@echo '  make lab       Start a Jupyter Lab server'
	@echo '  make test      Run all linting and unit tests'
	@echo '  make publish   Publish the generated catalog to S3'
	@echo '  make watch     Run all tests, watching for changes'
	@echo '  make clean     Delete all non-reference data in the data/ folder'
	@echo '  make clobber   Delete non-reference data and .venv'
	@echo


watch-all:
	poetry run watchmedo shell-command -c 'clear; make unittest; (cd vendor/owid-catalog-py && make unittest); (cd vendor/walden && make unittest)' --recursive --drop .

test-all: test
	cd vendor/owid-catalog-py && make test
	cd vendor/walden && make test

watch: .venv
	poetry run watchmedo shell-command -c 'clear; make check-formatting lint check-typing coverage' --recursive --drop .

.submodule-init:
	@echo '==> Initialising submodules'
	git submodule update --init
	touch $@

.venv: pyproject.toml poetry.toml poetry.lock vendor/*/* .submodule-init
	@echo '==> Installing packages'
	poetry install
	touch $@

check-typing: .venv
	@echo '==> Checking types'
	poetry run mypy --strict $(SRC)

coverage: .venv
	@echo '==> Unit testing with coverage'
	poetry run pytest --cov=etl --cov-report=term-missing tests

etl: .venv
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
	.venv/bin/publish

reindex: .venv
	@echo '==> Creating a catalog index'
	.venv/bin/reindex
