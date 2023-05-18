#
#  Makefile
#

.PHONY: etl docs full lab test-default publish grapher dot watch clean clobber deploy

include default.mk

SRC = etl snapshots backport walkthrough fasttrack tests
PYTHON_PLATFORM = $(shell python -c "import sys; print(sys.platform)")

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
	@echo '  make deploy    Re-run the full ETL on production'
	@echo '  make docs      Serve documentation locally'
	@echo

docs: .venv
	poetry run mkdocs serve

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

.sanity-check:
	@echo '==> Checking your Python setup'

	@if python -c "import sys; exit(0 if sys.platform.startswith('win32') else 1)"; then \
		echo 'ERROR: you are using a non-WSL Python interpreter, please consult the'; \
		echo '       docs on how to swich to WSL Python on windows'; \
		echo '       https://github.com/owid/etl/'; \
		exit 1; \
	fi
	touch .sanity-check

.venv: .sanity-check pyproject.toml poetry.toml poetry.lock .submodule-init
	@echo '==> Installing packages'
	poetry install || poetry install
	touch $@

check-typing: .venv
	@echo '==> Checking types'
	poetry run pyright $(SRC)

coverage: .venv
	@echo '==> Unit testing with coverage'
	poetry run pytest --cov=etl --cov-report=term-missing tests

etl: .venv
	@echo '==> Running etl on garden'
	poetry run etl garden

full: .venv
	@echo '==> Running full etl'
	poetry run etl

clean:
	@echo '==> Cleaning data/ folder'
	rm -rf data && git checkout data

clobber: clean
	find . -name .venv | xargs rm -rf
	find . -name .pytest_cache | xargs rm -rf
	find . -name .cachedir | xargs rm -rf

lab: .venv
	@echo '==> Starting Jupyter server'
	poetry run jupyter lab

publish: etl reindex
	@echo '==> Publishing the catalog'
	poetry run publish --private

reindex: .venv
	@echo '==> Creating a catalog index'
	poetry run reindex

prune: .venv
	@echo '==> Prune datasets with no recipe from catalog'
	poetry run prune

grapher: .venv
	@echo '==> Running full etl with grapher upsert'
	poetry run etl --grapher

dot: dependencies.pdf

dependencies.pdf: .venv dag/main.yml etl/to_graphviz.py
	poetry run generate_graph dependencies.dot
	dot -Tpdf dependencies.dot >$@.tmp
	mv -f $@.tmp $@

deploy:
	@echo '==> Rebuilding the production ETL from origin/master'
	ssh -t owid@analytics.owid.io /home/owid/analytics/ops/scripts/etl-prod.sh

version-tracker: .venv
	@echo '==> Check that no archive dataset is used by an active dataset, and that all active datasets are used'
	poetry run version_tracker
