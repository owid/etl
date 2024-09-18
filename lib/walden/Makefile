#
#  Makefile
#

include ../../default.mk

SRC = owid tests

default:
	@echo 'Available commands:'
	@echo
	@echo '  make audit     Audit the schema of all available files'
	@echo '  make fetch     Fetch all data files into the data/ folder'
	@echo '  make test      Run all linting and unit tests'
	@echo '  make watch     Run all tests, watching for changes'
	@echo '  make clean     Delete any fetched data files'
	@echo

audit: .venv
	@echo '==> Auditing JSON records'
	.venv/bin/python owid/walden/audit.py

fetch: .venv
	@echo '==> Fetching the full dataset'
	@.venv/bin/python owid/walden/fetch.py

clean:
	@echo '==> Deleting all downloaded data'
	rm -rf ~/.owid/walden

test: check-formatting lint check-typing unittest audit

lint: .venv
	@echo '==> Linting'
	@.venv/bin/flake8 owid

check-formatting: .venv
	@echo '==> Checking formatting'
	@.venv/bin/black --check owid/walden
	@.venv/bin/black --check ingests/
	@.venv/bin/python -m owid.walden.format_json --check
	@echo '==> Checking imports sorting'
	@.venv/bin/isort --check-only owid/walden/
	@.venv/bin/isort --check-only ingests/

check-typing: .venv
	@echo '==> Checking types'
	. .venv/bin/activate && .venv/bin/pyright owid/walden

unittest: .venv
	@echo '==> Running unit tests'
	@PYTHONPATH=. .venv/bin/pytest

format: .venv
	@echo '==> Reformatting files'
	@.venv/bin/black -q owid/walden/
	@.venv/bin/black -q ingests/
	@.venv/bin/python owid/walden/format_json.py
	@echo '==> Sorting imports'
	@.venv/bin/isort -q owid/walden/
	@.venv/bin/isort -q ingests/
