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
	uv run python owid/walden/audit.py

fetch: .venv
	@echo '==> Fetching the full dataset'
	@uv run python owid/walden/fetch.py

clean:
	@echo '==> Deleting all downloaded data'
	rm -rf ~/.owid/walden

test: check-formatting lint check-typing unittest audit

lint: .venv
	@echo '==> Linting'
	@uv run flake8 owid

check-formatting: .venv
	@echo '==> Checking formatting'
	@uv run black --check owid/walden
	@uv run black --check ingests/
	@uv run python -m owid.walden.format_json --check
	@echo '==> Checking imports sorting'
	@uv run isort --check-only owid/walden/
	@uv run isort --check-only ingests/

check-typing: .venv
	@echo '==> Checking types'
	@uv run pyright owid/walden

unittest: .venv
	@echo '==> Running unit tests'
	@PYTHONPATH=. uv run pytest

format: .venv
	@echo '==> Reformatting files'
	@uv run black -q owid/walden/
	@uv run black -q ingests/
	@uv run python owid/walden/format_json.py
	@echo '==> Sorting imports'
	@uv run isort -q owid/walden/
	@uv run isort -q ingests/

watch: .venv
	uv run watchmedo shell-command -c 'clear; make test' --recursive --drop .
