#
#  Makefile
#

default:
	@echo 'Available commands:'
	@echo
	@echo '  make audit     Audit the schema of all available files'
	@echo '  make fetch     Fetch all data files into the data/ folder'
	@echo '  make test      Run all linting and unit tests'
	@echo '  make watch     Run all tests, watching for changes'
	@echo '  make clean     Delete any fetched data files'
	@echo

.venv: pyproject.toml poetry.toml poetry.lock
	poetry install || poetry install
	touch $@

audit: .venv
	@echo '==> Auditing JSON records'
	poetry run python owid/walden/audit.py

fetch: .venv
	@echo '==> Fetching the full dataset'
	@poetry run python owid/walden/fetch.py

clean:
	@echo '==> Deleting all downloaded data'
	rm -rf ~/.owid/walden

test: check-formatting lint check-typing unittest audit

lint: .venv
	@echo '==> Linting'
	@poetry run flake8 owid

check-formatting: .venv
	@echo '==> Checking formatting'
	@poetry run black --check owid/walden
	@poetry run black --check ingests/
	@poetry run python -m owid.walden.format_json --check
	@echo '==> Checking imports sorting'
	@poetry run isort --check-only owid/walden/
	@poetry run isort --check-only ingests/

check-typing: .venv
	@echo '==> Checking types'
	@poetry run pyright owid/walden

unittest: .venv
	@echo '==> Running unit tests'
	@PYTHONPATH=. poetry run pytest

format: .venv
	@echo '==> Reformatting files'
	@poetry run black -q owid/walden/
	@poetry run black -q ingests/
	@poetry run python owid/walden/format_json.py
	@echo '==> Sorting imports'
	@poetry run isort -q owid/walden/
	@poetry run isort -q ingests/

watch: .venv
	poetry run watchmedo shell-command -c 'clear; make test' --recursive --drop .
