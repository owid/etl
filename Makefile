#
#  Makefile
#

.PHONY: etl


include default.mk

SRC = etl tests

watch-all:
	poetry run watchmedo shell-command -c 'clear; make unittest; (cd vendor/owid-catalog-py && make unittest); (cd vendor/walden && make unittest)' --recursive --drop .

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
	poetry run python -m etl.command

clean:
	rm -rf data/*

clobber: clean
	rm -rf .venv

lab: .venv
	@echo '==> Starting Jupyter server'
	poetry run jupyter lab
