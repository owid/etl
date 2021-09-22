#
#  Makefile
#

.PHONY: etl


include default.mk

SRC = etl tests

# watch:
# 	poetry run watchmedo shell-command -c 'clear; make unittest' --recursive --drop .

check-typing:
	@echo '==> Checking types'
	poetry run mypy --strict $(SRC)

coverage:
	@echo '==> Unit testing with coverage'
	poetry run pytest --cov=etl --cov-report=term-missing tests

etl:
	@echo '==> Running full etl'
	poetry run python -m etl.command

clean:
	rm -rf data/*

clobber: clean
	rm -rf .venv
