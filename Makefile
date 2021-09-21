#
#  Makefile
#


include default.mk

SRC = etl tests

# watch:
# 	poetry run watchmedo shell-command -c 'clear; make unittest' --recursive --drop .

check-typing:
	@echo '==> Checking types'
	PYTHONPATH=. poetry run mypy --strict .

coverage:
	@echo '==> Unit testing with coverage'
	poetry run pytest --cov=etl --cov-report=term-missing tests
