#
#  Makefile
#


include ../../default.mk

SRC = owid tests

# watch:
# 	poetry run watchmedo shell-command -c 'clear; make unittest' --recursive --drop .

check-typing: .venv
	@echo '==> Checking types'
	poetry run pyright $(SRC)

coverage: .venv
	@echo '==> Unit testing with coverage'
	poetry run pytest --cov=owid --cov-report=term-missing tests

watch: .venv
	@echo '==> Watching for changes and re-running tests'
	poetry run watchmedo shell-command -c 'clear; make check-formatting lint check-typing coverage' --recursive --drop .
