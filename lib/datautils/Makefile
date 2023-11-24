#
#  Makefile
#


include ../../default.mk

SRC = owid tests

# watch:
# 	poetry run watchmedo shell-command -c 'clear; make unittest' --recursive --drop .


report: report-coverage report-linting report-server-launch
	@poetry run python -m http.server .reports/

check-typing: .venv
	@echo '==> Checking types'
	poetry run pyright owid tests

report-coverage: .venv
	@echo '==> Unit testing with coverage'
	poetry run pytest --cov=owid --cov-report=term-missing --cov-report=html:.reports/coverage --cov-report=xml tests

report-linting: .venv
	@echo '==> Linting'
	@poetry run flake8 --format=html --htmldir=.reports/linting owid || true

report-server-launch: .venv
	@echo '==> Showing reports'
	@poetry run python -m http.server --directory .reports/

watch: .venv
	@echo '==> Watching for changes and re-running tests'
	poetry run watchmedo shell-command -c 'clear; make check-formatting lint check-typing coverage' --recursive --drop .

bump: .venv
	@echo '==> Bumping version'
	poetry run bump2version --no-tag  --no-commit $(filter-out $@, $(MAKECMDGOALS))
