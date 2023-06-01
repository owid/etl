#
#  default.mk
#

SRC = src test

default:
	@echo 'Available commands:'
	@echo
	@echo '  make test      Run all linting and unit tests'
	@echo '  make watch     Run all tests, watching for changes'
	@echo

.venv: pyproject.toml poetry.toml poetry.lock
	poetry install || poetry install
	touch $@

# check formatting before lint, since an autoformat might fix linting issues
test-default: check-formatting lint check-typing unittest

lint-default: .venv
	@echo '==> Linting'
	@poetry run flake8 $(SRC)

check-formatting-default: .venv
	@echo '==> Checking formatting'
	@poetry run black --check $(SRC)
	@echo '==> Checking imports sorting'
	@poetry run isort --check-only $(SRC)

check-typing-default: .venv
	@echo '==> Checking types'
	PYTHONPATH=. poetry run mypy .

unittest-default: .venv
	@echo '==> Running unit tests'
	@PYTHONPATH=. poetry run pytest

format-default: .venv
	@echo '==> Reformatting files'
	@poetry run black $(SRC)
	@echo '==> Sorting imports'
	@poetry run isort $(SRC)

watch-default: .venv
	@echo '==> Watching for changes and re-running tests'
	poetry run watchmedo shell-command -c 'clear; make test' --recursive --drop .

# allow you to override a command, e.g. "watch", but if you do not, then use
# the default
%: %-default
	@true
