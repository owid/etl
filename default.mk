#
#  default.mk
#

SRC = src test

default: help

help-default:
	@echo 'Available commands:'
	@echo
	@echo '  make test      Run all linting and unit tests'
	@echo '  make watch     Run all tests, watching for changes'
	@echo

# check formatting before lint, since an autoformat might fix linting issues
test-default: check-formatting lint check-typing unittest version-tracker

.venv-default:
	@echo '==> Installing packages'
	git submodule update --init
	poetry install
	touch $@

lint-default: .venv
	@echo '==> Linting'
	@poetry run flake8 $(SRC)

check-formatting-default: .venv
	@echo '==> Checking formatting'
	@poetry run black --check $(SRC)
	@echo '==> Checking import sorting'
	@poetry run isort --check-only $(SRC)

check-typing-default: .venv
	@echo '==> Checking types'
	poetry run pyright $(SRC)

unittest-default: .venv
	@echo '==> Running unit tests'
	poetry run pytest $(SRC)

format-default: .venv
	@echo '==> Reformatting files'
	@poetry run black $(SRC)
	@echo '==> Sorting imports'
	@poetry run isort $(SRC)

watch-default: .venv
	poetry run watchmedo shell-command -c 'clear; make test' --recursive --drop .

# allow you to override a command, e.g. "watch", but if you do not, then use
# the default
%: %-default
	@true
