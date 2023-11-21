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
	@echo '  make check     Format & Lint & Typecheck changed files from master'
	@echo

# check formatting before lint, since an autoformat might fix linting issues
test-default: check-formatting lint check-typing unittest

.venv-default:
	@echo '==> Installing packages'
	poetry install
	touch .venv

check-default:
	@echo '==> Format & Lint & Typecheck changed files'
	@git fetch -q origin master
	@RELATIVE_PATH=$$(pwd | sed "s|^$$(git rev-parse --show-toplevel)/||"); \
	CHANGED_PY_FILES=$$(git diff --name-only origin/master -- . | sed "s|^$$RELATIVE_PATH/||" | grep '\.py'); \
	if [ -n "$$CHANGED_PY_FILES" ]; then \
		echo "$$CHANGED_PY_FILES" | xargs ruff format; \
		echo "$$CHANGED_PY_FILES" | xargs ruff check --fix; \
		echo "$$CHANGED_PY_FILES" | xargs pyright; \
	fi

lint-default: .venv
	@echo '==> Linting & Sorting imports'
	@poetry run ruff check --fix $(SRC)

check-linting-default: .venv
	@echo '==> Checking linting'
	@poetry run ruff check $(SRC)

check-formatting-default: .venv
	@echo '==> Checking formatting'
	@poetry run ruff format --check $(SRC)

check-typing-default: .venv
	@echo '==> Checking types'
	poetry run pyright $(SRC)

unittest-default: .venv
	@echo '==> Running unit tests'
	poetry run pytest $(SRC)

format-default: .venv
	@echo '==> Reformatting files'
	@poetry run ruff format $(SRC)

watch-default: .venv
	poetry run watchmedo shell-command -c 'clear; make test' --recursive --drop .

# allow you to override a command, e.g. "watch", but if you do not, then use
# the default
%: %-default
	@true
