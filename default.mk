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
test-default: check-formatting check-linting check-typing unittest

.sanity-check:
	@echo '==> Checking your Python setup'

	@if python -c "import sys; exit(0 if sys.platform.startswith('win32') else 1)"; then \
		echo 'ERROR: you are using a non-WSL Python interpreter, please consult the'; \
		echo '       docs on how to swich to WSL Python on windows'; \
		echo '       https://github.com/owid/etl/'; \
		exit 1; \
	fi
	touch .sanity-check

install-uv-default:
	@if ! command -v uv >/dev/null 2>&1; then \
		echo '==> UV not found. Installing...'; \
		curl -LsSf https://astral.sh/uv/install.sh | sh && source $$HOME/.cargo/env; \
	fi

.venv-default: install-uv .sanity-check
	@echo '==> Installing packages'
	@if [ -n "$(PYTHON_VERSION)" ]; then \
		echo '==> Using Python version $(PYTHON_VERSION)'; \
		poetry env use python$(PYTHON_VERSION); \
		export UV_PYTHON=$(PYTHON_VERSION); \
	fi
	uv sync --all-extras

check-default:
	@echo '==> Lint & Format & Typecheck changed files'
	@git fetch -q origin master
	@RELATIVE_PATH=$$(pwd | sed "s|^$$(git rev-parse --show-toplevel)/||"); \
	CHANGED_PY_FILES=$$(git diff --name-only origin/master HEAD -- . && git diff --name-only && git ls-files --others --exclude-standard | grep '\.py'); \
	CHANGED_PY_FILES=$$(echo "$$CHANGED_PY_FILES" | sed "s|^$$RELATIVE_PATH/||" | grep '\.py' | xargs -I {} sh -c 'test -f {} && echo {}'); \
	if [ -n "$$CHANGED_PY_FILES" ]; then \
		echo "$$CHANGED_PY_FILES" | xargs ruff check --fix; \
		echo "$$CHANGED_PY_FILES" | xargs ruff format; \
		echo "$$CHANGED_PY_FILES" | xargs pyright; \
	fi

lint-default: .venv
	@echo '==> Linting & Sorting imports'
	@uv run ruff check --fix $(SRC)

check-linting-default: .venv
	@echo '==> Checking linting'
	@uv run ruff check $(SRC)

check-formatting-default: .venv
	@echo '==> Checking formatting'
	@uv run ruff format --check $(SRC)

check-typing-default: .venv
	@echo '==> Checking types'
	uv run pyright $(SRC)

unittest-default: .venv
	@echo '==> Running unit tests'
	uv run pytest $(SRC)

format-default: .venv
	@echo '==> Reformatting files'
	@uv run ruff format $(SRC)

coverage-default: .venv
	@echo '==> Unit testing with coverage'
	uv run pytest --cov=owid --cov-report=term-missing tests

watch-default: .venv
	@echo '==> Watching for changes and re-running checks'
	uv run watchmedo shell-command -c 'clear; make check' --recursive --drop .

bump-default: .venv
	@echo '==> Bumping version'
	uv run bump2version --no-tag  --no-commit $(filter-out $@, $(MAKECMDGOALS))


# allow you to override a command, e.g. "watch", but if you do not, then use
# the default
%: %-default
	@true
