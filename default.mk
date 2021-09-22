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

# check formatting before lint, since an autoformat might fix linting issues
test-default: check-formatting lint check-typing unittest

lint-default:
	@echo '==> Linting'
	@poetry run flake8 $(SRC)

check-formatting-default:
	@echo '==> Checking formatting'
	@poetry run black --check $(SRC)

check-typing-default:
	@echo '==> Checking types'
	poetry run mypy $(SRC)

unittest-default:
	@echo '==> Running unit tests'
	poetry run pytest $(SRC)

format-default:
	@echo '==> Reformatting files'
	@poetry run black $(SRC)

watch-default:
	poetry run watchmedo shell-command -c 'clear; make test' --recursive --drop .

# allow you to override a command, e.g. "watch", but if you do not, then use
# the default
%: %-default
	@true
