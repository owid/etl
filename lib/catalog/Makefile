#
#  Makefile
#


include ../../default.mk

SRC = owid tests

# Tests marked `integration` call live OWID services, so a blip there would fail CI; run them with `make test-integration`.
unittest: .venv
	@echo '==> Running unit tests'
	.venv/bin/pytest -m "not integration" $(SRC)

test-integration: .venv
	@echo '==> Running integration tests'
	.venv/bin/pytest -m integration tests
