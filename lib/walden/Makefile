#
#  Makefile
#

include ../../default.mk

SRC = owid tests

default:
	@echo 'Available commands:'
	@echo
	@echo '  make audit     Audit the schema of all available files'
	@echo '  make fetch     Fetch all data files into the data/ folder'
	@echo '  make test      Run all linting and unit tests'
	@echo '  make clean     Delete any fetched data files'
	@echo

audit: .venv
	@echo '==> Auditing JSON records'
	.venv/bin/python owid/walden/audit.py

fetch: .venv
	@echo '==> Fetching the full dataset'
	@.venv/bin/python owid/walden/fetch.py

clean:
	@echo '==> Deleting all downloaded data'
	rm -rf ~/.owid/walden

test: check-formatting check-linting check-typing unittest audit
