#
#  Makefile
#

.PHONY: etl docs full lab test-default publish grapher dot watch clean clobber deploy api activate vscode-exclude-archived

include default.mk

SRC = etl snapshots apps api tests docs
PYTHON_PLATFORM = $(shell python -c "import sys; print(sys.platform)")
LIBS = lib/*

help:
	@echo 'Available commands:'
	@echo
	@echo '  make clean     	Delete all non-reference data in the data/ folder'
	@echo '  make clobber   	Delete non-reference data and .venv'
	@echo '  make deploy    	Re-run the full ETL on production'
	@echo '  make docs      	Serve documentation locally'
	@echo '  make dot       	Build a visual graph of the dependencies'
	@echo '  make etl       	Fetch data and run all transformations for garden'
	@echo '  make format    	Format code'
	@echo '  make format-all 	Format code (including modules in lib/)'
	@echo '  make full      	Fetch all data and run full transformations'
	@echo '  make grapher   	Publish supported datasets to Grapher'
	@echo '  make sync.catalog  Sync catalog from R2 into local data/ folder'
	@echo '  make lab       	Start a Jupyter Lab server'
	@echo '  make publish   	Publish the generated catalog to S3'
	@echo '  make api   		Start the ETL API on port 8081'
	@echo '  make fasttrack 	Start Fast-track on port 8082'
	@echo '  make chart-sync 	Start Chart-sync on port 8083'
	@echo '  make test      	Run all linting and unit tests'
	@echo '  make test-all  	Run all linting and unit tests (including for modules in lib/)'
	@echo '  make vscode-exclude-archived  Exclude archived steps from VSCode user settings'
	@echo '  make watch     	Run all tests, watching for changes'
	@echo '  make watch-all 	Run all tests, watching for changes (including for modules in lib/)'
	@echo

docs: .venv
	.venv/bin/mkdocs serve

watch-all:
	.venv/bin/watchmedo shell-command -c 'clear; make unittest; for lib in $(LIBS); do (cd $$lib && make unittest); done' --recursive --drop .

test-all:
	@echo '================ etl ================='
	@make test
	@for lib in $(LIBS); do \
		echo "================ $$lib ================="; \
		(cd $$lib && make test); \
	done

format-all:
	@echo '================ etl ================='
	@make test
	@for lib in $(LIBS); do \
		echo "================ $$lib ================="; \
		(cd $$lib && make format); \
	done

watch: .venv
	.venv/bin/watchmedo shell-command -c 'clear; make check-formatting lint check-typing coverage' --recursive --drop .

unittest: .venv
	@echo '==> Running unit tests'
	.venv/bin/pytest -m "not integration" tests

test: check-formatting check-linting check-typing unittest version-tracker

test-integration: .venv
	@echo '==> Running integration tests'
	.venv/bin/pytest -m integration tests

coverage: .venv
	@echo '==> Unit testing with coverage'
	.venv/bin/pytest --cov=etl --cov-report=term-missing tests

etl: .venv
	@echo '==> Running etl on garden'
	.venv/bin/etl run garden

full: .venv
	@echo '==> Running full etl'
	.venv/bin/etl run

clean:
	@echo '==> Cleaning data/ folder'
	rm -rf data && git checkout data

clobber: clean
	find . -name .venv | xargs rm -rf
	find . -name .pytest_cache | xargs rm -rf
	find . -name .cachedir | xargs rm -rf

lab: .venv
	@echo '==> Starting Jupyter server'
	.venv/bin/jupyter lab

publish: etl reindex
	@echo '==> Publishing the catalog'
	.venv/bin/etl d publish --private

reindex: .venv
	@echo '==> Creating a catalog index'
	.venv/bin/etl d reindex

prune: .venv
	@echo '==> Prune datasets with no recipe from catalog'
	.venv/bin/etl d prune

# Syncing catalog is useful if you want to avoid rebuilding it locally from scratch
# which could take a few hours. This will download ~10gb from the main channels
# (meadow, garden, open_numbers) and is especially useful when we increase ETL_EPOCH
# or update regions.
sync.catalog: .venv
	@echo '==> Sync catalog from R2 into local data/ folder (~10gb)'
	rclone copy owid-r2:owid-catalog/ data/ --verbose --fast-list --transfers=64 --checkers=64 --include "/meadow/**" --include "/garden/**" --include "/open_numbers/**"

grapher: .venv
	@echo '==> Running full etl with grapher upsert'
	.venv/bin/etl run --grapher

dot: dependencies.pdf

dependencies.pdf: .venv dag/main.yml etl/to_graphviz.py
	.venv/bin/etl graphviz dependencies.dot
	dot -Tpdf dependencies.dot >$@.tmp
	mv -f $@.tmp $@

deploy:
	@echo '==> Rebuilding the production ETL from origin/master'
	ssh -t owid@analytics.owid.io /home/owid/analytics/ops/scripts/etl-prod.sh

version-tracker: .venv
	@echo '==> Check that no archive dataset is used by an active dataset, and that all active datasets are used'
	.venv/bin/etl d version-tracker

api: .venv
	@echo '==> Starting ETL API on http://localhost:8081/api/v1/indicators'
	.venv/bin/uvicorn api.main:app --reload --port 8081 --host 0.0.0.0

fasttrack: .venv
	@echo '==> Starting Fast-track on http://localhost:8082/'
	.venv/bin/fasttrack --skip-auto-open --port 8082

wizard: .venv
	@echo '==> Starting Wizard on http://localhost:8053/'
	.venv/bin/etlwiz

# If VSCode exists, install a list of published extensions (defined in EXTENSIONS) and a list of custom extensions (defined in CUSTOM_EXTENSIONS).
# Custom extensions are expected to be in the vscode_extensions folder, with a subfolder for each extension containing a folder install/ with a VSIX file.
# The latest VSIX file in each install/ folder will be installed.
install-vscode-extensions:
	@echo '==> Checking and installing required VS Code extensions'
	@if command -v code > /dev/null; then \
		EXTENSIONS="ms-toolsai.jupyter"; \
		CUSTOM_EXTENSIONS="run-until-cursor find-latest-etl-step"; \
		EXTENSIONS_PATH="vscode_extensions"; \
		for EXT in $$EXTENSIONS; do \
			if ! code --list-extensions | grep -q "$$EXT"; then \
				code --install-extension $$EXT; \
			fi; \
		done; \
		for EXT in $$CUSTOM_EXTENSIONS; do \
			if ! code --list-extensions | grep -q "owid.$$EXT"; then \
				VSIX_FILE=$$(ls -v $$EXTENSIONS_PATH/$$EXT/install/$$EXT-*.vsix 2>/dev/null | tail -n 1); \
				if [ -n "$$VSIX_FILE" ]; then \
					echo "Installing owid.$$EXT from $$VSIX_FILE"; \
					code --install-extension "$$VSIX_FILE"; \
				else \
					echo "⚠️ No VSIX file found for owid.$$EXT. Skipping."; \
				fi; \
			fi; \
		done; \
	else \
		echo "⚠️ VS Code CLI (code) is not installed. Skipping extension installation."; \
	fi

vscode-exclude-archived: .venv
	@echo '==> Excluding archived steps from VSCode user settings'
	.venv/bin/python scripts/exclude_archived_steps.py --settings-scope user
