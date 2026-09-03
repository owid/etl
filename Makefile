#
#  Makefile
#

.PHONY: etl docs full lab test-default publish grapher dot watch clean clobber deploy activate owid_mcp vsce-compile vsce-sync install-hooks setup.worktree

include default.mk

SRC = etl snapshots apps api_search tests docs owid_mcp owl_steps lib/owl/owl
PYTHON_PLATFORM = $(shell python -c "import sys; print(sys.platform)")
LIBS = lib/catalog lib/datautils lib/owl lib/repack

help:
	@echo 'Available commands:'
	@echo
	@echo '  make clean     	Delete all non-reference data in the data/ folder'
	@echo '  make clobber   	Delete non-reference data and .venv'
	@echo '  make deploy    	Re-run the full ETL on production'
	@echo '  make docs.build    Build documentation'
	@echo '  make docs.serve    Serve documentation locally'
	@echo '  make docs.post     Transform non-md files in docs/ after build (e.g. notebooks)'
	@echo '  make dot       	Build a visual graph of the dependencies'
	@echo '  make etl       	Fetch data and run all transformations for garden'
	@echo '  make format    	Format code'
	@echo '  make format-all 	Format code (including modules in lib/)'
	@echo '  make full      	Fetch all data and run full transformations'
	@echo '  make grapher   	Publish supported datasets to Grapher'
	@echo '  make sync.catalog  Sync catalog from R2 into local data/ folder'
	@echo '  make lab       	Start a Jupyter Lab server'
	@echo '  make publish   	Publish the generated catalog to S3'
	@echo '  make api-search   	Start the Search API on port 8084'
	@echo '  make fasttrack 	Start Fast-track on port 8082'
	@echo '  make chart-sync 	Start Chart-sync on port 8083'
	@echo '  make query SQL="..." Run SQL query on staging MySQL for current branch'
	@echo '  make install-hooks	Activate pre-commit hook (auto-runs with make .venv)'
	@echo '  make setup.worktree	Copy .env from the main checkout into this worktree (auto-runs with make .venv)'
	@echo '  make test      	Run all linting and unit tests'
	@echo '  make test-all  	Run all linting and unit tests (including for modules in lib/)'
	@echo '  make check-all 	Format, lint, and typecheck (including for modules in lib/)'
	@echo '  make vsce-sync 	Sync VS Code extensions (install missing, upgrade outdated)'
# 	@echo '  make vsce-compile EXT=name [BUMP=patch|minor|major] [INSTALL=1]  Compile and package VS Code extension'
	@echo '  make watch     	Run all tests, watching for changes'
	@echo '  make watch-all 	Run all tests, watching for changes (including for modules in lib/)'
	@echo

docs.pre: .venv
	@echo '==> Fetching external documentation files'
	@.venv/bin/python docs/ignore/pre-build/bake_catalog_api.py
	@echo '==> Generating dynamic documentation files'
	@.venv/bin/python docs/ignore/pre-build/bake_metadata_reference.py
	@.venv/bin/python -m docs.ignore.pre-build.bake_search_api
	@.venv/bin/python -m docs.ignore.pre-build.bake_chart_api
	@.venv/bin/python -m docs.ignore.pre-build.bake_semantic_search_api
	@.venv/bin/python docs/ignore/pre-build/generate_analytics_docs.py

docs.llms: .venv
	@echo '==> Generating llms.txt'
	@.venv/bin/python docs/ignore/others/bake_llms_txt.py

docs.post: .venv
	@echo '==> Converting Jupyter Notebooks to HTML'
	.venv/bin/python docs/ignore/post-build/convert_notebooks.py

docs.build: .venv
	@echo '==> Cleaning previous build'
	@rm -rf site/ .cache/
	@mkdir -p .cache
	@echo '==> Pre-processing documentation files'
	@$(MAKE) --no-print-directory docs.pre
	@echo '==> Building documentation with Zensical'
	@DOCS_BUILD=1 .venv/bin/python -c "import zensical.config as c; o=c._list_sources; c._list_sources=lambda cfg,p:type(r:=o(cfg,p))(x for x in r if '/.venv' not in x[0]); __import__('zensical').build(__import__('os').path.abspath('zensical.toml'),{'clean':True,'strict':False})"
	@echo '==> Post-processing documentation files'
	@$(MAKE) --no-print-directory docs.post

docs.serve: .venv
	DOCS_BUILD=1 .venv/bin/python -c "import zensical.config as c; o=c._list_sources; c._list_sources=lambda cfg,p:type(r:=o(cfg,p))(x for x in r if '/.venv' not in x[0]); __import__('zensical').serve(__import__('os').path.abspath('zensical.toml'),{'dev_addr':'localhost:9010','open':False,'strict':False})"

watch-all:
	.venv/bin/watchmedo shell-command -c 'clear; make unittest; for lib in $(LIBS); do (cd $$lib && make unittest); done' --recursive --drop .

test-all:
	@echo '================ etl ================='
	@make test
	@for lib in $(LIBS); do \
		echo "================ $$lib ================="; \
		(cd $$lib && make test); \
	done

check-all:
	@echo '================ etl ================='
	@make check
	@for lib in $(LIBS); do \
		echo "================ $$lib ================="; \
		(cd $$lib && make check); \
	done

format-all:
	@echo '================ etl ================='
	@make format
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

check-typing: .venv
	@echo '==> Checking types'
	.venv/bin/ty check $(SRC) --exclude "etl/steps/**" --exclude "snapshots/**" --python .venv

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
	rm -rf data && mkdir data

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

query:
	@if [ -z "$(SQL)" ]; then \
		echo "Usage: make query SQL=\"SELECT ...\""; \
		exit 1; \
	fi
	@BRANCH=$$(git rev-parse --abbrev-ref HEAD); \
	NORMALIZED=$$(echo "$$BRANCH" | sed 's/[\/\._]/-/g' | sed 's/^staging-site-//' | cut -c1-28 | sed 's/-*$$//'); \
	HOST="staging-site-$$NORMALIZED"; \
	mysql -h "$$HOST" -u owid --port 3306 -D owid -e "$(SQL)"

api-search: .venv
	@echo '==> Starting Search API on http://localhost:8084/indicators'
	.venv/bin/uvicorn api_search.main:app --reload --port 8084 --host 0.0.0.0 --reload-exclude '.cache/*'

fasttrack: .venv
	@echo '==> Starting Fast-track on http://localhost:8082/'
	.venv/bin/fasttrack --skip-auto-open --port 8082

wizard: .venv
	@echo '==> Starting Wizard on http://localhost:8053/'
	.venv/bin/etlwiz

# Sync VS Code extensions: installs missing ones and upgrades outdated ones (version-aware)
vsce-sync:
	@unset NODE_OPTIONS; \
	if command -v code > /dev/null; then \
		INSTALLED=$$(code --list-extensions --show-versions); \
		EXTENSIONS="ms-toolsai.jupyter"; \
		for EXT in $$EXTENSIONS; do \
			if ! echo "$$INSTALLED" | grep -q "$$EXT"; then \
				echo "Installing $$EXT"; \
				code --install-extension $$EXT; \
			fi; \
		done; \
		EXTENSIONS_PATH="vscode_extensions"; \
		for EXT_DIR in $$EXTENSIONS_PATH/*/; do \
			EXT=$$(basename $$EXT_DIR); \
			VSIX_FILE=$$(ls -v $$EXT_DIR/install/$$EXT-*.vsix 2>/dev/null | tail -n 1); \
			if [ -z "$$VSIX_FILE" ]; then continue; fi; \
			REPO_VER=$$(echo "$$VSIX_FILE" | sed 's/.*-\([0-9].*\)\.vsix/\1/'); \
			INSTALLED_VER=$$(echo "$$INSTALLED" | grep "owid\.$$EXT@" | sed 's/.*@//'); \
			if [ "$$INSTALLED_VER" != "$$REPO_VER" ]; then \
				echo "Installing owid.$$EXT $$REPO_VER (was: $${INSTALLED_VER:-not installed})"; \
				code --install-extension "$$VSIX_FILE" --force; \
			fi; \
		done; \
	else \
		echo "⚠️ VS Code CLI (code) is not installed. Skipping extension sync."; \
	fi

# Activate the tracked pre-commit hook by symlinking it into the repo's hooks
# directory. Idempotent; runs as a dependency of .venv so a fresh clone gets the
# hook the first time anyone sets up the environment.
#
# Deliberately NOT `git config core.hooksPath scripts/hooks`, which is what this
# target used to do. `core.hooksPath` *replaces* the hooks directory rather than
# adding to it, and a repo-local setting beats a global one — so on a machine that
# points `core.hooksPath` at a directory of its own hooks, this repo silently
# disabled all of them and kept only ours. That is easy to miss, because a hook
# that never runs looks exactly like a hook that passed; the one that bit was a
# `post-checkout` giving each new worktree its gitignored `.env`, whose absence
# surfaces much later as a confusing connection error.
#
# `$GIT_DIR/hooks` is git's own default, so the symlink needs no config at all, and
# a machine-wide hooks directory that forwards to `$GIT_DIR/hooks` still finds it.
# `--git-common-dir` resolves to the primary `.git` from a linked worktree, so one
# install covers every worktree, and the link points at the primary checkout's copy
# of the script so it survives that worktree being removed.
install-hooks:
	@if git rev-parse --is-inside-work-tree >/dev/null 2>&1; then \
		COMMON="$$(cd "$$(git rev-parse --git-common-dir)" && pwd)"; \
		HOOK="$$COMMON/hooks/pre-commit"; \
		TARGET="$$(dirname "$$COMMON")/scripts/hooks/pre-commit"; \
		chmod +x scripts/hooks/pre-commit; \
		mkdir -p "$$COMMON/hooks"; \
		if [ -e "$$HOOK" ] && [ ! -L "$$HOOK" ]; then \
			echo "⚠️ $$HOOK already exists and is not a symlink — leaving it alone."; \
		elif [ "$$(readlink "$$HOOK" 2>/dev/null)" != "$$TARGET" ]; then \
			ln -sfn "$$TARGET" "$$HOOK"; \
			echo "==> pre-commit hook active ($$HOOK)"; \
		fi; \
		if [ "$$(git config --local --get core.hooksPath 2>/dev/null)" = "scripts/hooks" ]; then \
			git config --local --unset core.hooksPath; \
			echo "==> removed the core.hooksPath override earlier versions of this target set"; \
		fi; \
	fi

# Give this checkout the gitignored config a worktree can't inherit from git.
# Idempotent — an existing file is never touched, so it is safe to re-run and safe
# to adjust a worktree's own `.env` afterwards.
#
# `setup.worktree` is the agreed name for "make this checkout usable", so anything
# that creates a worktree can provision it without knowing what this repo needs: a
# worktree manager's setup script, a machine-wide `post-checkout` hook, or a human.
# It is also a prerequisite of `.venv` below, so a worktree nobody provisioned
# fixes itself on the first `make` — nothing here depends on the hook having fired.
# Keep it cheap and offline for that reason: no installs, no network.
#
# Copies rather than symlinks, so a worktree can diverge from the main checkout
# (a different `STAGING`, a scratch credential) without editing everyone's config.
# The cost is that a rotated secret doesn't reach worktrees that already exist —
# delete the file and re-run to pick it up.
setup.worktree:
	@PRIMARY="$$(dirname "$$(cd "$$(git rev-parse --git-common-dir)" && pwd)")"; \
	if [ "$$PRIMARY" = "$$PWD" ]; then \
		echo '==> This is the main checkout, nothing to provision'; \
	else \
		for SRC in "$$PRIMARY"/.env "$$PRIMARY"/.env.prod*; do \
			[ -e "$$SRC" ] || continue; \
			NAME="$$(basename "$$SRC")"; \
			[ -e "$$NAME" ] && continue; \
			cp -p "$$SRC" "$$NAME" && echo "==> copied $$NAME from the main checkout"; \
		done; \
	fi

.venv: .venv-default install-hooks setup.worktree
	@true

# Backward-compatible alias
install-vscode-extensions: vsce-sync

# Compile and package a VS Code extension
# Usage: make vsce-compile EXT=detect-outdated-practices [BUMP=patch|minor|major] [INSTALL=1]
vsce-compile:
	@if [ -z "$(EXT)" ]; then \
		echo "❌ Error: EXT parameter is required."; \
		echo "Usage: make vsce-compile EXT=extension-name [BUMP=patch|minor|major] [INSTALL=1]"; \
		echo ""; \
		echo "Available extensions:"; \
		find vscode_extensions -maxdepth 1 -type d ! -name vscode_extensions ! -name '.*' -exec basename {} \; | sed 's/^/  - /'; \
		exit 1; \
	fi; \
	EXT_PATH="vscode_extensions/$(EXT)"; \
	if [ ! -d "$$EXT_PATH" ]; then \
		echo "❌ Error: Extension '$(EXT)' not found at $$EXT_PATH"; \
		exit 1; \
	fi; \
	echo "🔧 Building extension: $(EXT)"; \
	cd "$$EXT_PATH" && \
	if [ -n "$(BUMP)" ]; then \
		echo "📦 Bumping version ($(BUMP))..."; \
		npm version $(BUMP) --no-git-tag-version; \
	else \
		CURRENT_VERSION=$$(node -p "require('./package.json').version"); \
		echo "📦 Using current version ($$CURRENT_VERSION)..."; \
	fi; \
	echo "⚙️  Compiling TypeScript..."; \
	npm run compile && \
	echo "📦 Packaging VSIX..."; \
	npx @vscode/vsce package && \
	mkdir -p install install/archived && \
	if [ -n "$(BUMP)" ]; then \
		OLD_VSIX_COUNT=$$(ls -1 install/*.vsix 2>/dev/null | wc -l); \
		if [ "$$OLD_VSIX_COUNT" -gt 0 ]; then \
			echo "📦 Archiving $$OLD_VSIX_COUNT old version(s)..."; \
			mv install/*.vsix install/archived/ 2>/dev/null || true; \
		fi; \
	fi; \
	mv *.vsix install/ && \
	VSIX_FILE=$$(ls -t install/*.vsix | head -n 1); \
	VSIX_PATH=$$(pwd)/$$VSIX_FILE; \
	echo "✅ Extension packaged: $$VSIX_FILE"; \
	if [ "$(INSTALL)" = "1" ]; then \
		echo ""; \
		echo "🔄 Installing extension..."; \
		cd - > /dev/null && code --install-extension "$$VSIX_PATH" --force; \
		echo "✅ Extension installed!"; \
	else \
		echo ""; \
		echo "To install, run: make vsce-compile EXT=$(EXT) INSTALL=1"; \
		echo "Or install directly: code --install-extension $$VSIX_PATH --force"; \
	fi
