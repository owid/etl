#
#  Makefile
#


include ../../default.mk

SRC = owid tests

.venv: install-uv .sanity-check
	@echo '==> Installing packages (with local overrides)'
	@if [ -n "$(PYTHON_VERSION)" ]; then \
		echo '==> Using Python version $(PYTHON_VERSION)'; \
		[ -f $$HOME/.cargo/env ] && . $$HOME/.cargo/env || true && UV_PYTHON=$(PYTHON_VERSION) uv sync --all-extras; \
	else \
		[ -f $$HOME/.cargo/env ] && . $$HOME/.cargo/env || true && uv sync --all-extras; \
	fi

	@if [ -d "../../../owid-grapher-py" ]; then \
		echo '==> Linking local owid-grapher-py'; \
		uv pip install -e ../../../owid-grapher-py; \
	fi
