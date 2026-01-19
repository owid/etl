# Stub file for backwards compatibility - re-exports from core/jinja.py
# New code should import from owid.catalog.core.jinja
from owid.catalog.core.jinja import (
    _cached_jinja_template,
    _expand_jinja,
    _expand_jinja_text,
    _uses_jinja,
    jinja_env,
    log,
    raise_helper,
)

__all__ = [
    "jinja_env",
    "raise_helper",
    "_uses_jinja",
    "_cached_jinja_template",
    "_expand_jinja_text",
    "_expand_jinja",
    "log",
]
