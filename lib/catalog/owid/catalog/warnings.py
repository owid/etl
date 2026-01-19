# Stub file for backwards compatibility - re-exports from core/warnings.py
# New code should import from owid.catalog.core.warnings or owid.catalog directly
from owid.catalog.core.warnings import (
    DifferentValuesWarning,
    DisplayNameWarning,
    NoOriginsWarning,
    catch_warnings,
    ignore_warnings,
    log,
    simplefilter,
    warn,
)

__all__ = [
    "DifferentValuesWarning",
    "DisplayNameWarning",
    "NoOriginsWarning",
    "ignore_warnings",
    "catch_warnings",
    "simplefilter",
    "warn",
    "log",
]
