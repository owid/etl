# Stub file for backwards compatibility - re-exports from core/warnings.py
# New code should import from owid.catalog.core.warnings or owid.catalog directly
from owid.catalog.core.warnings import (
    DifferentValuesWarning,
    DisplayNameWarning,
    GroupingByCategoricalWarning,
    MetadataWarning,
    NoOriginsWarning,
    StepWarning,
    catch_warnings,
    ignore_warnings,
    log,
    simplefilter,
    warn,
    warn_with_structlog,
)

__all__ = [
    "MetadataWarning",
    "StepWarning",
    "DifferentValuesWarning",
    "DisplayNameWarning",
    "NoOriginsWarning",
    "GroupingByCategoricalWarning",
    "ignore_warnings",
    "catch_warnings",
    "simplefilter",
    "warn",
    "warn_with_structlog",
    "log",
]
