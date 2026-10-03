# Stub - re-exports from core.tables for backwards compatibility
import warnings

warnings.warn(
    "Importing from 'owid.catalog.tables' is deprecated. Use 'owid.catalog.core.tables' instead.",
    DeprecationWarning,
    stacklevel=2,
)
from owid.catalog.core.tables import *  # noqa: E402,F401,F403
