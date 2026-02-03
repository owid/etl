# Stub - re-exports from core.meta for backwards compatibility
import warnings

warnings.warn(
    "Importing from 'owid.catalog.meta' is deprecated. Use 'owid.catalog.core.meta' instead.",
    DeprecationWarning,
    stacklevel=2,
)
from owid.catalog.core.meta import *  # noqa: E402,F401,F403
