# Stub - re-exports from core.utils for backwards compatibility
import warnings

warnings.warn(
    "Importing from 'owid.catalog.utils' is deprecated. Use 'owid.catalog.core.utils' instead.",
    DeprecationWarning,
    stacklevel=2,
)
from owid.catalog.core.utils import *  # noqa: E402,F401,F403
