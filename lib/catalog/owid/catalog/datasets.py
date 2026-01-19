# Stub file for backwards compatibility - re-exports from core/datasets.py
# New code should import from owid.catalog.core.datasets
from owid.catalog.core.datasets import (
    # Constants
    CHANNEL,
    DEFAULT_FORMATS,
    NULLABLE_DTYPES,
    PREFERRED_FORMAT,
    SUPPORTED_FORMATS,
    # Main class
    Dataset,
    # Type alias
    FileFormat,
    # Exceptions
    NonUniqueIndex,
    PrimaryKeyMissing,
    # Functions
    checksum_file,
)

__all__ = [
    # Main class
    "Dataset",
    # Type alias
    "FileFormat",
    # Constants
    "CHANNEL",
    "DEFAULT_FORMATS",
    "NULLABLE_DTYPES",
    "PREFERRED_FORMAT",
    "SUPPORTED_FORMATS",
    # Functions
    "checksum_file",
    # Exceptions
    "NonUniqueIndex",
    "PrimaryKeyMissing",
]
