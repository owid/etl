#
#  owid.catalog.api.utils
#
#  Utility functions shared across API modules.
#
from __future__ import annotations

import sys
import threading
import time
from contextlib import contextmanager
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from owid.catalog.core.datasets import FileFormat

# Constants
OWID_CATALOG_VERSION = 3
S3_OWID_URI = "s3://owid-catalog"
S3_OWID_URI_PRIVATE = "s3://owid-catalog-private"
PREFERRED_FORMAT: FileFormat = "feather"
SUPPORTED_FORMATS: list[FileFormat] = ["feather", "parquet", "csv"]
INDEX_FORMATS: list[FileFormat] = ["feather"]

# =============================================================================
# Default API URLs
# =============================================================================
# These are used only by Client.__init__() as defaults. All other functions
# and classes require URLs to be passed explicitly.

DEFAULT_CATALOG_URL = "https://catalog.ourworldindata.org/"
DEFAULT_SITE_URL = "https://ourworldindata.org"
DEFAULT_INDICATORS_SEARCH_URL = "https://search.owid.io/indicators"
DEFAULT_SITE_SEARCH_URL = "https://ourworldindata.org/api/search"


@contextmanager
def _loading_data_from_api(message: str = "Loading data"):
    """Context manager that shows a loading indicator while data is being fetched.

    Displays animated dots in terminal or Jupyter notebook to indicate progress.

    Args:
        message: Message to display (default: "Loading data")

    Example:
        ```python
        with _loading_data_from_api("Fetching chart"):
            data = expensive_operation()
        ```
    """
    # Check if we're in a Jupyter notebook
    try:
        get_ipython  # type: ignore
        in_notebook = True
    except NameError:
        in_notebook = False

    # Check if output is to a terminal (not redirected)
    is_tty = sys.stdout.isatty()

    # Only show indicator in interactive environments
    if not (in_notebook or is_tty):
        yield
        return

    # Animation state
    stop_event = threading.Event()
    animation_chars = ["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"]

    def animate():
        """Animate the loading indicator."""
        idx = 0
        while not stop_event.is_set():
            char = animation_chars[idx % len(animation_chars)]
            # Use carriage return to overwrite the line
            print(f"\r{char} {message}...", end="", flush=True)
            idx += 1
            time.sleep(0.1)

    # Start animation thread
    animation_thread = threading.Thread(target=animate, daemon=True)
    animation_thread.start()

    try:
        yield
    finally:
        # Stop animation
        stop_event.set()
        animation_thread.join(timeout=0.5)
        # Clear the line
        print("\r" + " " * (len(message) + 10) + "\r", end="", flush=True)
