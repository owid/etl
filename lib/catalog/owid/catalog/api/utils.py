#
#  owid.catalog.api.utils
#
#  Utility functions shared across API modules.
#
from __future__ import annotations

import platform
import sys
import threading
import time
from contextlib import contextmanager
from importlib.metadata import PackageNotFoundError, version
from typing import TYPE_CHECKING

import requests

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
# User-Agent for outbound HTTP requests
# =============================================================================
# Tagging traffic with a recognizable User-Agent lets us distinguish library
# usage from generic ``python-requests``/``pandas`` clients in CDN/access logs,
# and lets us track which library version is in the wild.

try:
    _LIB_VERSION = version("owid-catalog")
except PackageNotFoundError:
    _LIB_VERSION = "unknown"

USER_AGENT = f"owid-catalog/{_LIB_VERSION} (python {platform.python_version()})"


def _make_session() -> requests.Session:
    s = requests.Session()
    s.headers["User-Agent"] = USER_AGENT
    return s


#: Module-level :class:`requests.Session` with the catalog UA pre-set as a
#: default header. Use ``session.get`` / ``session.post`` in place of
#: ``requests.get`` / ``requests.post`` so every call is tagged automatically
#: and reuses pooled connections.
session = _make_session()

#: Pass as ``storage_options=`` to ``pd.read_csv``/``read_feather``/``read_parquet``
#: when reading from an HTTP(S) URL. pandas forwards these as headers (see
#: ``IOHandles`` / fsspec's HTTP backend). Used at the two HTTP-pandas-read
#: sites since pandas does not accept a ``requests.Session``.
STORAGE_OPTIONS = {"User-Agent": USER_AGENT}


def storage_options_for_http(path: object) -> dict[str, str]:
    """Return :data:`STORAGE_OPTIONS` if ``path`` is an HTTP(S) URL string, else ``{}``.

    Use to safely inject the catalog UA into ``pd.read_*`` calls that may
    receive either a local path/buffer or a remote URL — pandas raises
    ``ValueError`` if ``storage_options`` is passed alongside a non-fsspec
    local path.
    """
    if isinstance(path, str) and (path.startswith("http://") or path.startswith("https://")):
        return STORAGE_OPTIONS
    return {}


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
        get_ipython  # ty: ignore
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
