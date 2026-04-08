"""Web utils."""

import re
import warnings
from urllib.parse import urlparse

import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.ssl_ import create_urllib3_context  # type: ignore


def get_base_url(url: str, include_scheme: bool = True) -> str:
    """Extract base URL from a full URL path.

    Parses a URL and returns just the base domain with optional scheme.
    Automatically adds `http://` scheme if not present in input.

    Args:
        url: Input URL to parse (e.g., "https://example.com/some/path").
        include_scheme: If True, include "http(s)://" in result.
            If False, return domain only.

    Returns:
        Base URL extracted from input.

    Example:
        ```python
        from owid.datautils.web import get_base_url

        # With scheme
        get_base_url("https://example.com/some/path")
        # Returns: "https://example.com"

        # Without scheme
        get_base_url("https://example.com/some/path", include_scheme=False)
        # Returns: "example.com"

        # URL without scheme (assumes http://)
        get_base_url("example.com/path")
        # Returns: "http://example.com"
        ```
    """
    # Function urlparse cannot parse a url if it does not start with http(s)://.
    # If such a url is passed, assume "http://".
    if not re.match(r"https?://", url):
        warnings.warn(f"Schema not defined for url {url}; assuming http.")
        url = f"http://{url}"

    # Parse url, and return the base url either starting with "http(s)://" (if include_scheme is True) or without it.
    parsed_url = urlparse(url)
    if include_scheme:
        base_url = f"{parsed_url.scheme}://{parsed_url.netloc}"
    else:
        base_url = parsed_url.netloc

    return base_url


class _DESAdapter(HTTPAdapter):  # type: ignore
    """A TransportAdapter that re-enables Triple DES support in requests.

    From: https://stackoverflow.com/a/46186957/5056599

    """

    ciphers = "HIGH:!DH:!aNULL:DEFAULT@SECLEVEL=1"

    def init_poolmanager(self, *args, **kwargs):  # type: ignore
        context = create_urllib3_context(ciphers=self.ciphers)
        kwargs["ssl_context"] = context
        return super(_DESAdapter, self).init_poolmanager(*args, **kwargs)

    def proxy_manager_for(self, *args, **kwargs):  # type: ignore
        context = create_urllib3_context(ciphers=self.ciphers)
        kwargs["ssl_context"] = context
        return super(_DESAdapter, self).proxy_manager_for(*args, **kwargs)


def download_file_from_url(
    url: str,
    local_path: str,
    chunk_size: float = 1024 * 1024,
    timeout: float = 30,
    verify: bool = True,
    ciphers_low: bool = False,
) -> None:
    """Download a file from URL to local filesystem.

    Downloads files in chunks to handle large files efficiently. Supports
    legacy websites requiring lower encryption standards.

    Args:
        url: URL of the file to download.
        local_path: Destination path for the downloaded file.
        chunk_size: Size of download chunks in bytes. Default is 1MB.
        timeout: Request timeout in seconds. Default is 30 seconds.
        verify: If True, verify SSL certificates for HTTPS requests.
            Set to False for self-signed certificates.
        ciphers_low: If True, use less restrictive encryption (Triple DES).
            Required for some legacy websites with outdated SSL configurations.

    Example:
        Basic download
        ```python
        from owid.datautils.web import download_file_from_url

        download_file_from_url(
            "https://example.com/data.csv",
            "local_data.csv"
        )
        ```

        Download from legacy website
        ```python
        download_file_from_url(
            "https://old-site.com/file.zip",
            "local_file.zip",
            ciphers_low=True  # Enable Triple DES for old SSL
        )
        ```

        Large file with custom chunk size
        ```python
        download_file_from_url(
            "https://example.com/bigfile.bin",
            "bigfile.bin",
            chunk_size=10 * 1024 * 1024  # 10MB chunks
        )
        ```
    """
    # Create a persistent request session.
    with requests.Session() as session:
        if ciphers_low:
            # Special type of request.
            session.mount(get_base_url(url), _DESAdapter())
            response = session.get(url, timeout=timeout)
        else:
            # Send a standard GET request.
            response = session.get(url, stream=True, timeout=timeout, verify=verify)

    # Save the requested data into a local file.
    with open(local_path, "wb") as output_file:
        for chunk in response.iter_content(chunk_size=chunk_size):  # type: ignore
            output_file.write(chunk)
