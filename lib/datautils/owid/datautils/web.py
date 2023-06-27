"""Web utils."""

import re
import warnings
from urllib.parse import urlparse

import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.ssl_ import create_urllib3_context  # type: ignore


def get_base_url(url: str, include_scheme: bool = True) -> str:
    """Get base URL from an arbitrary URL path (e.g. "https://example.com/some/path" -> "https://example.com").

    If the given URL does not start with "http(s)://"

    Parameters
    ----------
    url : str
        Input URL.
    include_scheme : bool, optional
        True to include "http(s)://" at the beginning of the returned base URL.
        False to hide the "http(s)://" (so that "https://example.com/some/path" -> "example.com").

    Returns
    -------
    base_url : str
        Base URL.

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
    """Download file from a URL into a local file.

    Parameters
    ----------
    url : str
        URL of the file to be downloaded.
    local_path : str
        Path to local file that will be created.
    chunk_size : float, optional
        Maximum size of the chunks of the response of a request.
    timeout : float, optional
        Timeout for standard GET requests.
    verify : bool, optional
        Verify SSL certificate for HTTPS requests.
    ciphers_low : bool, optional
        Use less restrictive encryption for request (required by certain old web sites).

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
