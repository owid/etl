"""Script to create a snapshot of the ISSA "Type of old-age pension(s)" page.

The page is protected by Cloudflare, which blocks plain HTTP requests (they get a 403 "Just a moment..." page). So,
without a file, this script opens the page in a visible Chrome window (Playwright, `channel="chrome"`), waits for the
challenge to clear, and saves the page's HTML. A headless browser doesn't get through the challenge.

Alternatively, open the page in your browser, save it as "Webpage, HTML only", and pass the file:
    .venv/bin/etls issa/2026-11-11/old_age_pension_types --path-to-file <path/to/file.html>

"""

import tempfile
import time
from pathlib import Path

from playwright.sync_api import sync_playwright

from etl.helpers import PathFinder

paths = PathFinder(__file__)

URL = "https://www.issa.int/databases/country-profiles/pensions"


def run(upload: bool = True, path_to_file: str | None = None) -> None:
    """Create a new snapshot.

    Args:
        upload: Whether to upload the snapshot to S3.
        path_to_file: Path to a local copy of the page. If not given, the page is downloaded with a browser.
    """
    # Init Snapshot object
    snap = paths.init_snapshot()

    # Download the page with a browser, unless a local copy is given.
    if path_to_file is None:
        path_to_file = str(Path(tempfile.mkdtemp()) / "old_age_pension_types.html")
        Path(path_to_file).write_text(download_page(URL))

    # Save snapshot from local file.
    snap.create_snapshot(filename=path_to_file, upload=upload)


def download_page(url: str) -> str:
    """Open a page in a visible Chrome window and return its HTML once the Cloudflare challenge clears."""
    with sync_playwright() as p:
        browser = p.chromium.launch(channel="chrome", headless=False)
        page = browser.new_page()
        page.goto(url, wait_until="domcontentloaded", timeout=90_000)
        # Wait for the challenge to clear and the table to load.
        page.wait_for_selector("td.td-country", timeout=120_000)
        time.sleep(2)
        html = page.content()
        browser.close()
    return html
