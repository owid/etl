"""Script to create a snapshot of dataset 'FluID, World Health Organization'."""

import time
from datetime import date, datetime, timezone
from pathlib import Path

import pandas as pd
import structlog
from requests.exceptions import RequestException

from etl.download_helpers import DownloadCorrupted, download
from etl.snapshot import Snapshot

log = structlog.get_logger()

# Version for current snapshot dataset.
SNAPSHOT_VERSION = Path(__file__).parent.name

# The WHO xmart API is served through Azure Front Door, which repeatedly hands this host a *cached*
# body assembled from more than one origin response, spliced together at 8 MiB boundaries. The body
# is exactly the length the Content-Length header promises, so the download itself looks healthy,
# but rows around each splice are fragmented, duplicated and lost — one observed body was missing
# 282 rows, carried 156 duplicates, and merged two half-rows into a 66-field line. AFD ignores
# `Cache-Control: no-cache`, and every plain request keeps hitting the same poisoned entry (five in
# a row returned an identical corrupt md5). A unique query parameter is a fresh cache key, so it
# always misses the cache and reaches the origin, which serves a correct body.
CACHE_BUST_PARAM = "owid_cache_bust"

# Number of download attempts. Each one uses a new cache-busting value, so this also covers the
# 504s the origin returns when it is slow to generate the export.
MAX_ATTEMPTS = 3

# Snapshot should have at least 50mb, otherwise something went wrong.
MIN_SIZE_BYTES = 50 * 2**20


def run(upload: bool = True) -> None:
    # Create a new snapshot.
    snap = Snapshot(f"who/{SNAPSHOT_VERSION}/fluid.csv")
    assert snap.metadata.origin, "origin is not set"
    assert snap.metadata.origin.url_download, "url_download is not set"

    snap.path.parent.mkdir(parents=True, exist_ok=True)

    for attempt in range(1, MAX_ATTEMPTS + 1):
        url = _cache_busting_url(snap.metadata.origin.url_download)
        try:
            download(url, str(snap.path))
            _assert_not_corrupted(snap.path)
            break
        except (DownloadCorrupted, RequestException) as e:
            log.warning("fluid.download_failed", attempt=attempt, max_attempts=MAX_ATTEMPTS, error=str(e))
            if attempt == MAX_ATTEMPTS:
                raise
            time.sleep(10 * attempt)

    # Bump date_accessed to today since this is a `latest/` snapshot that re-pulls fresh data. This
    # happens only once the download is known good, so a failed run leaves the .dvc file untouched.
    snap.metadata.origin.date_accessed = date.today().isoformat()
    snap.metadata.save()

    # Add file to DVC and upload to S3.
    snap.dvc_add(upload=upload)


def _cache_busting_url(url_download: str) -> str:
    """Add a value that has never been requested before, to force a cache miss."""
    nonce = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S%f")
    separator = "&" if "?" in url_download else "?"
    return f"{url_download}{separator}{CACHE_BUST_PARAM}={nonce}"


def _assert_not_corrupted(path: Path) -> None:
    """Reject a body that was spliced together from several origin responses.

    A splice shows up in one of two ways: it merges two half-rows into a line with the wrong number
    of fields, which pandas refuses to tokenize, or it lands on a row boundary and instead
    duplicates and drops whole rows, which parses cleanly and would otherwise be published as
    silently wrong data. A correct body has no duplicate rows at all.

    NOTE: this is a backstop, not the fix — the cache-busting URL above is what stops corrupted
    bodies being served in the first place, since every observed splice came from a cache hit. It
    cannot catch a hypothetical splice that only drops rows without duplicating any. Closing that
    gap needs a second full download to compare against (there is no ETag, no Content-MD5, and
    `$count` times out at the origin), which would double the load on an endpoint that already
    returns 504s — a worse trade than the residual risk. If the parse or duplicate check ever fires
    on a body that came back with a fresh cache key, that assumption needs revisiting.
    """
    size = path.stat().st_size
    if size < MIN_SIZE_BYTES:
        raise DownloadCorrupted(f"FluID snapshot is {size} bytes, expected at least {MIN_SIZE_BYTES}.")

    try:
        df = pd.read_csv(path, low_memory=False)
    except pd.errors.ParserError as e:
        raise DownloadCorrupted(f"FluID snapshot is not valid CSV: {e}") from e

    duplicates = int(df.duplicated().sum())
    if duplicates:
        raise DownloadCorrupted(f"FluID snapshot has {duplicates} duplicate rows, a correct body has none.")
