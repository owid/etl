"""Capturing and republishing a bespoke feed that was published by hand.

`migrant-demographics` was built outside the ETL and uploaded to `s3://owid-public/bespoke/`, which
is why it is the last bespoke bundle whose data does not come out of the pipeline. Until it is built
from garden, its `viz://bespoke` step republishes a capture of that upload: a snapshot script fetches
the feed as it is served and writes it into one zip, and the step unpacks that zip into its output
folder for the framework to sync (`etl.viz.bespoke`).

The module is written for a feed of that shape -- an index file naming every entity and the file that
carries it, plus one file per entity -- so the fetching, the zip, and the unpacking live here, while
how the index names its data files stays the snapshot script's business. `demography` was captured
the same way until it was rewritten on top of the `un_wpp` garden dataset.

Everything in this module exists for that interim arrangement, and should be deleted with it once
the feed is built from garden.
"""

from __future__ import annotations

import json
import zipfile
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from structlog import get_logger
from tqdm.auto import tqdm

from etl.http import session

log = get_logger()

# Fixed timestamp for every zip entry, so a capture's bytes depend only on the feed's contents and
# re-running an unchanged capture leaves the snapshot alone. It is the earliest a zip can store.
ZIP_TIMESTAMP = (1980, 1, 1, 0, 0, 0)


def fetch_feed_file(base_url: str, filename: str) -> bytes:
    """Fetch one file of a published feed, and return its bytes unaltered.

    The bytes are kept rather than the parsed object: the point of the capture is that the feed can
    be republished from the ETL byte for byte, and a re-serialization would not be. The parse is
    only a check that the file arrived intact -- a truncated download is otherwise indistinguishable
    from a small country.
    """
    response = session.get(f"{base_url.rstrip('/')}/{filename}", timeout=60)
    response.raise_for_status()
    json.loads(response.content)
    return response.content


def fetch_feed_files(base_url: str, filenames: list[str], max_workers: int = 16) -> dict[str, bytes]:
    """Fetch every named file of a published feed, keyed by file name."""
    log.info("bespoke_capture.fetching", base_url=base_url, n_files=len(filenames))
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        contents = list(
            tqdm(
                executor.map(lambda filename: fetch_feed_file(base_url, filename), filenames),
                total=len(filenames),
                desc=base_url.rstrip("/").rsplit("/", 1)[-1],
            )
        )
    return dict(zip(filenames, contents))


def write_feed_zip(files: dict[str, bytes], path: Path) -> None:
    """Write a captured feed into a zip whose bytes depend only on the feed's contents."""
    with zipfile.ZipFile(path, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=9) as archive:
        for name in sorted(files):
            info = zipfile.ZipInfo(name, date_time=ZIP_TIMESTAMP)
            info.compress_type = zipfile.ZIP_DEFLATED
            # Fixed permissions; ZipInfo() would otherwise take them from the running process.
            info.external_attr = 0o644 << 16
            archive.writestr(info, files[name])


def unpack_feed_zip(zip_path: Path, dest_dir: Path) -> list[str]:
    """Unpack a captured feed into a step's output folder, and return the file names written."""
    dest_dir.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(zip_path) as archive:
        names = sorted(archive.namelist())
        for name in names:
            (dest_dir / name).write_bytes(archive.read(name))
    return names


__all__ = ["fetch_feed_file", "fetch_feed_files", "unpack_feed_zip", "write_feed_zip"]
