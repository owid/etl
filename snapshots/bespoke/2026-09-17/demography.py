"""Capture the demography bespoke feed as it was hand-published, into a single zip.

The feed is a folder of JSON files: `demography.metadata.json`, which lists every entity and the
file name (`slug`) that carries it, plus one `demography.<slug>.data.json` per entity. They were
built from World Population Prospects 2024 outside the ETL and uploaded by hand to
`owid-public.owid.io`, which is where this script reads them from -- the producer publishes no
equivalent of them.

The zip is written deterministically (entries sorted, timestamps fixed), so re-running this against
an unchanged feed produces the same file and leaves the snapshot alone.

    etls bespoke/2026-09-17/demography
"""

import json
import zipfile
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from tempfile import TemporaryDirectory

from structlog import get_logger
from tqdm.auto import tqdm

from etl.helpers import PathFinder
from etl.http import session

log = get_logger()
paths = PathFinder(__file__)

# Where the feed is served from today. It is an Our World in Data host, not the producer's.
BASE_URL = "https://owid-public.owid.io/bespoke/demography"

# The file that indexes the rest of the feed.
INDEX_FILENAME = "demography.metadata.json"

# Fixed timestamp for every zip entry, so the archive's bytes depend only on the feed's contents.
# The value is the earliest one the zip format can store.
ZIP_TIMESTAMP = (1980, 1, 1, 0, 0, 0)


def fetch_json(filename: str) -> bytes:
    """Fetch one file of the feed, and return its bytes unaltered.

    The bytes are kept rather than the parsed object: the point of this snapshot is that the feed
    can be republished from the ETL byte-for-byte, and a re-serialization would not be.
    """
    response = session.get(f"{BASE_URL}/{filename}", timeout=60)
    response.raise_for_status()
    # Parse only to check the file is intact -- a truncated download is otherwise indistinguishable
    # from a small country.
    json.loads(response.content)
    return response.content


def sanity_check_index(index: dict) -> None:
    """Check the index names the entities it is supposed to, and maps each to a distinct file."""
    countries, slugs = index.get("countries"), index.get("slugs")
    assert countries and slugs, f"{INDEX_FILENAME} is missing its `countries` or `slugs` list."
    assert set(countries) == set(slugs), "The index's `countries` and the keys of its `slugs` disagree."
    assert len(set(slugs.values())) == len(slugs), "Two entities in the index share a file slug."
    # The feed has covered every country plus the UN regions and income groups since it was first
    # published; a capture of a third of it would mean the upload was in progress.
    assert len(countries) > 200, f"The index only lists {len(countries)} entities, which is too few to be the feed."


def sanity_check_files(files: dict[str, bytes], index: dict) -> None:
    """Check every entity in the index came back, and that nothing came back empty."""
    expected = {INDEX_FILENAME} | {f"demography.{slug}.data.json" for slug in index["slugs"].values()}
    assert set(files) == expected, f"Fetched files don't match the index: {sorted(set(files) ^ expected)}"
    # Every entity carries ~70 years of population by age and sex plus the projections, so a file
    # this small is a stub or an error page that happened to parse.
    tiny = sorted(name for name, content in files.items() if len(content) < 10_000 and name != INDEX_FILENAME)
    assert not tiny, f"Suspiciously small entity files: {tiny}"


def write_zip(files: dict[str, bytes], path: Path) -> None:
    """Write the feed into a zip whose bytes depend only on the feed's contents."""
    with zipfile.ZipFile(path, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=9) as archive:
        for name in sorted(files):
            info = zipfile.ZipInfo(name, date_time=ZIP_TIMESTAMP)
            info.compress_type = zipfile.ZIP_DEFLATED
            # Default permissions; ZipInfo() would otherwise take them from the running process.
            info.external_attr = 0o644 << 16
            archive.writestr(info, files[name])


def run(upload: bool = True) -> None:
    snap = paths.init_snapshot()

    index_content = fetch_json(INDEX_FILENAME)
    index = json.loads(index_content)
    sanity_check_index(index)

    filenames = [f"demography.{slug}.data.json" for slug in sorted(index["slugs"].values())]
    log.info("demography.fetching_feed", n_files=len(filenames) + 1, base_url=BASE_URL)
    with ThreadPoolExecutor(max_workers=16) as executor:
        contents = list(tqdm(executor.map(fetch_json, filenames), total=len(filenames), desc="demography feed"))

    files = {INDEX_FILENAME: index_content, **dict(zip(filenames, contents))}
    sanity_check_files(files, index)

    with TemporaryDirectory() as temp_dir:
        path = Path(temp_dir) / "demography.zip"
        write_zip(files, path)
        snap.create_snapshot(filename=path, upload=upload)
