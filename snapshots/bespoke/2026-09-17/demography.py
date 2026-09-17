"""Capture the demography bespoke feed as it was hand-published, into a single zip.

The feed is a folder of JSON files: `demography.metadata.json`, which lists every entity and the
file name (`slug`) that carries it, plus one `demography.<slug>.data.json` per entity. They were
built from World Population Prospects 2024 outside the ETL and uploaded by hand to
`s3://owid-public/bespoke/demography/`, which is where this script reads them from -- the producer
publishes no equivalent of them.

Only the entities the index names are captured. The upload also carries eight data files the index
does not list -- Vatican and seven UN development groupings -- which the visualization resolves
entity names through the index and so can never reach.

    etls bespoke/2026-09-17/demography

See `etl.viz.bespoke_capture` for why this exists and when it should go.
"""

import json
from pathlib import Path
from tempfile import TemporaryDirectory

from etl.helpers import PathFinder
from etl.viz.bespoke_capture import fetch_feed_file, fetch_feed_files, write_feed_zip

paths = PathFinder(__file__)

# Where the feed is served from today. It is an Our World in Data host, not the producer's.
BASE_URL = "https://owid-public.owid.io/bespoke/demography"

# The file that indexes the rest of the feed.
INDEX_FILENAME = "demography.metadata.json"


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


def run(upload: bool = True) -> None:
    snap = paths.init_snapshot()

    index_content = fetch_feed_file(BASE_URL, INDEX_FILENAME)
    index = json.loads(index_content)
    sanity_check_index(index)

    filenames = [f"demography.{slug}.data.json" for slug in sorted(index["slugs"].values())]
    files = {INDEX_FILENAME: index_content, **fetch_feed_files(BASE_URL, filenames)}
    sanity_check_files(files, index)

    with TemporaryDirectory() as temp_dir:
        path = Path(temp_dir) / "demography.zip"
        write_feed_zip(files, path)
        snap.create_snapshot(filename=path, upload=upload)
