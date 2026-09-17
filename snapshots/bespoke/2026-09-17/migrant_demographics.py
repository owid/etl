"""Capture the migrant-demographics bespoke feed as it was hand-published, into a single zip.

The feed is a folder of JSON files: `migrant-demographics.metadata.json`, which carries the age
bands, the years, the source line the visualization shows, and every entity with the numeric code
that names its file, plus one `migrant-demographics.<code>.json` per entity. They were built from
International Migrant Stock 2020 and World Population Prospects outside the ETL and uploaded by hand
to `s3://owid-public/bespoke/migrant-demographics/`, which is where this script reads them from --
the producer publishes no equivalent of them.

The upload also still carries `migrant-demographics.json`, the single file the visualization fetched
before it was split into one file per entity (owid-grapher#7291). Nothing reads it any more, so it
is not captured.

    etls bespoke/2026-09-17/migrant_demographics

See `etl.viz.bespoke_capture` for why this exists and when it should go.
"""

import json
from pathlib import Path
from tempfile import TemporaryDirectory

from etl.helpers import PathFinder
from etl.viz.bespoke_capture import fetch_feed_file, fetch_feed_files, write_feed_zip

paths = PathFinder(__file__)

# Where the feed is served from today. It is an Our World in Data host, not the producer's.
BASE_URL = "https://owid-public.owid.io/bespoke/migrant-demographics"

# The file that indexes the rest of the feed.
INDEX_FILENAME = "migrant-demographics.metadata.json"


def sanity_check_index(index: dict) -> None:
    """Check the index carries what the visualization reads off it, and one distinct file per entity."""
    assert index.get("ageBands") and index.get("years"), f"{INDEX_FILENAME} is missing its age bands or years."
    assert index.get("meta", {}).get("source"), (
        f"{INDEX_FILENAME} is missing the source line shown under the visualization."
    )
    entities = index.get("entities") or []
    assert entities, f"{INDEX_FILENAME} names no entities."
    codes = [entity["code"] for entity in entities]
    assert len(set(codes)) == len(codes), "Two entities in the index share a file code."
    # The feed has covered ~200 countries since it was first published; a capture of a fraction of
    # it would mean the upload was in progress.
    assert len(entities) > 150, f"The index only lists {len(entities)} entities, which is too few to be the feed."


def sanity_check_files(files: dict[str, bytes], index: dict) -> None:
    """Check every entity in the index came back, with a record on every age band in every year."""
    expected = {INDEX_FILENAME} | {f"migrant-demographics.{entity['code']}.json" for entity in index["entities"]}
    assert set(files) == expected, f"Fetched files don't match the index: {sorted(set(files) ^ expected)}"

    # The visualization throws on an entity whose years or age bands don't line up, which would
    # surface only in the browser, and only for whoever picked that entity.
    n_bands, years = len(index["ageBands"]), [str(year) for year in index["years"]]
    malformed = sorted(
        name
        for name, content in files.items()
        if name != INDEX_FILENAME
        and any(
            len(json.loads(content).get(year, {}).get(key, [])) != n_bands
            for year in years
            for key in ("m", "f", "pm", "pf")
        )
    )
    assert not malformed, f"Entity files that don't line up with the feed's years and age bands: {malformed}"


def run(upload: bool = True) -> None:
    snap = paths.init_snapshot()

    index_content = fetch_feed_file(BASE_URL, INDEX_FILENAME)
    index = json.loads(index_content)
    sanity_check_index(index)

    filenames = [
        f"migrant-demographics.{entity['code']}.json" for entity in sorted(index["entities"], key=lambda e: e["code"])
    ]
    files = {INDEX_FILENAME: index_content, **fetch_feed_files(BASE_URL, filenames)}
    sanity_check_files(files, index)

    with TemporaryDirectory() as temp_dir:
        path = Path(temp_dir) / "migrant_demographics.zip"
        write_feed_zip(files, path)
        snap.create_snapshot(filename=path, upload=upload)
