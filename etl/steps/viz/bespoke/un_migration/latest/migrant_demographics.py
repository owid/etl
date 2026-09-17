"""Bespoke viz step publishing the JSON feed for the migrant-demographics visualization.

Unlike the other bespoke feeds, this one is not built from a garden table: it unpacks the snapshot
that captured the feed as it was hand-published under `s3://owid-public/bespoke/`, so that the same
bytes are served from the same place as every other feed. What comes out is
`migrant-demographics.metadata.json`, which carries the age bands, the years, the source line the
visualization shows, and every entity with the code naming its file, plus one
`migrant-demographics.<code>.json` per entity.

The files go to the step's output folder; the framework syncs that folder to the R2 path of the
environment being built, so the feed is served at
`<root>/v1/bespoke/un_migration/latest/migrant_demographics/migrant-demographics.metadata.json` and
`.../migrant-demographics.<code>.json` -- `api.ourworldindata.org` on production, and
`api-staging.owid.io/<env>` on a staging server or a laptop.

There is no `metadata.json` (the feed provenance `etl.viz.bespoke.build_feed_metadata` derives)
because there are no garden columns to derive it from; the source line the visualization shows comes
from the feed's own index. Building this feed from the migrant-stock and population garden datasets
is the follow-up this step exists to make possible: the feed's URL stops moving now, and the step's
innards can change later without the visualization or the registry noticing.

Run without --grapher to skip the upload and only write the local files:
  .venv/bin/etlr viz://bespoke/un_migration/latest/migrant_demographics
"""

import json

from structlog import get_logger

from etl.helpers import PathFinder
from etl.viz.bespoke_capture import unpack_feed_zip

log = get_logger()
paths = PathFinder(__file__)

# The file that indexes the rest of the feed.
INDEX_FILENAME = "migrant-demographics.metadata.json"


def run() -> None:
    snap = paths.load_snapshot("migrant_demographics")
    names = unpack_feed_zip(snap.path, paths.output_dir)

    # The visualization resolves every data file through the index, so a feed whose index names a
    # file the snapshot doesn't carry would fail only for whoever picked that entity.
    index = json.loads((paths.output_dir / INDEX_FILENAME).read_text())
    expected = {INDEX_FILENAME} | {f"migrant-demographics.{entity['code']}.json" for entity in index["entities"]}
    assert set(names) == expected, f"The snapshot's files don't match its index: {sorted(set(names) ^ expected)}"

    log.info(
        "migrant_demographics.written",
        n_files=len(names),
        n_entities=len(index["entities"]),
        dest=str(paths.output_dir),
    )
