"""Bespoke viz step publishing the JSON feed for the demography visualization.

Unlike the other bespoke feeds, this one is not built from a garden table: it unpacks the snapshot
that captured the feed as it was hand-published under `s3://owid-public/bespoke/`, so that the same
bytes are served from the same place as every other feed. What comes out is
`demography.metadata.json`, which lists every entity and the file name carrying it, plus one
`demography.<slug>.data.json` per entity.

The files go to the step's output folder; the framework syncs that folder to the R2 path of the
environment being built, so the feed is served at
`<root>/v1/bespoke/un_wpp/latest/demography/demography.metadata.json` and
`.../demography.<slug>.data.json` -- `api.ourworldindata.org` on production, and
`api-staging.owid.io/<env>` on a staging server or a laptop.

There is no `metadata.json` (the feed provenance `etl.viz.bespoke.build_feed_metadata` derives)
because there are no garden columns to derive it from. Building this feed from the `un_wpp` garden
dataset -- which holds every series in it -- is the follow-up this step exists to make possible: the
feed's URL stops moving now, and the step's innards can change later without the visualization or
the registry noticing.

Run without --grapher to skip the upload and only write the local files:
  .venv/bin/etlr viz://bespoke/un_wpp/latest/demography
"""

import json

from structlog import get_logger

from etl.helpers import PathFinder
from etl.viz.bespoke_capture import unpack_feed_zip

log = get_logger()
paths = PathFinder(__file__)

# The file that indexes the rest of the feed.
INDEX_FILENAME = "demography.metadata.json"


def run() -> None:
    snap = paths.load_snapshot("demography")
    names = unpack_feed_zip(snap.path, paths.output_dir)

    # The visualization resolves every data file through the index, so a feed whose index names a
    # file the snapshot doesn't carry would fail only for whoever picked that entity.
    index = json.loads((paths.output_dir / INDEX_FILENAME).read_text())
    expected = {INDEX_FILENAME} | {f"demography.{slug}.data.json" for slug in index["slugs"].values()}
    assert set(names) == expected, f"The snapshot's files don't match its index: {sorted(set(names) ^ expected)}"

    log.info("demography.written", n_files=len(names), n_entities=len(index["slugs"]), dest=str(paths.output_dir))
