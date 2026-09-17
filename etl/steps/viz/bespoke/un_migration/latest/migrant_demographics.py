"""Bespoke viz step publishing the JSON feed for the migrant-demographics visualization.

Unlike the other bespoke feeds, this one is not built from a garden table: it copies the snapshot
that captured the feed as it was hand-published at `owid-public.owid.io`, so that the same bytes are
served from the same place as every other feed. The feed is the single file
`migrant-demographics.json`, which the visualization uses as both its data and its metadata.

The file goes to the step's output folder; the framework syncs that folder to the R2 path of the
environment being built, so the feed is served at
`<root>/v1/bespoke/un_migration/latest/migrant_demographics/migrant-demographics.json` --
`api.ourworldindata.org` on production, and `api-staging.owid.io/<env>` on a staging server or a
laptop.

There is no `metadata.json` (the feed provenance `etl.viz.bespoke.build_feed_metadata` derives)
because there are no garden columns to derive it from; the source line the visualization shows comes
from the `meta.source` field inside the file itself. Building this feed from the migrant-stock and
population garden datasets is the follow-up this step exists to make possible: the feed's URL stops
moving now, and the step's innards can change later without the visualization or the registry
noticing.

Run without --grapher to skip the upload and only write the local file:
  .venv/bin/etlr viz://bespoke/un_migration/latest/migrant_demographics
"""

import json

from structlog import get_logger

from etl.helpers import PathFinder

log = get_logger()
paths = PathFinder(__file__)

# The name the visualization fetches, which is not the snapshot's own file name.
FEED_FILENAME = "migrant-demographics.json"


def run() -> None:
    snap = paths.load_snapshot("migrant_demographics")

    content = snap.path.read_bytes()
    data = json.loads(content)

    # The visualization throws on a file missing any of these, and drops any entity whose year
    # records don't line up with the age bands -- failures that would surface only in the browser.
    assert data.get("ageBands") and data.get("years"), "The feed is missing its age bands or years."
    assert data.get("meta", {}).get("source"), "The feed is missing the source line shown under the visualization."
    n_bands, years = len(data["ageBands"]), [str(year) for year in data["years"]]
    entities = [entity for entity in data["entities"] if not entity.get("isAggregate")]
    assert entities, "The feed carries no entities the visualization would show."
    malformed = sorted(
        entity["name"]
        for entity in entities
        if any(
            len(entity["data"].get(year, {}).get(key, [])) != n_bands
            for year in years
            for key in ("m", "f", "pm", "pf")
        )
    )
    assert not malformed, f"Entities whose data doesn't line up with the feed's age bands: {malformed}"

    paths.output_dir.mkdir(parents=True, exist_ok=True)
    (paths.output_dir / FEED_FILENAME).write_bytes(content)

    log.info("migrant_demographics.written", n_entities=len(entities), dest=str(paths.output_dir / FEED_FILENAME))
