"""Snapshot for FAOSTAT FBS metadata (items + itemgroup).

We snapshot only the `item` and `itemgroup` categories of the FBS domain,
rather than depending on the broader bundled `faostat_metadata.json` snapshot,
so the food-trade pipeline stays independent of the rest of the FAOSTAT
machinery and pins the FBS rollup definitions to the same version as the
trade-matrix snapshot.

Provenance: FAOSTAT's public definitions API now requires an Authorization
header (returns 401 "Missing Authorization Header" on otherwise-anonymous
GETs to https://faostatservices.fao.org/api/v1/en/definitions/domain/fbs/...
as of 2026-05). No FAO API credentials are wired into our pipeline yet, so
this script slices the FBS subtree out of the existing bundled
`snapshot://faostat/2026-02-25/faostat_metadata.json` (taken when the API
was still open) rather than refetching directly. Once FAO publishes a way
to authenticate (or reopens the endpoint), swap the slice-from-bundled
section for a direct `requests.get` on the API URLs commented below.
"""

import json
import tempfile
from pathlib import Path

import click

from etl.snapshot import Snapshot

SNAPSHOT_VERSION = Path(__file__).parent.name

# When the FAO definitions API is reachable, point this at:
#   https://faostatservices.fao.org/api/v1/en/definitions/domain/fbs/item
#   https://faostatservices.fao.org/api/v1/en/definitions/domain/fbs/itemgroup
# and replace the bundled-snapshot slice below with two requests.get calls.
SOURCE_BUNDLED_SNAPSHOT = "faostat/2026-02-25/faostat_metadata.json"
CATEGORIES = ("item", "itemgroup")


@click.command()
@click.option("--upload/--skip-upload", default=True, type=bool, help="Upload dataset to Snapshot")
def main(upload: bool) -> None:
    snap = Snapshot(f"faostat/{SNAPSHOT_VERSION}/faostat_fbs_metadata.json")

    source = Snapshot(SOURCE_BUNDLED_SNAPSHOT)
    with open(source.path) as f:
        bundled = json.load(f)

    payload = {category: bundled["fbs"][category] for category in CATEGORIES}

    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
        json.dump(payload, f, indent=2, sort_keys=True)
        local = f.name

    snap.create_snapshot(filename=local, upload=upload)
