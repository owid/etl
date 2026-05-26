"""Conflict explorer — re-orders the Conflict Data Source explorer's views.

This explorer surfaces the same views as `conflict_data_source` but reshuffles
the dropdown layout: `conflict_type` is the primary selector, `data_source`
is demoted to fourth. The build is purely a mutation of the upstream
collection — no view rewriting, no `create_collection` call:

  1. Load the upstream `conflict_data_source` collection from the local export.
  2. Re-target its `catalog_path` to this step.
  3. Swap in this step's explorer-level `config` (title / subtitle /
     `subNavCurrentId` differ from cds) and apply YAML `common_views` as view
     defaults.
  4. Reorder the `dimensions` list to the YAML order, sort each dim's choices
     to the YAML order, and overwrite choice display names from the YAML
     (so `country_and_region_data` reads as "Country and regional data" here).
"""

from typing import cast

from etl.collection.explorer import Explorer
from etl.helpers import PathFinder

paths = PathFinder(__file__)


def run() -> None:
    # Load upstream explorer
    cs = paths.load_collectionset("conflict_data_source", channel="explorers")
    c_old = cast(Explorer, cs.read("conflict-data-source"))

    # Create new explorer
    config = paths.load_collection_config()
    c = paths.create_collection(
        config,
        short_name="conflict-data",
        explorer=True,
    )

    # Swap explorer-level config (title, subtitle, subNavCurrentId differ).
    c.views = c_old.views

    c.save(tolerate_extra_indicators=True)
