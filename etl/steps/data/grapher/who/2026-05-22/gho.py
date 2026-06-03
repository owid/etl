"""Load a garden dataset and create a grapher dataset."""

import structlog

from etl.helpers import PathFinder

log = structlog.get_logger()

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Load garden dataset.
    ds_garden = paths.load_dataset("gho")

    #
    # Process data.
    #
    # All data fixes (unit corrections, dropped dimensions, typo patches in
    # description_from_producer, etc.) live in the garden step. Here we only:
    # 1. skip indicator-tables that have zero data rows (WHO catalogues many
    #    indicators in `/api/Indicator` for which `/api/<IndicatorCode>` returns
    #    an empty response — placeholder shells the producer hasn't populated
    #    yet, or recently retired indicators kept in the catalog for one
    #    release). Uploading them would create variables with no data points;
    #    cleaner to drop them at the grapher boundary. ~50–70 tables fall into
    #    this bucket each release (climate-change-attributable burden,
    #    dementia care/services, lead-attributable burden, some policy
    #    surveys), and the set churns release-to-release.
    # 2. drop the `comments` ferry column — it carries the raw WHO metadata
    #    JSON from snapshot→meadow→garden so that downstream consumers like
    #    `garden/who/2023-06-01/cholera` can read it. We don't want it landing
    #    in Grapher as an indicator.
    tables = []
    for tb_name in ds_garden.table_names:
        tb = ds_garden[tb_name]

        if tb.empty:
            log.warning(f"Table '{tb_name}' is empty (no data published in this release). Skipping.")
            continue

        tb = tb.drop(columns=["comments"], errors="ignore")

        tables.append(tb)

    #
    # Save outputs.
    #
    # Create a new grapher dataset with the same metadata as the garden dataset.
    ds_grapher = paths.create_dataset(tables=tables, check_variables_metadata=True, default_metadata=ds_garden.metadata)

    # Save changes in the new grapher dataset.
    ds_grapher.save()
