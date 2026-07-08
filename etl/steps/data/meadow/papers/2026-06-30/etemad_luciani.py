"""Meadow step for Etemad & Luciani's historical energy production data.

The Shift Data Portal file we snapshot contains rows from several sources (labeled in a "source"
column). Here we keep only the Etemad & Luciani rows (1900-1979) and pivot the energy families into
columns. The EIA rows (1980+) in the same file are discarded.
"""

import owid.catalog.processing as pr

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    snap = paths.load_snapshot("etemad_luciani.csv")
    tb = snap.read_csv()

    #
    # Process data.
    #
    # Keep only Etemad & Luciani data (the file also contains EIA data for 1980 onwards).
    tb = tb[tb["source"] == "Etemad & Luciani"].reset_index(drop=True)

    # Keep relevant columns, and rename the entity column to "country".
    tb = tb[["group_name", "energy_family", "year", "energy"]].rename(columns={"group_name": "country"})
    tb["energy"] = pr.to_numeric(tb["energy"], errors="coerce")

    # The raw file splits some fuels into unlabeled sub-components (e.g. coal types) across several rows.
    # Sum them per country, year and energy family. This is verified to be correct: the summed countries
    # exactly reproduce the file's own World totals.
    origins = tb["energy"].metadata.origins
    tb = tb.groupby(["country", "year", "energy_family"], as_index=False, observed=True)["energy"].sum(min_count=1)
    tb["energy"].metadata.origins = origins

    # Pivot the energy families into columns (values are in million tonnes of oil equivalent).
    tb = tb.pivot(index=["country", "year"], columns="energy_family", values="energy", join_column_levels_with="_")

    # Underscore column names and set an appropriate index.
    tb = tb.underscore().format(["country", "year"], short_name=paths.short_name)

    #
    # Save outputs.
    #
    ds_meadow = paths.create_dataset(tables=[tb], default_metadata=snap.metadata)
    ds_meadow.save()
