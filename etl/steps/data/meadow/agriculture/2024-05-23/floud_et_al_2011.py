"""Load a snapshot and create a meadow dataset."""

import owid.catalog.processing as pr

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshots.
    snap_europe = paths.load_snapshot("floud_et_al_2011_daily_calories_europe.csv")
    snap_us = paths.load_snapshot("floud_et_al_2011_daily_calories_us.csv")

    # Load data from snapshots.
    tb_europe = snap_europe.read()
    tb_us = snap_us.read()

    #
    # Process data.
    #
    # Transform Europe data to have a year column.
    tb_europe = tb_europe.melt(id_vars=["country"], var_name="year", value_name="daily_calories")

    # Prepare US data.
    tb_us = tb_us.rename(columns={"Year": "year", "Calories": "daily_calories"}, errors="raise").assign(
        **{"country": "United States"}
    )

    # Combine both tables.
    tb = pr.concat([tb_europe, tb_us], ignore_index=True)

    # Format table conveniently.
    tb = tb.format(["country", "year"], short_name=paths.short_name)

    #
    # Save outputs.
    #
    # Create a new meadow dataset.
    ds_meadow = paths.create_dataset(tables=[tb])
    ds_meadow.save()
