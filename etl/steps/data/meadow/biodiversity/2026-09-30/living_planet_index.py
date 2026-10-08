"""Load a snapshot and create a meadow dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Map the producer's column names to the short names used since the 2024 release.
COLUMNS = {
    "Entity": "country",
    "Year": "year",
    "Living Planet Index": "lpi_final",
    "Lower confidence interval of Living Planet Index": "ci_low",
    "Upper confidence interval of Living Planet Index": "ci_high",
}


def run() -> None:
    #
    # Load inputs.
    #
    snap = paths.load_snapshot("living_planet_index.csv")
    tb = snap.read()

    #
    # Process data.
    #
    assert set(tb.columns) == set(COLUMNS), f"Unexpected columns: {set(tb.columns) ^ set(COLUMNS)}"
    tb = tb.rename(columns=COLUMNS, errors="raise")
    tb["country"] = tb["country"].astype("category")
    tb = tb.format(["country", "year"])

    #
    # Save outputs.
    #
    ds_meadow = paths.create_dataset(tables=[tb], default_metadata=snap.metadata)
    ds_meadow.save()
