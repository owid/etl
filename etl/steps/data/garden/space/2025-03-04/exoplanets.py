"""Load a meadow dataset and create a garden dataset."""

import pandas as pd
from owid.datautils.dataframes import map_series

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Mapping for column names.
COLUMNS = {
    "pl_name": "planet",
    "disc_year": "year",
    "discoverymethod": "method",
}

# Label for the "Other methods" category.
OTHER_METHODS_LABEL = "Other methods"

# Mapping of discovery method names (sorted by decreasing expected number of discoveries).
METHOD_MAPPING = {
    "Transit": "Transit",
    "Radial Velocity": "Radial velocity",
    "Microlensing": "Microlensing",
    "Imaging": OTHER_METHODS_LABEL,
    "Transit Timing Variations": OTHER_METHODS_LABEL,
    "Eclipse Timing Variations": OTHER_METHODS_LABEL,
    "Orbital Brightness Modulation": OTHER_METHODS_LABEL,
    "Pulsar Timing": OTHER_METHODS_LABEL,
    "Astrometry": OTHER_METHODS_LABEL,
    "Pulsation Timing Variations": OTHER_METHODS_LABEL,
    "Disk Kinematics": OTHER_METHODS_LABEL,
}


def check_inputs(tb):
    error = "List of discovery methods has changed."
    assert set(tb["discoverymethod"]) == set(METHOD_MAPPING.keys()), error
    counts = tb.groupby(["discoverymethod"], as_index=False).count().sort_values("disc_year", ascending=False)
    error = "The three most common discovery methods have changed."
    assert counts["discoverymethod"].tolist()[0:3] == list(METHOD_MAPPING.keys())[0:3], error
    error = f"The sum of discoveries from {OTHER_METHODS_LABEL} (any that is not one of the main three) was expected to be below 3%."
    assert counts[3:]["disc_year"].sum() / counts["disc_year"].sum() * 100 < 3, error


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("exoplanets")

    # Read table from meadow dataset.
    tb = ds_meadow.read("exoplanets")

    #
    # Process data.
    #
    # Sanity check inputs.
    check_inputs(tb=tb)

    # Rename columns.
    tb = tb.rename(columns=COLUMNS, errors="raise")

    # Filter out data from the current year, which is incomplete.
    current_year = int(tb["year"].metadata.origins[0].date_published[0:4])
    tb = tb[tb["year"] < current_year].reset_index(drop=True)

    # Keep the 3 top discovery methods, and label the rest as "Other methods".
    tb["method"] = map_series(
        tb["method"], mapping=METHOD_MAPPING, warn_on_missing_mappings=True, warn_on_unused_mappings=True
    )

    # Count discoveries by year and method.
    tb = tb.groupby(["year", "method"], as_index=False).agg({"planet": "count"}).rename(columns={"planet": "n_planets"})

    # Create all possible combinations of years and methods.
    all_years = range(tb["year"].min(), tb["year"].max() + 1)
    all_methods = tb["method"].unique()

    # Reindex the table to include all (year, method) combinations.
    tb = (
        tb.set_index(["year", "method"])
        .reindex(pd.MultiIndex.from_product([all_years, all_methods], names=["year", "method"]))
        .fillna({"n_planets": 0})
        .reset_index()
        .astype({"n_planets": int})
    )

    # Calculate cumulative number of discovered exoplanets by method.
    tb = tb.sort_values("year").reset_index(drop=True)
    tb["n_planets_cumulative"] = tb.groupby("method")["n_planets"].cumsum()

    # Improve table format.
    tb = tb.format(["year", "method"])

    #
    # Save outputs.
    #
    # Initialize a new garden dataset.
    ds_garden = create_dataset(dest_dir, tables=[tb], default_metadata=ds_meadow.metadata)

    # Save garden dataset.
    ds_garden.save()
