"""Load a meadow dataset and create a garden dataset."""

import numpy as np

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)
REGIONS = ["North America", "South America", "Europe", "Africa", "Asia", "Oceania", "World"]


def run() -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("schistosomiasis")
    # Load regions dataset.
    # Read table from meadow dataset.
    tb = ds_meadow.read("schistosomiasis")

    #
    # Process data.
    #
    tb = paths.regions.harmonize_names(tb)
    tb = tb.drop(columns=["region", "age_group", "country_code"])
    tb = paths.regions.add_aggregates(
        tb,
        regions=REGIONS,
        min_num_values_per_year=1,
    )
    # Replace regional values in percentage columns with NaN
    tb.loc[tb["country"].isin(REGIONS), ["programme_coverage__pct", "national_coverage__pct"]] = np.nan
    tb = tb.format(["country", "year"])

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = paths.create_dataset(tables=[tb], check_variables_metadata=True, default_metadata=ds_meadow.metadata)

    # Save changes in the new garden dataset.
    ds_garden.save()
