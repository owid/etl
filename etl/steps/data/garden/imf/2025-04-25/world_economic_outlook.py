"""Load a meadow dataset and create a garden dataset."""

import pandas as pd

from etl.data_helpers import geo
from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("world_economic_outlook")

    # Read table from meadow dataset.
    tb = ds_meadow.read("world_economic_outlook")

    #
    # Process data.
    #
    tb = geo.harmonize_countries(
        df=tb,
        countries_file=paths.country_mapping_path,
    )

    # Convert 'year' to datetime if using time interpolation
    tb["year_dt"] = pd.to_datetime(tb["year"], format="%Y")
    tb = tb.sort_values(["country", "year_dt"]).reset_index(drop=True)
    for col in tb.columns:
        if col not in ["country", "year", "year_dt"]:
            # Apply interpolation and rolling per group
            tb["rolling_" + col] = (
                tb.groupby("country")
                .apply(lambda g: g.set_index("year_dt")[col].interpolate(method="time").rolling(10).mean())
                .reset_index(drop=True)
            )
    tb = tb.drop(columns=["year_dt"])
    tb = tb.format(["country", "year"])

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = paths.create_dataset(tables=[tb], check_variables_metadata=True, default_metadata=ds_meadow.metadata)

    # Save changes in the new garden dataset.
    ds_garden.save()
