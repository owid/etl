"""Load a meadow dataset and create a garden dataset."""

import numpy as np

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("oil_spills")

    # Read table from meadow dataset.
    tb = ds_meadow["oil_spills"].reset_index()

    #
    # Process data.
    #

    # Group the data by decade for 'World'
    for column in ["bel_700t", "ab_700t", "oil_spilled"]:
        mask = tb["spill_type"] == "World"  # Filter for 'world' country
        tb.loc[mask, "decadal_" + str(column)] = (
            tb.loc[mask, column].groupby(tb.loc[mask, "year"] // 10 * 10).transform("mean")
        )
        # set NaN everywhere except start of a decade
        tb.loc[mask, "decadal_" + str(column)] = tb.loc[mask, "decadal_" + str(column)].where(
            tb.loc[mask, "year"].astype(int) % 10 == 0, np.nan
        )

    # Replace any '__' in column names with a space (done because of double _ in some variable names)
    newnames = [name.replace("__", "_") for name in tb.columns]
    tb.columns = newnames

    for column in ["decadal_bel_700t", "decadal_ab_700t", "decadal_oil_spilled"]:
        tb[column].metadata.origins = tb["bel_700t"].metadata.origins

    tb = tb.format(["spill_type", "year"])
    #
    # Save outputs.
    #

    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=ds_meadow.metadata
    )

    # Save changes in the new garden dataset.
    ds_garden.save()
