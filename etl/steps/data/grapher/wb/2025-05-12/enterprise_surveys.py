"""Load a garden dataset and create a grapher dataset."""

import owid.catalog.processing as pr

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Load garden dataset.
    ds_garden = paths.load_dataset("enterprise_surveys")

    # Read table from garden dataset.
    tb = ds_garden.read("enterprise_surveys", reset_index=False)
    print(tb.columns)

    # Assuming your DataFrame is named df
    df_long = pr.melt(
        df,
        id_vars=["year", "country"],  # Keep these as identifier variables
        var_name="indicator_name",  # Name of the new column for indicators
        value_name="value",  # Name of the new column for the corresponding values
    )

    #
    # Save outputs.
    #
    # Initialize a new grapher dataset.
    ds_grapher = paths.create_dataset(tables=[tb], default_metadata=ds_garden.metadata)

    # Save grapher dataset.
    ds_grapher.save()
