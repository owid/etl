"""Load a garden dataset and create a grapher dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Load garden dataset.
    ds_garden = paths.load_dataset("media_deaths")

    # Read table from garden dataset.
    tb = ds_garden.read("media_deaths")

    origins_tb = tb["year"].m.origins

    # transpose table to have causes as columns
    tb = tb.T
    tb.columns = tb.iloc[0]  # set the first row as header
    tb = tb.iloc[2:]  # remove the first two rows (index & year)

    # add metadata for each column
    for col in tb.columns:
        tb[col].m.title = f"Cause of Death: {col}"
        tb[col].m.description = f"This column contains the share of media mentions for the cause of death: {col}"
        tb[col].m.origins = origins_tb
        tb[col].m.unit = "%"
        tb[col].m.short_unit = "%"

    tb = tb.reset_index()

    # rename cause to country for grapher
    tb = tb.rename(columns={"index": "country"})
    tb["year"] = 2023

    tb = tb.format(["country", "year"])

    #
    # Save outputs.
    #
    # Initialize a new grapher dataset.
    ds_grapher = paths.create_dataset(tables=[tb], default_metadata=ds_garden.metadata)

    # Save grapher dataset.
    ds_grapher.save()
