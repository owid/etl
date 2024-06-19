"""Load a garden dataset and create a grapher dataset."""

import pandas as pd

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load garden dataset
    ds_garden = paths.load_dataset("guinea_worm")

    # Read table from garden dataset.
    tb = ds_garden["guinea_worm"].reset_index()

    # remove certified year for all years except the current year
    tb = remove_certified_year(tb, 2023)

    tb["year_certified"] = tb["year_certified"].replace({"Pre-certification": 3000, "Endemic": 4000})
    # change to numeric dtype
    tb["year_certified"] = (
        pd.to_numeric(tb["year_certified"], errors="coerce").astype("Int64").copy_metadata(tb["year_certified"])
    )

    tb = tb.format(["country", "year"])

    #
    # Save outputs.
    #
    # Create a new grapher dataset with the same metadata as the garden dataset.
    ds_grapher = create_dataset(
        dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=ds_garden.metadata
    )

    # Save changes in the new grapher dataset.
    ds_grapher.save()


def remove_certified_year(tb, current_year):
    """Remove the year in which a country was certified as disease free
    except for the row of the current year."""
    tb.loc[tb["year"] != current_year, "year_certified"] = pd.NA
    return tb
