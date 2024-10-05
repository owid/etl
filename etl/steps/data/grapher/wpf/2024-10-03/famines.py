"""Load a garden dataset and create a grapher dataset."""

import pandas as pd

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load garden dataset.
    ds_garden = paths.load_dataset("famines")

    # Read table from garden dataset.
    tb = ds_garden["famines"].reset_index()

    #
    # Process data.
    #
    # Divide each row's 'wpf_authoritative_mortality_estimate' by the length of the corresponding 'Date' value to assume a uniform distribution of deaths over the period
    tb["wpf_authoritative_mortality_estimate"] = tb.apply(
        lambda row: row["wpf_authoritative_mortality_estimate"] / len(row["date"].split(","))
        if pd.notna(row["date"])
        else row["wpf_authoritative_mortality_estimate"],
        axis=1,
    )
    # Remove rows where 'wpf_authoritative_mortality_estimate' is NaN
    tb = tb.dropna(subset=["wpf_authoritative_mortality_estimate"])

    # Unravel the 'date' column so that there is only one value per row. Years separated by commas are split into separate rows.
    tb = unravel_dates(tb)
    tb = tb.rename({"conventional_title": "country", "date": "year"}, axis=1)
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


def unravel_dates(tb):
    """
    Unravel the 'date' column so that there is only one value per row. Years separated by commas are split into separate rows.
    """
    # Split the 'date' column into multiple rows
    tb = tb.assign(date=tb["date"].str.split(",")).explode("date").drop_duplicates().reset_index(drop=True)

    return tb
