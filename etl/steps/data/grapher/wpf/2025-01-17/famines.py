"""Load a garden dataset and create a grapher dataset."""

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

    # Unravel the 'date' column so that there is only one value per row. Years separated by commas are split into separate rows.
    tb = tb.assign(date=tb["date"].str.split(",")).explode("date").drop_duplicates().reset_index(drop=True)

    # Drop rows where the Chinese famine is broken down by year (only China 1958-1962 should exist)
    famine_names_to_drop = ["China 1958", "China 1959", "China 1960", "China 1961", "China 1962"]
    tb = tb[~tb["famine_name"].isin(famine_names_to_drop)]

    # Keep only the earliest date for each country
    tb = tb.sort_values(by="date").drop_duplicates(subset=["famine_name"], keep="first").reset_index(drop=True)

    # Rename columns for plotting
    tb = tb.rename({"country": "place", "famine_name": "country", "date": "year"}, axis=1)
    tb = tb.drop(columns=["place"])
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
