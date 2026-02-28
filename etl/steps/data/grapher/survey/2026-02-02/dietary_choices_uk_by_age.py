"""Load a garden dataset and create a grapher dataset with UK data, using age groups as entities."""

import pandas as pd

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Age groups to select from the UK survey data.
SELECTED_GROUPS = ["All adults", "18-24", "25-49", "50-64", "65+"]


def run() -> None:
    #
    # Load inputs.
    #
    # Load garden dataset and read its main table.
    ds_garden = paths.load_dataset("dietary_choices")
    tb = ds_garden.read("dietary_choices", safe_types=False)

    #
    # Process data.
    #
    # Filter to UK data only.
    tb = tb[tb["country"] == "United Kingdom"].reset_index(drop=True)

    # Drop columns not needed for grapher.
    tb = tb.drop(columns=["country", "base", "base_unweighted"], errors="raise")

    # Filter to selected age groups.
    tb = tb[tb["group"].isin(SELECTED_GROUPS)].reset_index(drop=True)

    # Sanity check.
    error = "A survey group may have been renamed."
    assert set(tb["group"]) == set(SELECTED_GROUPS), error

    # Use age group as the entity column.
    tb = tb.rename(columns={"group": "country", "date": "year"}, errors="raise")

    # Prepare display metadata.
    date_earliest = tb["year"].astype(str).min()
    for column in tb.drop(columns=["country", "year"]).columns:
        tb[column].metadata.display["yearIsDay"] = True
        tb[column].metadata.display["zeroDay"] = date_earliest

    # Convert year column into a number of days since the earliest date in the table.
    tb["year"] = tb["year"].astype("datetime64")
    tb["year"] = (tb["year"] - pd.to_datetime(date_earliest)).dt.days

    # Improve table format.
    tb = tb.format(short_name=paths.short_name)

    # Keep only origin of survey about Brits (and remove the one about Americans).
    for column in tb.columns:
        tb[column].metadata.origins = [origin for origin in tb[column].metadata.origins if "Brits" in origin.title]

    # Update dataset title.
    tb.metadata.title = "Dietary choices of Brits by age"

    #
    # Save outputs.
    #
    # Initialize a new grapher dataset.
    ds_grapher = paths.create_dataset(tables=[tb])

    # Save grapher dataset.
    ds_grapher.save()
