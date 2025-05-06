"""Load a garden dataset and create a grapher dataset."""

import pandas as pd

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Groups to select from the survey data.
SELECTED_GROUPS = ["All adults", "18-24", "25-49", "50-64", "65+"]


def run() -> None:
    #
    # Load inputs.
    #
    # Load garden dataset and read its main table.
    ds_garden = paths.load_dataset("dietary_choices_uk")
    tb = ds_garden.read("dietary_choices_uk", safe_types=False)

    #
    # Process data.
    #
    # Adapt table format to grapher requirements.
    tb = tb.rename(columns={"group": "country", "date": "year"}, errors="raise").drop(
        columns=["base", "base_unweighted"], errors="raise"
    )

    # Select only the groups that are going to be displayed in grapher.
    tb = tb[tb["country"].isin(SELECTED_GROUPS)].reset_index(drop=True)

    # Sanity check.
    error = "A survey group may have been renamed."
    assert set(tb["country"]) == set(SELECTED_GROUPS), error

    # Prepare display metadata.
    date_earliest = tb["year"].astype(str).min()
    for column in tb.drop(columns=["country", "year"]).columns:
        tb[column].metadata.display["yearIsDay"] = True
        tb[column].metadata.display["zeroDay"] = date_earliest

    # Convert year column into a number of days since the earliest date in the table.
    tb["year"] = tb["year"].astype("datetime64")
    tb["year"] = (tb["year"] - pd.to_datetime(date_earliest)).dt.days

    # Improve table format.
    tb = tb.format()

    #
    # Save outputs.
    #
    # Initialize a new grapher dataset.
    ds_grapher = paths.create_dataset(tables=[tb])

    # Save grapher dataset.
    ds_grapher.save()
