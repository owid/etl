"""Load a garden dataset and create a grapher dataset."""

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Groups to select from the survey data.
SELECTED_GROUPS = ["All adults", "18-24", "25-49", "50-64", "65+"]


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load garden dataset and read its main table.
    ds_garden = paths.load_dataset("dietary_choices_uk")
    tb = ds_garden["dietary_choices_uk"].reset_index()

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

    # Convert year column into a number of days since the earliest date in the table.
    tb["year"] = tb["year"].astype("datetime64")
    tb["year"] = (tb["year"] - tb["year"].min()).dt.days

    # Ensure the table is well formatted.
    tb = tb.format()

    #
    # Save outputs.
    #
    # Create a new grapher dataset.
    ds_grapher = create_dataset(dest_dir, tables=[tb], check_variables_metadata=True)
    ds_grapher.save()
