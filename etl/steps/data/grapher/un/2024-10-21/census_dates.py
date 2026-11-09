"""Load a garden dataset and create a grapher dataset."""

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load garden dataset.
    ds_garden = paths.load_dataset("census_dates")

    # Read table from garden dataset.
    tb = ds_garden["years_since_last_census"]

    #
    # Process data.
    #
    # Store the 0/1 "census in the last 10 years" flag as No/Yes, so the Swimlane chart can show it.
    # NOTE: On each update, check that the flag is still 0/1. The recoding fails on any other value.
    unknown = set(tb["recent_census"].dropna().unique()) - {0, 1}
    assert not unknown, f"`recent_census` has values other than 0 and 1: {unknown}"
    metadata = tb["recent_census"].metadata
    tb["recent_census"] = tb["recent_census"].map({0: "No", 1: "Yes"}).astype("category")
    tb["recent_census"].metadata = metadata
    tb["recent_census"].metadata.type = "string"
    tb["recent_census"].metadata.sort = ["No", "Yes"]

    #
    # Save outputs.
    #
    # Create a new grapher dataset with the same metadata as the garden dataset.
    ds_grapher = create_dataset(
        dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=ds_garden.metadata
    )

    # Save changes in the new grapher dataset.
    ds_grapher.save()
