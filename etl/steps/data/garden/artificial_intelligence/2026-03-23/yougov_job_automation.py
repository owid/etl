"""Load a meadow dataset and create a garden dataset."""

import shared as sh
from structlog import get_logger

from etl.helpers import PathFinder

log = get_logger()

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("yougov_job_automation")

    # Read table from meadow dataset.
    tb = ds_meadow.read("yougov_job_automation")

    # Remove specific groups from the group column (likely small sample sizes for some of these resulting in strange values sometimes)
    groups_to_remove = [
        "Middle Eastern",
        "Native American",
        "Other",
        "Asian",
        "Black",
        "Hispanic",
        "Two or more races",
        "White",
        "No HS",
        "High school graduate",
        "Some college",
        "2-year",
        "4-year",
        "Post-grad",
    ]
    tb = tb[~tb["group"].isin(groups_to_remove)]

    # Rename the "all" group to be more descriptive
    tb["group"] = tb["group"].replace({"US Adults in work": "All working adults"})

    #
    # Process data.
    #
    tb = sh.preprocess_data(
        tb,
        index_columns=["group", "date"],
        pivot_column="how_worried__if_it_all__are_you_that_your_type_of_work_could_be_automated_within_your_lifetime",
        value_column="value",
    )

    tb = tb.format(["group", "date"])
    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = paths.create_dataset(tables=[tb], check_variables_metadata=True, default_metadata=ds_meadow.metadata)
    # Save changes in the new garden dataset.
    ds_garden.save()
