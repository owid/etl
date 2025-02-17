"""Load a meadow dataset and create a garden dataset."""

import shared as sh
from structlog import get_logger

from etl.helpers import PathFinder, create_dataset

log = get_logger()

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("yougov_job_automation")

    # Read table from meadow dataset.
    tb = ds_meadow.read("yougov_job_automation")

    # Remove specific groups from the group column (likely small sample sizes for some of these resulting in strange values sometimes)
    groups_to_remove = ["Middle Eastern", "Native American", "Other", "Asian", "Black", "Hispanic", "Two or more races"]
    tb = tb[~tb["group"].isin(groups_to_remove)]

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
    ds_garden = create_dataset(
        dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=ds_meadow.metadata
    )
    # Save changes in the new garden dataset.
    ds_garden.save()
