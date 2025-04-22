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
    ds_meadow = paths.load_dataset("yougov_robots")

    # Read table from meadow dataset.
    tb = ds_meadow.read("yougov_robots")
    #
    # Process data.
    #
    # Remove specific groups from the group column (likely small sample sizes for some of these resulting in strange values sometimes)
    groups_to_remove = ["Middle Eastern", "Native American", "Other", "Asian", "Black", "Hispanic", "Two or more races"]
    tb = tb[~tb["group"].isin(groups_to_remove)]

    tb = sh.preprocess_data(
        tb,
        index_columns=["group", "date"],
        pivot_column="which_one__if_any__of_the_following_statements_do_you_most_agree_with",
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
