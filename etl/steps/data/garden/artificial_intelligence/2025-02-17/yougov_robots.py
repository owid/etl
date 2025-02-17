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
    ds_meadow = paths.load_dataset("yougov_robots")

    # Read table from meadow dataset.
    tb = ds_meadow.read("yougov_robots")
    #
    # Process data.
    #
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
    ds_garden = create_dataset(
        dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=ds_meadow.metadata
    )
    # Save changes in the new garden dataset.
    ds_garden.save()
