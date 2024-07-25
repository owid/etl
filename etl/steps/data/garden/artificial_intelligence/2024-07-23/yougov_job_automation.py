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
    tb = ds_meadow["yougov_job_automation"].reset_index()
    #
    # Process data.
    #
    tb = sh.preprocess_data(
        tb,
        date_column="date",
        index_columns=["group", "days_since_2021"],
        pivot_column="how_worried__if_it_all__are_you_that_your_type_of_work_could_be_automated_within_your_lifetime",
        value_column="value",
    )
    tb = tb.format(["group", "days_since_2021"])
    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=ds_meadow.metadata
    )
    # Save changes in the new garden dataset.
    ds_garden.save()
