"""Load a meadow dataset and create a garden dataset."""

from typing import cast

from owid.catalog import Dataset
from structlog import get_logger

from etl.helpers import PathFinder, create_dataset

log = get_logger()

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    log.info("yougov_end_of_humanity.start")

    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = cast(Dataset, paths.load_dependency("yougov_end_of_humanity"))

    # Read table from meadow dataset.
    tb = ds_meadow["yougov_end_of_humanity"]

    #
    # Process data.
    #
    # Shorten variable names
    tb = tb.rename(columns=lambda x: x.replace("how_", "").replace("__if_at_all__", "_").replace("__", "_"))
    tb = tb.rename(columns=lambda x: x.replace("do_you_think_it_is_that_the_following_would_cause_the_", ""))
    tb = tb.rename(columns=lambda x: x.replace("are_you_about_the_possibility_that_the_following_will_cause_the_", ""))
    tb = tb.rename(columns=lambda x: "answers_age" + x if x.startswith("_") else x)

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(dest_dir, tables=[tb], default_metadata=ds_meadow.metadata)

    # Save changes in the new garden dataset.
    ds_garden.save()

    log.info("yougov_end_of_humanity.end")
