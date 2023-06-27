"""Load a meadow dataset and create a garden dataset."""

from typing import cast

from owid.catalog import Dataset
from structlog import get_logger

from etl.helpers import PathFinder, create_dataset

log = get_logger()

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    log.info("yougov_jobs.start")

    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = cast(Dataset, paths.load_dependency("yougov_jobs"))

    # Read table from meadow dataset.
    tb = ds_meadow["yougov_jobs"]
    tb = tb.rename(columns=lambda x: x.replace("do_you_think_that_advances_in_artificial_intelligence__", ""))
    tb = tb.rename(columns=lambda x: x.replace("_in_the_u_s__for_the_following_people", ""))
    tb = tb.rename(
        columns=lambda x: x.replace(
            "thinking_about_the_effects_artificial_intelligence__ai__will_have_on_businesses_and_their_employees__", ""
        )
    )

    #
    # Process data.
    #

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(dest_dir, tables=[tb], default_metadata=ds_meadow.metadata)

    # Save changes in the new garden dataset.
    ds_garden.save()

    log.info("yougov_jobs.end")
