"""Load a meadow dataset and create a garden dataset."""

from owid.catalog import Dataset
from structlog import get_logger

from etl.helpers import PathFinder, create_dataset

log = get_logger()

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


# Unit conversion factor to change from bushel of corn to metric tonnes.
BUSHELS_OF_CORN_TO_TONNES = 0.0254

# Unit conversion factor to change from acres to hectares.
ACRES_TO_HECTARES = 0.4047


def run(dest_dir: str) -> None:
    log.info("us_corn_yields.start")

    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow: Dataset = paths.load_dependency("us_corn_yields")

    # Read table from meadow dataset.
    tb = ds_meadow["us_corn_yields"]

    #
    # Process data.
    #
    # Change units of corn yield.
    tb["corn_yield"] *= BUSHELS_OF_CORN_TO_TONNES / ACRES_TO_HECTARES

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(dest_dir, tables=[tb], default_metadata=ds_meadow.metadata)

    # Save changes in the new garden dataset.
    ds_garden.save()

    log.info("us_corn_yields.end")
