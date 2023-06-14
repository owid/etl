"""Load a meadow dataset and create a garden dataset."""

from typing import cast

from owid.catalog import Dataset
from structlog import get_logger

from etl.helpers import PathFinder, create_dataset

log = get_logger()

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    log.info("papers_with_code_math.start")

    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = cast(Dataset, paths.load_dependency("papers_with_code_math"))

    # Read table from meadow dataset.
    tb = ds_meadow["papers_with_code_math"]
    tb.drop("additional_data", axis=1, inplace=True)
    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(dest_dir, tables=[tb], default_metadata=ds_meadow.metadata)

    # Save changes in the new garden dataset.
    ds_garden.save()

    log.info("papers_with_code_math.end")
