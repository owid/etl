"""Load a meadow dataset and create a garden dataset."""

from typing import cast

from owid.catalog import Dataset
from shared import add_metadata_vars_distribution

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = cast(Dataset, paths.load_dependency("world_inequality_database_distribution"))

    # Read table from meadow dataset.
    tb = ds_meadow["world_inequality_database_distribution"].reset_index()

    #
    # Process data.
    # Multiple share and share_extrapolated columns by 100
    tb[["share", "share_extrapolated"]] *= 100
    tb = tb.set_index(["country", "year", "welfare", "percentile", "p"], verify_integrity=True)

    # Add metadata by code
    tb = add_metadata_vars_distribution(tb)

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(dest_dir, tables=[tb], default_metadata=ds_meadow.metadata)

    # Save changes in the new garden dataset.
    ds_garden.save()
