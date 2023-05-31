"""Load a garden dataset and create a grapher dataset."""

from typing import cast

from owid.catalog import Dataset

from etl.helpers import PathFinder, create_dataset, grapher_checks

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #

    # Load garden dataset.
    ds_garden = cast(Dataset, paths.load_dependency("long_term_crop_yields"))

    # Read table from garden dataset.
    tb = ds_garden["long_term_crop_yields"]

    #
    # Save outputs.
    #
    # Create a new grapher dataset with the same metadata as the garden dataset.
    ds_grapher = create_dataset(dest_dir, tables=[tb], default_metadata=ds_garden.metadata)

    # To avoid a very long (and mostly irrelevant) dataset description, remove the sources descriptions.
    for source in ds_grapher.metadata.sources:
        source.description = None

    #
    # Checks.
    #
    grapher_checks(ds_grapher)

    # Save changes in the new grapher dataset.
    ds_grapher.save()
