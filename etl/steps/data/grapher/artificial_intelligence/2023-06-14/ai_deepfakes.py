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
    ds_garden = cast(Dataset, paths.load_dependency("ai_deepfakes"))
    tb_garden = ds_garden["ai_deepfakes"]

    # Add the name of the model as country for plotting in grapher
    tb_garden["country"] = "Celeb-DF"

    #
    # Save outputs.
    #
    # Create a new grapher dataset with the same metadata as the garden dataset.
    ds_grapher = create_dataset(dest_dir, tables=[tb_garden], default_metadata=ds_garden.metadata)

    #
    # Checks.
    #
    grapher_checks(ds_grapher)

    # Save changes in the new grapher dataset.
    ds_grapher.save()
