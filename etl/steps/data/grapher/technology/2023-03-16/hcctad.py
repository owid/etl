"""Load a garden dataset and create a grapher dataset."""

from owid.catalog import Dataset

from etl.helpers import PathFinder, create_dataset, grapher_checks

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load garden dataset.
    ds_garden: Dataset = paths.load_dependency("hcctad")

    # Read table from garden dataset.
    tb_garden = ds_garden["hcctad"]

    #
    # Process data.
    #

    #
    # Save outputs.
    #
    # Create a new grapher dataset with the same metadata as the garden dataset.
    ds_grapher = create_dataset(dest_dir, tables=[tb_garden], default_metadata=ds_garden.metadata)

    #
    # Checks.
    #
    grapher_checks(ds_garden)

    # Save changes in the new grapher dataset.
    ds_grapher.save()
