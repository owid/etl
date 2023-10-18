"""Load a garden dataset and create a grapher dataset."""

from owid.catalog import Dataset

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load garden dataset.
    ds_garden: Dataset = paths.load_dependency("near_surface_temperature")

    # Read table from garden dataset.
    tb_garden = ds_garden["near_surface_temperature"].reset_index()

    # For compatibility with grapher, change the name of "region" column to "country".
    tb_garden = tb_garden.rename(columns={"region": "country"})

    #
    # Process data.
    #

    #
    # Save outputs.
    #
    # Create a new grapher dataset with the same metadata as the garden dataset.
    ds_grapher = Dataset.create_empty(dest_dir, ds_garden.metadata)

    # Add table of processed data to the new dataset.
    ds_grapher.add(tb_garden)

    # Save changes in the new grapher dataset.
    ds_grapher.save()
