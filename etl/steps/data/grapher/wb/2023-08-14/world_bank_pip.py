"""Load a garden dataset and create a grapher dataset."""

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load garden dataset.
    ds_garden = paths.load_dataset("world_bank_pip")

    # Read tables from garden dataset.
    tb_2011 = ds_garden["inc_or_cons_2011"]
    tb_2017 = ds_garden["inc_or_cons_2017"]

    #
    # Process data.
    #

    #
    # Save outputs.
    #
    # Create a new grapher dataset with the same metadata as the garden dataset.
    ds_grapher = create_dataset(
        dest_dir, tables=[tb_2011, tb_2017], check_variables_metadata=True, default_metadata=ds_garden.metadata
    )

    # Save changes in the new grapher dataset.
    ds_grapher.save()
