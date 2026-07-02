"""Load a garden dataset and create a grapher dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Load garden dataset.
    ds_garden = paths.load_dataset("who_glass_by_antibiotic")

    # Read table from garden dataset.
    tb_anti = ds_garden.read("antibiotic_table", reset_index=False)
    tb_bci = ds_garden.read("bci_table", reset_index=False)

    #
    # Save outputs.
    #
    # Create a new grapher dataset with the same metadata as the garden dataset.
    ds_grapher = paths.create_dataset(
        tables=[tb_bci, tb_anti], check_variables_metadata=True, default_metadata=ds_garden.metadata
    )

    # Save changes in the new grapher dataset.
    ds_grapher.save()
