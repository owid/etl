"""Load a garden dataset and create a grapher dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Load garden dataset.
    ds_garden = paths.load_dataset("wsts_historical_billings")

    # Read tables from garden dataset.
    tb_yearly = ds_garden.read("wsts_historical_billings_yearly", reset_index=False)
    tb_3mma = ds_garden.read("wsts_historical_billings_3mma", reset_index=False)

    #
    # Process data.
    #
    # Rename index to 'country' for grapher compatibility
    tb_yearly = tb_yearly.rename_index_names({"region": "country"})
    tb_3mma = tb_3mma.rename_index_names({"region": "country"})

    #
    # Save outputs.
    #
    # Create a new grapher dataset with the same metadata as the garden dataset.
    ds_grapher = paths.create_dataset(tables=[tb_yearly, tb_3mma], default_metadata=ds_garden.metadata)

    # Save changes in the new grapher dataset.
    ds_grapher.save()
