"""Load a garden dataset and create a grapher dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Load garden dataset.
    ds_garden = paths.load_dataset("affordable_housing")

    # Read table from garden dataset.
    tb_hcb = ds_garden.read("housing_costs_burden", reset_index=False)
    tb_soc = ds_garden.read("housing_costs_share", reset_index=False)

    #
    # Save outputs.
    #
    # Initialize a new grapher dataset.
    ds_grapher = paths.create_dataset(tables=[tb_hcb, tb_soc], default_metadata=ds_garden.metadata)

    # Save grapher dataset.
    ds_grapher.save()
