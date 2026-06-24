"""Load a garden dataset and create a grapher dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Load garden dataset.
    ds_garden = paths.load_dataset("luxembourg_income_study")

    # Read table from garden dataset.
    tb_poverty = ds_garden.read("poverty", reset_index=False)
    tb_incomes = ds_garden.read("incomes", reset_index=False)
    tb_inequality = ds_garden.read("inequality", reset_index=False)

    #
    # Save outputs.
    #
    # Initialize a new grapher dataset.
    ds_grapher = paths.create_dataset(
        tables=[
            tb_poverty,
            tb_incomes,
            tb_inequality,
        ],
        default_metadata=ds_garden.metadata,
    )

    # Save grapher dataset.
    ds_grapher.save()
