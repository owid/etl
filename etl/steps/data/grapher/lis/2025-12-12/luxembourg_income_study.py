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
    tb_absolute_poverty = ds_garden.read("absolute_poverty", reset_index=False)
    tb_relative_poverty = ds_garden.read("relative_poverty", reset_index=False)
    tb_mean_median = ds_garden.read("mean_median", reset_index=False)
    tb_deciles = ds_garden.read("deciles", reset_index=False)
    tb_inequality = ds_garden.read("inequality", reset_index=False)

    #
    # Save outputs.
    #
    # Initialize a new grapher dataset.
    ds_grapher = paths.create_dataset(
        tables=[
            tb_absolute_poverty,
            tb_relative_poverty,
            tb_mean_median,
            tb_deciles,
            tb_inequality,
        ],
        default_metadata=ds_garden.metadata,
    )

    # Save grapher dataset.
    ds_grapher.save()
