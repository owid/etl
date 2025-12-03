"""Load a garden dataset and create a grapher dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Load garden dataset.
    ds_garden = paths.load_dataset("historical_poverty")

    # Read table from garden dataset.
    tb_constant_inequality = ds_garden.read("constant_inequality", reset_index=False)
    tb_population_constant_inequality = ds_garden.read("population_constant_inequality", reset_index=False)
    tb_interpolated_quantiles = ds_garden.read("interpolated_quantiles", reset_index=False)
    tb_population_interpolated_quantiles = ds_garden.read("population_interpolated_quantiles", reset_index=False)
    tb_interpolated_ginis = ds_garden.read("interpolated_ginis", reset_index=False)
    tb_population_interpolated_ginis = ds_garden.read("population_interpolated_ginis", reset_index=False)

    tb_comparison = ds_garden.read("comparison", reset_index=False)

    #
    # Save outputs.
    #
    # Initialize a new grapher dataset.
    ds_grapher = paths.create_dataset(
        tables=[
            tb_constant_inequality,
            tb_interpolated_ginis,
            tb_interpolated_quantiles,
            tb_population_constant_inequality,
            tb_population_interpolated_ginis,
            tb_population_interpolated_quantiles,
            tb_comparison,
        ],
        default_metadata=ds_garden.metadata,
    )

    # Save grapher dataset.
    ds_grapher.save()
