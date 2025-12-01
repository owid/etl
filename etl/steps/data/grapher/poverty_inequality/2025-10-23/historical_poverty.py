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
    tb = ds_garden.read("historical_poverty", reset_index=False)
    tb_population = ds_garden.read("population", reset_index=False)
    tb_from_interpolated_mean_gini = ds_garden.read("historical_poverty_from_interpolated_mean_gini", reset_index=False)
    tb_population_from_interpolated_mean_gini = ds_garden.read(
        "population_from_interpolated_mean_gini", reset_index=False
    )
    tb_from_interpolated_mean = ds_garden.read("historical_poverty_from_interpolated_mean", reset_index=False)
    tb_population_from_interpolated_mean = ds_garden.read("population_from_interpolated_mean", reset_index=False)
    tb_comparison = ds_garden.read("comparison", reset_index=False)

    #
    # Save outputs.
    #
    # Initialize a new grapher dataset.
    ds_grapher = paths.create_dataset(
        tables=[
            tb,
            tb_from_interpolated_mean_gini,
            tb_from_interpolated_mean,
            tb_population,
            tb_population_from_interpolated_mean_gini,
            tb_population_from_interpolated_mean,
            tb_comparison,
        ],
        default_metadata=ds_garden.metadata,
    )

    # Save grapher dataset.
    ds_grapher.save()
