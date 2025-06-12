"""Load a garden dataset and create a grapher dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Load garden dataset.
    ds_garden = paths.load_dataset("tuberculosis")

    # Read table from garden dataset.
    tb_cases_pre_1975 = ds_garden.read("cases_before_1975", reset_index=False)
    tb_cases_post_1975 = ds_garden.read("cases_after_1975", reset_index=False)
    tb_deaths_pre_1979 = ds_garden.read("deaths_before_1979", reset_index=False)
    tb_deaths_post_1979 = ds_garden.read("deaths_after_1979", reset_index=False)

    #
    # Save outputs.
    #
    # Initialize a new grapher dataset.
    ds_grapher = paths.create_dataset(
        tables=[tb_cases_pre_1975, tb_cases_post_1975, tb_deaths_pre_1979, tb_deaths_post_1979],
        default_metadata=ds_garden.metadata,
    )

    # Save grapher dataset.
    ds_grapher.save()
