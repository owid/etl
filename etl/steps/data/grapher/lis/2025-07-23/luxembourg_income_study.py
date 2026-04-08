"""Load luxembourg_income_Study garden dataset and create the luxembourg_income_study grapher dataset."""

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
    tb_garden = ds_garden.read("luxembourg_income_study", reset_index=False)

    #
    # Save outputs.
    #
    ds_garden = paths.create_dataset(
        tables=[tb_garden], check_variables_metadata=True, default_metadata=ds_garden.metadata
    )

    ds_garden.save()
