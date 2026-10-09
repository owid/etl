"""Grapher step for OECD Family Database."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Load garden dataset
    ds_garden = paths.load_dataset("family_database")

    # Read table from garden dataset.
    marriage_divorce_rates = ds_garden.read("marriage_divorce_rates", reset_index=False)
    births_outside_marriage = ds_garden.read("births_outside_marriage", reset_index=False)
    children_in_families = ds_garden.read("children_in_families", reset_index=False)
    mean_age_first_marriage = ds_garden.read("mean_age_first_marriage", reset_index=False)

    #
    # Save outputs.
    #
    # Create a new grapher dataset
    ds_grapher = paths.create_dataset(
        tables=[marriage_divorce_rates, mean_age_first_marriage, births_outside_marriage, children_in_families],
        check_variables_metadata=True,
        default_metadata=ds_garden.metadata,
    )

    # Save the dataset
    ds_grapher.save()
