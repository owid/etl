"""Load a garden dataset and create a grapher dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Load garden dataset.
    ds_garden = paths.load_dataset("unaids")

    # Read table from garden dataset.
    tables = [
        ds_garden["epi"],
        ds_garden["gam"],
        ds_garden["gam_sex"],
        ds_garden["gam_age"],
        ds_garden["gam_group"],
        ds_garden["gam_estimates"],
        ds_garden["gam_hepatitis"],
        ds_garden["gam_age_group"],
        ds_garden["gam_sex_group"],
        ds_garden["gam_age_sex"],
        ds_garden["gam_age_sex_group"],
    ]

    #
    # Save outputs.
    #
    # Initialize a new grapher dataset.
    ds_grapher = paths.create_dataset(tables=tables, default_metadata=ds_garden.metadata)

    # Save grapher dataset.
    ds_grapher.save()
