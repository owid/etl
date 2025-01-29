"""Load a garden dataset and create a grapher dataset."""

from etl.helpers import PathFinder, create_dataset, grapher_checks

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load garden dataset.
    ds_garden = paths.load_dataset("unaids")

    #
    # Process data.
    #
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
    # Create a new grapher dataset with the same metadata as the garden dataset.
    ds_grapher = create_dataset(
        dest_dir,
        tables=tables,
        default_metadata=ds_garden.metadata,
    )

    #
    # Checks.
    #
    grapher_checks(ds_grapher)

    # Save changes in the new grapher dataset.
    ds_grapher.save()
