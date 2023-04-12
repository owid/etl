"""Load a garden dataset and create a grapher dataset."""

from owid.catalog import Dataset

from etl.helpers import PathFinder, create_dataset, grapher_checks

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load garden dataset.
    ds_garden: Dataset = paths.load_dependency("additional_variables")

    # Read tables from garden dataset.
    tb_arable_land_per_crop_output = ds_garden["arable_land_per_crop_output"]
    tb_sustainable_and_overexploited_fish = ds_garden["share_of_sustainable_and_overexploited_fish"]
    # To insert table into grapher DB, change "item" column to "country" (which will be changed back in the admin).
    tb_area_used_per_crop_type = ds_garden["area_used_per_crop_type"].reset_index().rename(columns={"item": "country"})

    #
    # Process data.
    #

    #
    # Save outputs.
    #
    # Create a new grapher dataset with the same metadata as the garden dataset.
    ds_grapher = create_dataset(
        dest_dir,
        tables=[tb_arable_land_per_crop_output, tb_area_used_per_crop_type, tb_sustainable_and_overexploited_fish],
        default_metadata=ds_garden.metadata,
    )

    #
    # Checks.
    #
    grapher_checks(ds_grapher)

    # Save changes in the new grapher dataset.
    ds_grapher.save()
