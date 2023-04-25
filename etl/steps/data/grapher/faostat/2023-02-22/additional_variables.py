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
    tb_area_used_per_crop_type = ds_garden["area_used_per_crop_type"]
    tb_sustainable_and_overexploited_fish = ds_garden["share_of_sustainable_and_overexploited_fish"]
    tb_land_spared_by_increased_crop_yields = ds_garden["land_spared_by_increased_crop_yields"]
    tb_food_available_for_consumption = ds_garden["food_available_for_consumption"]
    tb_macronutrient_compositions = ds_garden["macronutrient_compositions"]
    tb_fertilizers = ds_garden["fertilizers"]
    tb_vegetable_oil_yields = ds_garden["vegetable_oil_yields"]

    #
    # Process data.
    #
    # To insert table into grapher DB, change "item" column to "country" (which will be changed back in the admin).
    tb_area_used_per_crop_type = tb_area_used_per_crop_type.reset_index().rename(columns={"item": "country"})

    # For land spared by increased crop yields, for the moment we only need global data, by crop type.
    # And again, change "item" to "country" to fit grapher DB needs.
    tb_land_spared_by_increased_crop_yields = tb_land_spared_by_increased_crop_yields.reset_index()
    tb_land_spared_by_increased_crop_yields = (
        tb_land_spared_by_increased_crop_yields[tb_land_spared_by_increased_crop_yields["country"] == "World"]
        .drop(columns=["country"])
        .rename(columns={"item": "country"})
        .set_index(["country", "year"], verify_integrity=True)
        .sort_index()
    )

    #
    # Save outputs.
    #
    # Create a new grapher dataset with the same metadata as the garden dataset.
    ds_grapher = create_dataset(
        dest_dir,
        tables=[
            tb_arable_land_per_crop_output,
            tb_area_used_per_crop_type,
            tb_sustainable_and_overexploited_fish,
            tb_land_spared_by_increased_crop_yields,
            tb_food_available_for_consumption,
            tb_macronutrient_compositions,
            tb_fertilizers,
            tb_vegetable_oil_yields,
        ],
        default_metadata=ds_garden.metadata,
    )

    #
    # Checks.
    #
    grapher_checks(ds_grapher)

    # Save changes in the new grapher dataset.
    ds_grapher.save()
