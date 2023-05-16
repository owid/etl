"""Load a garden dataset and create a grapher dataset."""

import pandas as pd
from owid.catalog import Dataset, Table
from owid.catalog.utils import underscore_table

from etl.helpers import PathFinder, create_dataset, grapher_checks

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def prepare_maize_and_wheat_in_the_context_of_the_ukraine_war(tb_maize_and_wheat: Table) -> Table:
    # Prepare groupings that will be shown in a stacked discrete bar chart.
    # Ukraine and Russia exports of maize and wheat.
    ukraine_and_russia_exports = (
        pd.merge(
            tb_maize_and_wheat[["maize_exports", "wheat_exports"]].loc["Ukraine"],
            tb_maize_and_wheat[["maize_exports", "wheat_exports"]].loc["Russia"],
            left_index=True,
            right_index=True,
            suffixes=(" Ukraine", " Russia"),
        )
        .assign(**{"country": "Ukraine and Russia exports"})
        .reset_index()
    )
    # EU and UK maize and wheat used for animal feed.
    eu_and_uk_feed = (
        pd.merge(
            tb_maize_and_wheat[["maize_animal_feed", "wheat_animal_feed"]].loc["European Union (27)"],
            tb_maize_and_wheat[["maize_animal_feed", "wheat_animal_feed"]].loc["United Kingdom"],
            left_index=True,
            right_index=True,
            suffixes=(" EU", " UK"),
        )
        .assign(**{"country": "EU and UK animal feed"})
        .reset_index()
    )
    # EU and UK maize and wheat devoted to other uses (predominantly biofuels).
    eu_and_uk_biofuels = (
        pd.merge(
            tb_maize_and_wheat[["maize_other_uses", "wheat_other_uses"]].loc["European Union (27)"],
            tb_maize_and_wheat[["maize_other_uses", "wheat_other_uses"]].loc["United Kingdom"],
            left_index=True,
            right_index=True,
            suffixes=(" EU", " UK"),
        )
        .assign(**{"country": "EU and UK biofuels"})
        .reset_index()
    )
    # US maize and wheat used for animal feed.
    us_feed = (
        tb_maize_and_wheat[["maize_animal_feed", "wheat_animal_feed"]]
        .loc["United States"]
        .rename(columns={"maize_animal_feed": "maize_animal_feed US", "wheat_animal_feed": "wheat_animal_feed US"})
        .assign(**{"country": "US animal feed"})
        .reset_index()
    )
    # US maize and wheat devoted to other uses (predominantly biofuels).
    us_biofuels = (
        tb_maize_and_wheat[["maize_other_uses", "wheat_other_uses"]]
        .loc["United States"]
        .rename(columns={"maize_other_uses": "maize_other_uses US", "wheat_other_uses": "wheat_other_uses US"})
        .assign(**{"country": "US biofuels"})
        .reset_index()
    )

    # Combine all groupings.
    combined = pd.concat(
        [ukraine_and_russia_exports, eu_and_uk_feed, eu_and_uk_biofuels, us_feed, us_biofuels], ignore_index=True
    )

    # Set an appropriate index and sort conveniently.
    combined = combined.set_index(["country", "year"], verify_integrity=True).sort_index().sort_index(axis=1)

    # Adapt metadata.
    combined.metadata.short_name = "maize_and_wheat_in_the_context_of_the_ukraine_war"
    for column in combined.columns:
        title = (
            column.replace("maize_", "Maize ")
            .replace("wheat_", "Wheat ")
            .replace("animal_feed", "used for animal feed in")
            .replace("exports", "exported by")
            .replace("other_uses", "used for biofuels in")
        )
        combined[column].metadata.title = title
        combined[column].metadata.unit = "tonnes"
        combined[column].metadata.short_unit = "t"
    combined = underscore_table(combined)

    return combined


def prepare_fertilizer_exports_in_the_context_of_the_ukraine_war(tb_fertilizer_exports: Table) -> Table:
    # Select the relevant countries for the chart.
    fertilizer_exports = tb_fertilizer_exports.loc[["Ukraine", "Russia", "Belarus"]].reset_index()

    # Transpose data.
    fertilizer_exports = fertilizer_exports.pivot(
        index=["item", "year"], columns="country", values=["exports", "share_of_exports"]
    )

    fertilizer_exports.columns = [column[0] + " " + column[1] for column in fertilizer_exports.columns]

    # To be able to work in grapher, rename "item" column to "country".
    fertilizer_exports.index.names = ["country", "year"]

    # Adapt metadata.
    fertilizer_exports.metadata.short_name = "fertilizer_exports_in_the_context_of_the_ukraine_war"
    for column in fertilizer_exports.columns:
        element, country = column.split(" ")
        title = element.capitalize().replace("_", " ") + " from " + country
        fertilizer_exports[column].metadata.title = title
        if "share" in column:
            fertilizer_exports[column].metadata.unit = "%"
            fertilizer_exports[column].metadata.short_unit = "%"
        else:
            fertilizer_exports[column].metadata.unit = "tonnes"
            fertilizer_exports[column].metadata.short_unit = "t"
    fertilizer_exports = underscore_table(fertilizer_exports)

    return fertilizer_exports


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
    tb_agriculture_land_use_evolution = ds_garden["agriculture_land_use_evolution"]
    tb_hypothetical_meat_consumption = ds_garden["hypothetical_meat_consumption"]
    tb_cereal_allocation = ds_garden["cereal_allocation"]
    tb_maize_and_wheat = ds_garden["maize_and_wheat"]
    tb_fertilizer_exports = ds_garden["fertilizer_exports"]

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

    # Prepare maize and what data in the context of the Ukraine war.
    tb_maize_and_wheat_in_the_context_of_the_ukraine_war = prepare_maize_and_wheat_in_the_context_of_the_ukraine_war(
        tb_maize_and_wheat=tb_maize_and_wheat
    )

    # Prepare fertilizer exports data in the context of the Ukraine war.
    tb_fertilizer_exports_in_the_context_of_the_ukraine_war = (
        prepare_fertilizer_exports_in_the_context_of_the_ukraine_war(tb_fertilizer_exports=tb_fertilizer_exports)
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
            tb_agriculture_land_use_evolution,
            tb_hypothetical_meat_consumption,
            tb_cereal_allocation,
            tb_maize_and_wheat_in_the_context_of_the_ukraine_war,
            tb_fertilizer_exports_in_the_context_of_the_ukraine_war,
        ],
        default_metadata=ds_garden.metadata,
    )

    #
    # Checks.
    #
    grapher_checks(ds_grapher)

    # Save changes in the new grapher dataset.
    ds_grapher.save()
