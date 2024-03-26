"""Load a meadow dataset and create a garden dataset."""

"""Load a meadow dataset and create a garden dataset."""

from owid.catalog import Table

from etl.data_helpers.misc import add_origins_to_mortality_database
from etl.helpers import PathFinder, create_dataset

from . import shared

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    # Load relevant datasets.
    ds_data = paths.load_dataset("mortality_database")
    ds_regions = paths.load_dataset("regions")
    ds_income_groups = paths.load_dataset("income_groups")
    ds_population = paths.load_dataset("population")
    tb_population = ds_population["population"].reset_index()
    tb = ds_data["neuropsychiatric_conditions__both_sexes__all_ages"].reset_index()
    tb = add_origins_to_mortality_database(tb_who=tb)

    tb_data = tb.filter(
        items=["country", "year"]
        + [col for col in tb if col.startswith("deaths_from_neuropsychiatric_conditions_per_100_000_people")]
    )
    tb_data = tb_data.dropna(
        subset=["deaths_from_neuropsychiatric_conditions_per_100_000_people_in__both_sexes_aged_all_ages"]
    )

    df_merged = shared.map_countries_and_merge_data(
        tb_data,
        ds_regions,
        ds_income_groups,
        tb_population,
        "deaths_from_neuropsychiatric_conditions_per_100_000_people_in__both_sexes_aged_all_ages",
    )

    # Calculate missing details for each region and income group.
    region_details = shared.calculate_missing_data(
        df_merged, "deaths_from_neuropsychiatric_conditions_per_100_000_people_in__both_sexes_aged_all_ages", "region"
    )
    income_details = shared.calculate_missing_data(
        df_merged,
        "deaths_from_neuropsychiatric_conditions_per_100_000_people_in__both_sexes_aged_all_ages",
        "income_group",
    )

    combined = shared.combine_and_prepare_final_dataset(
        region_details,
        income_details,
        df_merged,
        "deaths_from_neuropsychiatric_conditions_per_100_000_people_in__both_sexes_aged_all_ages",
    )
    tb_garden = Table(combined, short_name="neuropsychiatric_conditions")
    print(tb_garden)

    # Ensure metadata is correctly associated.
    for column in tb_garden.columns:
        tb_garden[column].metadata.origins = tb[
            "deaths_from_neuropsychiatric_conditions_per_100_000_people_in__both_sexes_aged_all_ages"
        ].metadata.origins

    # Save the final dataset.
    ds_garden = create_dataset(dest_dir=dest_dir, tables=[tb_garden], check_variables_metadata=True)
    ds_garden.save()
