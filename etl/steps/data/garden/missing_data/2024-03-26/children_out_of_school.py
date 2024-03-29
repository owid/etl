"""Load a meadow dataset and create a garden dataset."""

from owid.catalog import Table

from etl.helpers import PathFinder, create_dataset

from . import shared

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    # Load relevant datasets.
    ds_data = paths.load_dataset("gender_statistics")
    ds_regions = paths.load_dataset("regions")
    ds_income_groups = paths.load_dataset("income_groups")
    ds_population = paths.load_dataset("population")
    tb_population = ds_population["population"].reset_index()
    tb = ds_data["gender_statistics"].reset_index()

    tb_data = tb.filter(items=["country", "year"] + [col for col in tb if col.startswith("se_prm_uner")])
    tb_data = tb_data.dropna(subset=["se_prm_uner"])

    df_merged = shared.map_countries_and_merge_data(
        tb_data,
        ds_regions,
        ds_income_groups,
        tb_population,
        "se_prm_uner",
    )

    # Calculate missing details for each region and income group.
    region_details = shared.calculate_missing_data(df_merged, "se_prm_uner", "region")
    income_details = shared.calculate_missing_data(
        df_merged,
        "se_prm_uner",
        "income_group",
    )

    global_details = shared.calculate_missing_data(
        df_merged,
        "se_prm_uner",
        "global",
    )

    combined = shared.combine_and_prepare_final_dataset(
        region_details,
        income_details,
        global_details,
        df_merged,
        "se_prm_uner",
    )
    tb_garden = Table(combined, short_name="children_out_of_school")

    # Ensure metadata is correctly associated.
    for column in tb_garden.columns:
        tb_garden[column].metadata.origins = tb["se_prm_uner"].metadata.origins

    # Save the final dataset.
    ds_garden = create_dataset(dest_dir=dest_dir, tables=[tb_garden], check_variables_metadata=True)
    ds_garden.save()
