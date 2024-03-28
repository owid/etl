"""Load a meadow dataset and create a garden dataset."""

from owid.catalog import Table

from etl.helpers import PathFinder, create_dataset

from . import shared

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    # Load relevant datasets.
    ds_data = paths.load_dataset("self_inflicted_injuries")
    ds_regions = paths.load_dataset("regions")
    ds_income_groups = paths.load_dataset("income_groups")
    ds_population = paths.load_dataset("population")
    tb_population = ds_population["population"].reset_index()
    tb = ds_data["self_inflicted_injuries"].reset_index()

    tb_data = prepare_data(tb)

    df_merged = shared.map_countries_and_merge_data(
        tb_data, ds_regions, ds_income_groups, tb_population, "death_rate_per_100_000_population"
    )

    # Calculate missing details for each region and income group.
    region_details = shared.calculate_missing_data(df_merged, "death_rate_per_100_000_population", "region")
    income_details = shared.calculate_missing_data(df_merged, "death_rate_per_100_000_population", "income_group")

    combined = shared.combine_and_prepare_final_dataset(
        region_details, income_details, df_merged, "death_rate_per_100_000_population"
    )
    tb_garden = Table(combined, short_name="who_md_suicides")

    # Ensure metadata is correctly associated.
    for column in tb_garden.columns:
        tb_garden[column].metadata.origins = tb["death_rate_per_100_000_population"].metadata.origins

    # Save the final dataset.
    ds_garden = create_dataset(dest_dir=dest_dir, tables=[tb_garden], check_variables_metadata=True)
    ds_garden.save()


def prepare_data(tb):
    """
    Prepare the data to calculate missing data for. Here, we are interested in the death rate per 100,000 population.
    """
    tb_data = tb.filter(items=["country", "year", "sex", "age_group", "death_rate_per_100_000_population"])
    tb_data = tb_data.pivot(
        index=["country", "year"], columns=["sex", "age_group"], values="death_rate_per_100_000_population"
    )
    tb_data = tb_data.reset_index()
    tb_data.columns = [" ".join(col).strip() for col in tb_data.columns.values if col not in ["country", "year"]]

    tb_data = tb_data[["country", "year", "Both sexes all ages"]]
    tb_data = tb_data.rename(columns={"Both sexes all ages": "death_rate_per_100_000_population"})
    tb_data = tb_data.dropna(subset=["death_rate_per_100_000_population"])

    return tb_data
