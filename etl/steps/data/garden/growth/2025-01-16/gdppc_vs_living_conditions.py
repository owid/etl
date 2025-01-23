"""
This is to create a dataset that contains the GDP per capita and living conditions of countries.

This data is used to create the static chart "How is life at different levels of GDP per capita?", available in this article: https://ourworldindata.org/global-economic-inequality-introduction

Including this in the ETL facilitates creating new versions of the data in the future.

"""

import owid.catalog.processing as pr
from owid.catalog import Dataset, Table
from structlog import get_logger

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Initialize logger
log = get_logger()

# Define most recent year, so we don't process projections
MOST_RECENT_YEAR = 2024


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_wdi = paths.load_dataset("wdi")
    ds_un_wpp = paths.load_dataset("un_wpp")
    ds_igme = paths.load_dataset("igme")
    ds_mortality = paths.load_dataset("mortality_database")
    ds_wash = paths.load_dataset("who")
    ds_unwto = paths.load_dataset("unwto")
    ds_pwt = paths.load_dataset("penn_world_table")
    ds_edstats = paths.load_dataset("edstats")
    ds_unesco = paths.load_dataset("education_sdgs")
    ds_happiness = paths.load_dataset("happiness")
    ds_population = paths.load_dataset("population")
    ds_regions = paths.load_dataset("regions")

    # Read table from meadow dataset.
    tb_wdi = ds_wdi.read("wdi")
    tb_un_wpp = ds_un_wpp.read("life_expectancy")
    tb_igme = ds_igme.read("igme")
    tb_mortality = ds_mortality.read("mortality_database")
    tb_wash = ds_wash.read("who")
    tb_unwto = ds_unwto.read("unwto")
    tb_pwt = ds_pwt.read("penn_world_table")
    tb_edstats = ds_edstats.read("edstats")
    tb_unesco = ds_unesco.read("education_sdgs")
    tb_happiness = ds_happiness.read("happiness")

    #
    # Process data.
    #
    # Select only the necessary columns and dimensions from the tables.
    # WDI
    tb_wdi = tb_wdi[["country", "year", "ny_gdp_pcap_pp_kd", "sh_med_phys_zs", "eg_elc_accs_zs"]].rename(
        columns={
            "ny_gdp_pcap_pp_kd": "gdp_per_capita",
            "sh_med_phys_zs": "physicians_per_1000_people",
            "eg_elc_accs_zs": "access_to_electricity",
        }
    )

    # UN WPP
    tb_un_wpp = tb_un_wpp[
        (tb_un_wpp["sex"] == "all") & (tb_un_wpp["age"] == 0) & (tb_un_wpp["variant"] == "estimates")
    ].reset_index(drop=True)
    tb_un_wpp = tb_un_wpp[["country", "year", "life_expectancy"]]

    # IGME
    tb_igme = tb_igme[
        (tb_igme["indicator"] == "Under-five mortality rate")
        & (tb_igme["sex"] == "Total")
        & (tb_igme["wealth_quintile"] == "Total")
        & (tb_igme["unit_of_measure"] == "Deaths per 100 live births")
    ].reset_index(drop=True)
    tb_igme = tb_igme[["country", "year", "obs_value"]].rename(columns={"obs_value": "child_mortality_rate"})

    # Mortality Database
    tb_mortality = tb_mortality[
        (tb_mortality["sex"] == "Both sexes")
        & (tb_mortality["age_group"] == "all ages")
        & (tb_mortality["cause"] == "Maternal conditions")
        & (tb_mortality["icd10_codes"] == "O00-O99")
    ].reset_index(drop=True)
    tb_mortality = tb_mortality[
        ["country", "year", "age_standardized_death_rate_per_100_000_standard_population"]
    ].rename(columns={"age_standardized_death_rate_per_100_000_standard_population": "maternal_death_rate"})

    # WHO
    tb_wash = tb_wash[tb_wash["residence"] == "Total"].reset_index(drop=True)
    tb_wash = tb_wash[["country", "year", "wat_imp"]].rename(columns={"wat_imp": "access_to_improve_drinking_water"})

    # UNWTO
    tb_unwto = tb_unwto[["country", "year", "out_tour_departures_ovn_vis_tourists_per_1000"]].rename(
        columns={"out_tour_departures_ovn_vis_tourists_per_1000": "tourist_departures_per_1000_people"}
    )

    # Penn World Table
    tb_pwt = tb_pwt[["country", "year", "avh"]].rename(columns={"avh": "average_working_hours"})

    # Edstats
    tb_edstats = tb_edstats[["country", "year", "learning_adjusted_years_of_school", "harmonized_test_scores"]]

    # UNESCO
    tb_unesco = tb_unesco[
        ["country", "year", "adult_literacy_rate__population_15plus_years__both_sexes__pct__lr_ag15t99"]
    ].rename(
        columns={"adult_literacy_rate__population_15plus_years__both_sexes__pct__lr_ag15t99": "adult_literacy_rate"}
    )

    # Happiness
    tb_happiness = tb_happiness[["country", "year", "cantril_ladder_score"]].rename(
        columns={"cantril_ladder_score": "happiness_score"}
    )

    # Merge all the tables
    tb = pr.multi_merge(
        [tb_wdi, tb_un_wpp, tb_igme, tb_mortality, tb_wash, tb_unwto, tb_pwt, tb_edstats, tb_unesco, tb_happiness],
        on=["country", "year"],
        how="outer",
    )

    tb = geo.add_population_to_table(tb=tb, ds_population=ds_population, warn_on_missing_countries=False)

    tb = select_most_recent_data(tb)

    tb = add_regions_columns(tb, ds_regions)

    tb = tb.format(["country"], short_name="gdppc_vs_living_conditions")

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=ds_wdi.metadata, formats=["csv"]
    )

    # Save changes in the new garden dataset.
    ds_garden.save()


def select_most_recent_data(tb: Table) -> Table:
    """
    Select the most recent data for each indicator and country in the table.
    """

    tb = tb.sort_values(by=["country", "year"], ascending=False).reset_index(drop=True)

    # Define the columns that are indicators (the columns that are not country or year)
    indicators = tb.columns.difference(["country", "year"]).tolist()

    tb_list = []

    for indicator in indicators:
        tb_indicator = tb[["country", "year", indicator]].copy()

        # Drop rows with missing values
        tb_indicator = tb_indicator.dropna(subset=[indicator]).reset_index(drop=True)

        # Define latest year in the dataset
        latest_year = tb_indicator["year"].max()

        if latest_year > MOST_RECENT_YEAR:
            log.warning(
                f"Indicator {indicator} has data for until year {latest_year}, which is higher than {MOST_RECENT_YEAR}. We keep only data until {MOST_RECENT_YEAR}."
            )

            # Drop rows with year higher than MOST_RECENT_YEAR
            tb_indicator = tb_indicator[tb_indicator["year"] <= MOST_RECENT_YEAR].reset_index(drop=True)

        # Calculate latest year again
        latest_year = tb_indicator["year"].max()

        log.info(f"The latest year for indicator {indicator} is {latest_year}.")

        # Select all the rows where the data is at most 10 years old (MOST_RECENT_YEAR - 10)
        tb_indicator = tb_indicator[tb_indicator["year"] >= MOST_RECENT_YEAR - 10].reset_index(drop=True)

        # Drop year column
        tb_indicator = tb_indicator.drop(columns=["year"])

        # For each country, select the row with the latest year
        tb_indicator = tb_indicator.groupby("country").first().reset_index()

        tb_list.append(tb_indicator)

    tb = pr.multi_merge(tb_list, on=["country"], how="outer")

    return tb


def add_regions_columns(tb: Table, ds_regions: Dataset) -> Table:
    """
    Add region columns to the table.
    """

    tb_regions = geo.create_table_of_regions_and_subregions(ds_regions=ds_regions)

    # Explode the regions table to have one row per country
    tb_regions = tb_regions.explode("members").reset_index(drop=False)

    # Select OWID regions
    tb_regions = tb_regions[
        tb_regions["region"].isin(["North America", "South America", "Europe", "Africa", "Asia", "Oceania"])
    ].reset_index(drop=True)

    # Merge the regions table with the table
    tb = pr.merge(
        tb,
        tb_regions,
        left_on="country",
        right_on="members",
        how="left",
    )

    # Delete the members column
    tb = tb.drop(columns=["members"])

    # Keep only the rows where region is not missing
    tb = tb.dropna(subset=["region"]).reset_index(drop=True)

    return tb
