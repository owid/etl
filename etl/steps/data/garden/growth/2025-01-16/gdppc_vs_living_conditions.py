"""Load a meadow dataset and create a garden dataset."""

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


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
        (tb_un_wpp["sex"] == "all") & (tb_un_wpp["age"] == "0") & (tb_un_wpp["variant"] == "estimates")
    ].reset_index(drop=True)
    tb_un_wpp = tb_un_wpp[["country", "year", "life_expectancy"]]

    # IGME
    tb_igme = tb_igme[
        (tb_igme["obs_value"] == "Under five mortality rate")
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

    tb = geo.harmonize_countries(
        df=tb, countries_file=paths.country_mapping_path, excluded_countries_file=paths.excluded_countries_path
    )
    tb = tb.format(["country", "year"])

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=ds_meadow.metadata
    )

    # Save changes in the new garden dataset.
    ds_garden.save()
