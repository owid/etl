"""Load a meadow dataset and create a garden dataset."""

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Define dependencies, tables and indicators to use
DEPENDENCIES_TABLES_INDICATORS = {
    "wdi": {"table": ["wdi"], "indicators": ["ny_gdp_pcap_pp_kd", "sh_med_phys_zs", "eg_elc_accs_zs"]},
    "un_wpp": {"table": ["life_expectancy"], "indicators": ["life_expectancy__sex_all__age_0__variant_estimates"]},
    "igme": {
        "table": ["igme"],
        "indicators": [
            "obs_value__indicator_under_five_mortality_rate__sex_total__wealth_quintile_total__unit_of_measure_deaths_per_100_live_births"
        ],
    },
    "mortality_database": {
        "table": ["mortality_database"],
        "indicators": [
            "age_standardized_death_rate_per_100_000_standard_population__sex_both_sexes__age_group_all_ages__cause_maternal_conditions__icd10_codes_o00_o99"
        ],
    },
    "who": {"table": ["who"], "indicators": ["wat_imp__residence_total"]},
    "unwto": {"table": ["unwto"], "indicators": ["out_tour_departures_ovn_vis_tourists_per_1000"]},
    "penn_world_table": {"table": ["penn_world_table"], "indicators": ["avh"]},
    "edstats": {"table": ["edstats"], "indicators": ["learning_adjusted_years_of_school", "harmonized_test_scores"]},
    "education_sdgs": {
        "table": ["education_sdgs"],
        "indicators": ["adult_literacy_rate__population_15plus_years__both_sexes__pct__lr_ag15t99"],
    },
    "happiness": {"table": ["happiness"], "indicators": ["cantril_ladder_score"]},
}


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
    # Select only the necessary columns from the tables.
    tb_wdi = tb_wdi[["country", "year", "ny_gdp_pcap_pp_kd", "sh_med_phys_zs", "eg_elc_accs_zs"]]
    print(tb_un_wpp)
    tb_un_wpp = tb_un_wpp[["country", "year", "life_expectancy__sex_all__age_0__variant_estimates"]]
    tb_igme = tb_igme[
        [
            "country",
            "year",
            "obs_value__indicator_under_five_mortality_rate__sex_total__wealth_quintile_total__unit_of_measure_deaths_per_100_live_births",
        ]
    ]
    tb_mortality = tb_mortality[
        [
            "country",
            "year",
            "age_standardized_death_rate_per_100_000_standard_population__sex_both_sexes__age_group_all_ages__cause_maternal_conditions__icd10_codes_o00_o99",
        ]
    ]
    tb_wash = tb_wash[["country", "year", "wat_imp__residence_total"]]
    tb_unwto = tb_unwto[["country", "year", "out_tour_departures_ovn_vis_tourists_per_1000"]]
    tb_pwt = tb_pwt[["country", "year", "avh"]]
    tb_edstats = tb_edstats[["country", "year", "learning_adjusted_years_of_school", "harmonized_test_scores"]]
    tb_unesco = tb_unesco[
        ["country", "year", "adult_literacy_rate__population_15plus_years__both_sexes__pct__lr_ag15t99"]
    ]
    tb_happiness = tb_happiness[["country", "year", "cantril_ladder_score"]]

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
