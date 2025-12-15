"""Load a meadow dataset and create a garden dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


INDICATOR_SHORT_NAME_MAPPING = {
    "epi": {
        "People living with HIV": "plwh",
        "New HIV Infections": "new_infections",
        "AIDS-related deaths": "aids_deaths",
        "HIV Incidence per 1000 population": "hiv_incidence",
        "HIV Prevalence": "hiv_prevalence",
        "AIDS mortality per 1000 population": "aids_mortality_1000_pop",
        "Coverage of people living with HIV receiving ART": "percent_plwh",
        "Percent of people who know their status who are on ART": "percent_know_status_on_art",
        "Percent of people living with HIV who know their status": "plhiv_knowledge_of_status",
        "Deaths among people living with HIV (all causes)": "deaths_plhiv_all_causes",
        "AIDS Orphans": "aids_orphans",
        "People receiving antiretroviral therapy": "people_receiving_art",
        "Percent of people on ART who achieve viral suppression": "percent_art_vl_suppressed",
        "Percent of people living with HIV who have suppressed viral loads": "viral_load_suppression",
        "Incidence-to-prevalence ratio": "ipr",
        "HIV-exposed children who are uninfected": "children_exposed_uninfected",
        "New HIV Infections averted due to PMTCT": "infections_averted_pmtct",
        "People living with HIV who know their status": "number_plhiv_knowledge_of_status",
        "Deaths averted due to ART": "deaths_averted_art",
        "Incidence-to-mortality ratio": "imr",
        "People living with HIV who have suppressed viral loads": "number_viral_suppression",
        "Pregnant women needing ARV for preventing MTCT": "mothers_needing_arv_for_pmtct",
        "Coverage of pregnant women who receive ARV for preventing MTCT": "coverage_mothers_receiving_arv",
        "People with unsuppressed viral load": "people_unsupp_vl",
        "Mother-to-child transmission rate": "mtct_rate",
        "Pregnant women who received ARV for preventing MTCT": "mothers_receiving_arv_for_pmtct",
        ##
        "Did not receive ART during breastfeeding; child infected during breastfeeding": "art_none_bf",
        "Did not receive ART during pregnancy; child infected during pregnancy": "art_none_preg_child_inf_preg",
        "Mother dropped off ART during breastfeeding; child infected during breastfeeding": "art_drop_bf_child_inf",
        "Mother dropped off ART during pregnancy; child infected during pregnancy": "art_drop_preg_child_inf",
        "Mother infected during breastfeeding; child infected during breastfeeding": "mother_child_inf_bf",
        "Mother infected during pregnancy; child infected during pregnancy": "mother_child_inf_preg",
        ##
        "Started ART before the pregnancy; child infected during breastfeeding": "art_start_before_preg_child_inf_bf",
        "Started ART before the pregnancy; child infected during pregnancy": "art_start_before_preg_child_inf_preg",
        "Started ART during in pregnancy; child infected during breastfeeding": "art_start_during_preg_child_inf_bf",
        "Started ART during in pregnancy; child infected during pregnancy": "art_start_child_inf_during_preg",
        "Started ART late in pregnancy; child infected during breastfeeding": "art_start_late_preg_child_inf_bf",
        "Started ART late in pregnancy; child infected during pregnancy": "art_start_late_preg_child_inf_preg",
        ##
        "Percent change in AIDS-related deaths since 2010": "pct_change_aids_deaths_2010",
        "Percent change in new HIV infections since 2010": "pct_change_new_infections_2010",
    },
    "gam": {},  # TODO
    # "kpa": {},
    # "ncpi": {},
}


"""
TODO:
- Review NCPI: there are too few data points
- Normalize names for GAM
- Explore potential useful indicators in KPA/NCPI (check for year-country coverage)
    tb_kpa["country_year"] = tb_kpa["country"] + (tb_kpa["year"]).astype("string")
    results = tb_kpa.groupby("indicator").agg({
        "year": "nunique",
        "country": "nunique",
        "country_year": "nunique"
    })
    results.sort_values("country_year", ascending=False).head(30)

    KPA: Seems interesting, but convoluted: it includes data pre 2020 and non-country data (regional).
    NCPI: unclear
- Normalize dimensions

"""


def run() -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("unaids")

    ###############################
    # EPI data
    ###############################
    tb_epi = ds_meadow.read("epi")  ## 1,683,598 rows

    ###############################
    # GAM data
    ###############################
    tb_gam = ds_meadow.read("gam")  ## 1,683,598 rows

    # ###############################
    # # KPA data
    # ###############################
    # tb_kpa = ds_meadow.read("kpa")  ## 49,971 rows

    # ###############################
    # # NCPI data
    # ###############################
    # tb_ncpi = ds_meadow.read(name="ncpi")  ## 7,308 rows

    #
    # Process data.
    #
    # Harmonize country names.
    tb = paths.regions.harmonize_names(tb=tb)

    # Improve table format.
    tb = tb.format(["country", "year"])

    #
    # Save outputs.
    #
    # Initialize a new garden dataset.
    ds_garden = paths.create_dataset(tables=[tb], default_metadata=ds_meadow.metadata)

    # Save garden dataset.
    ds_garden.save()
