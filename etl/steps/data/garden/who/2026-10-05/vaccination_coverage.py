"""Load a meadow dataset and create a garden dataset."""

from owid.catalog import Dataset, Table
from owid.catalog import processing as pr

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Denominator for each antigen based on https://worldhealthorg.shinyapps.io/wuenic-trends/
DENOMINATOR = {
    "BCG": "Live births",  # live births
    "DTPCV1": "Surviving infants",  # the national annual number of infants surviving their first year of life
    "DTPCV3": "Surviving infants",  # the national annual number of infants surviving their first year of life
    "MCV1": "Surviving infants",  # the national annual number of infants surviving their first year of life
    "RCV1": "Surviving infants",  # the national annual number of infants surviving their first year of life
    "HEPB3": "Surviving infants",  # the national annual number of infants surviving their first year of life
    "HIB3": "Surviving infants",  # the national annual number of infants surviving their first year of life
    "HEPB_BD": "Live births",  # live births
    "MCV2": "Children",
    "ROTAC": "Surviving infants",  # the national annual number of infants surviving their first year of life
    # NOTE: PCV3 was renamed to PCVC in the WHO 2025 Revision (2026-10-05 update), same scope/history
    "PCVC": "Surviving infants",  # the national annual number of infants surviving their first year of life
    "IPV1": "Surviving infants",  # the national annual number of infants surviving their first year of life
    # NOTE: IPV2 was replaced by IPVC in the WHO 2025 Revision (2026-10-05 update) -- IPVC is the
    # new headline "complete primary series" measure, produced for all countries (not just the few
    # that reported IPV2). See https://www.who.int/docs/default-source/immunization/immunization-coverage/wuenic_notes.pdf
    "IPVC": "Surviving infants",  # the national annual number of infants surviving their first year of life
    "YFV": "Surviving infants",  # the national annual number of infants surviving their first year of life
    # NOTE: MENA_C was renamed to MEN_A_CONJ in the WHO 2025 Revision (2026-10-05 update), same
    "MEN_A_CONJ": "Surviving infants",  # the national annual number of infants surviving their first year of life
    # NOTE: POL3 discontinued by WHO -- see add_historical_polio() for detail.
    "POL3": "Surviving infants",  # the national annual number of infants surviving their first year of life
    # NOTE: MCV2X2 ("measles, second dose, two-year-olds") was removed (2026-10-05 update) -- it has
    # had no rows in the raw source in any version we have on disk, including the very first (2022)
    # version of this step, which explicitly dropped it (`df[df["antigen"] != "MCV2X2"]`). Confirmed
    # dead, not a newly-discontinued antigen.
}

# If the vaccine universally recommended in all countries by WHO and UNICEF, set to True.
# If not, set to False.
# See this table for details: https://www.who.int/publications/m/item/table1-summary-of-who-position-papers-recommendations-for-routine-immunization
# Note BCG is quite nuanced
UNIVERSAL = {
    "BCG": False,  # Only universally recommended in high TB incidence countries, only at risk groups in other countries
    "DTPCV1": True,
    "DTPCV3": True,
    "MCV1": True,
    "RCV1": True,  # All countries that have not yet introduced RCV should plan to do so.
    "HEPB3": True,
    "HIB3": True,
    "HEPB_BD": True,  # Hepatitis B birth dose is recommended in
    "MCV2": True,  # Measles second dose is recommended in all countries
    "ROTAC": True,  # Rotavirus vaccine is recommended in all countries
    "PCVC": True,  # Pneumococcal conjugate vaccine is
    "IPV1": True,  # Inactivated polio vaccine is recommended in all countries
    "IPVC": True,  # Inactivated polio vaccine is recommended in all countries
    "YFV": False,  # Yellow fever vaccine is recommended in countries with risk of yellow fever transmission
    "MEN_A_CONJ": False,
    "POL3": True,  # historical/discontinued -- see add_historical_polio() for detail.
}

REGIONS_TO_ADD = [
    "North America",
    "South America",
    "Europe",
    "Africa",
    "Asia",
    "Oceania",
]


def run() -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("vaccination_coverage", channel="meadow")
    ds_population = paths.load_dataset("un_wpp")
    # Read table from meadow dataset.
    tb = ds_meadow.read("vaccination_coverage")
    #
    # Process data.
    #
    # Keep only data from WUENIC (the estimates by World Health Organization and UNICEF).
    tb = use_only_wuenic_data(tb)
    tb = paths.regions.harmonize_names(tb)
    tb = clean_data(tb)
    # Add denominator column
    tb = tb.assign(denominator=tb["antigen"].map(DENOMINATOR))
    # Calculate the number of one-year-olds vaccinated for each antigen.
    tb_one_year_olds = calculate_one_year_olds_vaccinated(tb, ds_population)
    tb_newborns = calculate_newborns_vaccinated(tb, ds_population)
    tb_number = pr.concat([tb_one_year_olds, tb_newborns], short_name="numbers")

    tb = pr.merge(tb, tb_number, on=["country", "year", "antigen"], how="left")

    tb = paths.regions.add_aggregates(
        tb,
        index_columns=["country", "year", "antigen"],
        aggregations={"vaccinated": "sum", "unvaccinated": "sum"},
        regions=REGIONS_TO_ADD,
        min_num_values_per_year=1,
        frac_allowed_nans_per_year=0.3,  # Allow up to 30% missing values per year for regions
    )
    tb = calculate_coverage_for_regions_for_age_group(
        tb=tb,
        ds_population=ds_population,
        age_group="Surviving infants",
        age_group_number="1",
    )
    tb = calculate_coverage_for_regions_for_age_group(
        tb=tb,
        ds_population=ds_population,
        age_group="Live births",
        age_group_number="0",
    )
    tb = add_historical_polio(tb)
    tb = tb.format(["country", "year", "antigen"], short_name="vaccination_coverage")
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = paths.create_dataset(
        tables=[tb],
        check_variables_metadata=True,
        default_metadata=ds_meadow.metadata,
    )

    # Save changes in the new garden dataset.
    ds_garden.save()


def add_historical_polio(tb: Table) -> Table:
    """
    Add in the historical "POL3" (3rd dose of polio vaccine) series.

    POL3 was discontinued by WHO in the 2025 Revision (2026-10-05 update), partly replaced with IPVC. We use the previous garden version to present it as a separate, explicitly historical/discontinued antigen ("Polio (historical series)").
    WHO's own guidance: "the combined Polio3 indicator has been discontinued. For historical trend analyses, DTP3 may be used as a proxy."
    """

    ds_garden_old = paths.load_dataset("vaccination_coverage", channel="garden", version="2025-07-15")
    tb_old = ds_garden_old.read("vaccination_coverage")
    tb_pol3 = tb_old[tb_old["antigen"] == "POL3"]
    assert len(tb_pol3) > 0, "No POL3 rows found in the previous garden version -- can't freeze them in."
    assert tb_pol3["year"].max() == 2024, "Expected the frozen POL3 series to end in 2024 -- re-check."
    tb = pr.concat([tb, tb_pol3], short_name="vaccination_coverage")
    return tb


def calculate_coverage_for_regions_for_age_group(
    tb: Table, ds_population: Dataset, age_group: str, age_group_number: str
) -> Table:
    tb = tb.assign(denominator=tb["antigen"].map(DENOMINATOR))
    msk = (tb["denominator"] == age_group) & (tb["country"].isin(REGIONS_TO_ADD))
    tb_regions = tb[msk]
    tb_no_regions = tb[~msk]
    tb_pop = get_population_of_age_group(ds_population=ds_population, age=age_group_number)
    tb_pop = tb_pop.drop(columns=["sex", "age", "variant"])
    # Add regional aggregates for population
    tb_pop = paths.regions.add_aggregates(
        tb_pop,
        index_columns=["country", "year"],
        aggregations={"population": "sum"},
        regions=REGIONS_TO_ADD,
        min_num_values_per_year=1,
        frac_allowed_nans_per_year=0.3,
    )  # Allow up to 30% missing values per year for regions
    tb_regions = pr.merge(tb_regions, tb_pop, on=["country", "year"], how="left")
    tb_regions["coverage"] = (tb_regions["vaccinated"] / tb_regions["population"]) * 100
    # Drop age-specific population column

    assert tb_regions["coverage"].dropna().max() <= 100, "Coverage cannot be more than 100%."
    tb = pr.concat([tb_no_regions, tb_regions], short_name="vaccination_coverage")
    tb = tb.drop(columns=["denominator", "population"])
    return tb


def get_population_of_age_group(ds_population: Dataset, age: str) -> Table:
    tb_pop = ds_population.read("population", reset_metadata="keep_origins")
    tb_pop = tb_pop[(tb_pop["age"] == age) & (tb_pop["variant"] == "estimates") & (tb_pop["sex"] == "all")]
    tb_pop = tb_pop[["country", "year", "sex", "age", "variant", "population"]]
    return tb_pop


def calculate_one_year_olds_vaccinated(tb: Table, ds_population: Dataset) -> Table:
    """
    Calculate the number of one-year-olds vaccinated for each antigen.
    """

    tb = tb[(tb["denominator"] == "Surviving infants")]
    # Filter out vaccines that are not universally recommended by WHO and UNICEF
    tb = tb[tb["antigen"].map(UNIVERSAL)]
    tb_pop = get_population_of_age_group(ds_population=ds_population, age="1")

    tb = pr.merge(tb, tb_pop, on=["country", "year"], how="left")
    tb = tb.assign(
        vaccinated=tb["coverage"] / 100 * tb["population"],
        unvaccinated=(100 - tb["coverage"]) / 100 * tb["population"],
    )
    tb = tb[["country", "year", "antigen", "vaccinated", "unvaccinated"]]
    return tb


def calculate_newborns_vaccinated(tb: Table, ds_population: Dataset) -> Table:
    """
    Calculate the number of newborns vaccinated for each antigen.
    """

    tb = tb[tb["denominator"] == "Live births"]
    # Only calculate for vaccines that are universally recommended by WHO and UNICEF
    tb = tb[tb["antigen"].map(UNIVERSAL)]
    tb_pop = get_population_of_age_group(ds_population=ds_population, age="0")

    tb = pr.merge(tb, tb_pop, on=["country", "year"], how="left")
    tb = tb.assign(
        vaccinated=tb["coverage"] / 100 * tb["population"],
        unvaccinated=(100 - tb["coverage"]) / 100 * tb["population"],
    )
    tb = tb[["country", "year", "antigen", "vaccinated", "unvaccinated"]]
    return tb


def use_only_wuenic_data(tb: Table) -> Table:
    """
    Keep only data that is from WUENIC - estimated by World Health Organization and UNICEF.
    """
    assert "WUENIC" in tb["coverage_category"].unique(), "No data from WUENIC in the table."
    tb = tb[tb["coverage_category"] == "WUENIC"]
    tb = tb.drop(columns=["coverage_category"])
    return tb


def clean_data(tb: Table) -> Table:
    """
    Clean up the data:
    - Remove rows where coverage is NA
    - Remove unneeded columns
    """

    tb = tb[tb["coverage"].notna()]
    tb = tb.drop(
        columns=["group", "code", "antigen_description", "coverage_category_description", "target_number", "doses"]
    )

    return tb
