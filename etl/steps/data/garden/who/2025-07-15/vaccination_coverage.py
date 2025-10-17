"""Load a meadow dataset and create a garden dataset."""

from owid.catalog import Dataset, Table
from owid.catalog import processing as pr

from etl.data_helpers import geo
from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Denominator for each antigen based on https://worldhealthorg.shinyapps.io/wuenic-trends/
DENOMINATOR = {
    "BCG": "Live births",  # live births
    "DTPCV1": "Surviving infants",  # the national annual number of infants surviving their first year of life
    "DTPCV3": "Surviving infants",  # the national annual number of infants surviving their first year of life
    "MCV1": "Surviving infants",  # the national annual number of infants surviving their first year of life
    "POL3": "Surviving infants",  # the national annual number of infants surviving their first year of life
    "RCV1": "Surviving infants",  # the national annual number of infants surviving their first year of life
    "HEPB3": "Surviving infants",  # the national annual number of infants surviving their first year of life
    "HIB3": "Surviving infants",  # the national annual number of infants surviving their first year of life
    "HEPB_BD": "Live births",  # live births
    "MCV2": "Children",
    "ROTAC": "Surviving infants",  # the national annual number of infants surviving their first year of life
    "PCV3": "Surviving infants",  # the national annual number of infants surviving their first year of life
    "IPV1": "Surviving infants",  # the national annual number of infants surviving their first year of life
    "IPV2": "Surviving infants",  # the national annual number of infants surviving their first year of life
    "YFV": "Surviving infants",  # the national annual number of infants surviving their first year of life
    "MCV2X2": "Two-year-olds",
    "MENA_C": "Surviving infants",  # the national annual number of infants surviving their first year of life
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
    "POL3": True,
    "RCV1": True,  # All countries that have not yet introduced RCV should plan to do so.
    "HEPB3": True,
    "HIB3": True,
    "HEPB_BD": True,  # Hepatitis B birth dose is recommended in
    "MCV2": True,  # Measles second dose is recommended in all countries
    "ROTAC": True,  # Rotavirus vaccine is recommended in all countries
    "PCV3": True,  # Pneumococcal conjugate vaccine is
    "IPV1": True,  # Inactivated polio vaccine is recommended in all countries
    "IPV2": True,  # Inactivated polio vaccine is recommended in all
    "YFV": False,  # Yellow fever vaccine is recommended in countries with risk of yellow fever transmission
    "MCV2X2": True,  # Measles second dose is recommended
    "MENA_C": False,
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
    ds_meadow = paths.load_dataset("vaccination_coverage")
    ds_population = paths.load_dataset("un_wpp")
    # Load regions dataset.
    ds_regions = paths.load_dataset("regions")
    # Load income groups dataset.
    ds_income_groups = paths.load_dataset("income_groups")
    # Read table from meadow dataset.
    tb = ds_meadow.read("vaccination_coverage")
    #
    # Process data.
    #
    # Keep only data from WUENIC (the estimates by World Health Organization and UNICEF).
    tb = use_only_wuenic_data(tb)
    tb = geo.harmonize_countries(
        df=tb, countries_file=paths.country_mapping_path, excluded_countries_file=paths.excluded_countries_path
    )
    tb = clean_data(tb)
    # Add denominator column
    tb = tb.assign(denominator=tb["antigen"].map(DENOMINATOR))
    # Calculate the number of one-year-olds vaccinated for each antigen.
    tb_one_year_olds = calculate_one_year_olds_vaccinated(tb, ds_population)
    tb_newborns = calculate_newborns_vaccinated(tb, ds_population)
    tb_number = pr.concat([tb_one_year_olds, tb_newborns], short_name="numbers")

    tb = pr.merge(tb, tb_number, on=["country", "year", "antigen"], how="left")

    tb = geo.add_regions_to_table(
        tb=tb,
        index_columns=["country", "year", "antigen"],
        aggregations={"vaccinated": "sum", "unvaccinated": "sum"},
        ds_regions=ds_regions,
        ds_income_groups=ds_income_groups,
        regions=REGIONS_TO_ADD,
        min_num_values_per_year=1,
        frac_allowed_nans_per_year=0.3,  # Allow up to 30% missing values per year for regions
    )
    tb = calculate_coverage_for_regions_for_age_group(
        tb=tb,
        ds_population=ds_population,
        ds_regions=ds_regions,
        age_group="Surviving infants",
        age_group_number="1",
    )
    tb = calculate_coverage_for_regions_for_age_group(
        tb=tb,
        ds_population=ds_population,
        ds_regions=ds_regions,
        age_group="Live births",
        age_group_number="0",
    )
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


def calculate_coverage_for_regions_for_age_group(
    tb: Table, ds_population: Dataset, ds_regions: Dataset, age_group: str, age_group_number: str
) -> Table:
    tb = tb.assign(denominator=tb["antigen"].map(DENOMINATOR))
    msk = (tb["denominator"] == age_group) & (tb["country"].isin(REGIONS_TO_ADD))
    tb_regions = tb[msk]
    tb_no_regions = tb[~msk]
    tb_pop = get_population_of_age_group(ds_population=ds_population, age=age_group_number)
    tb_pop = tb_pop.drop(columns=["sex", "age", "variant"])
    # Add regional aggregates for population
    tb_pop = geo.add_regions_to_table(
        tb=tb_pop,
        index_columns=["country", "year"],
        aggregations={"population": "sum"},
        ds_regions=ds_regions,
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
