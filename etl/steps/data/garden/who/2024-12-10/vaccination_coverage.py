"""Load a meadow dataset and create a garden dataset."""

from owid.catalog import Dataset, Table
from owid.catalog import processing as pr

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

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


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("vaccination_coverage")
    ds_population = paths.load_dataset("un_wpp")
    # Read table from meadow dataset.
    tb = ds_meadow.read("vaccination_coverage")

    #
    # Process data.
    #
    tb = geo.harmonize_countries(
        df=tb, countries_file=paths.country_mapping_path, excluded_countries_file=paths.excluded_countries_path
    )
    # Keep only data from WUENIC
    tb = use_only_wuenic_data(tb)
    tb = clean_data(tb)
    # Add denominator column
    tb = tb.assign(denominator=tb["antigen"].map(DENOMINATOR))
    # Calculate the number of one-year-olds vaccinated for each antigen.
    tb_one_year_olds = calculate_one_year_olds_vaccinated(tb, ds_population)
    tb_newborns = calculate_newborns_vaccinated(tb, ds_population)

    tb = tb.drop(columns=["denominator"])
    tb = tb.format(["country", "year", "antigen"], short_name="vaccination_coverage")
    tb_one_year_olds = tb_one_year_olds.format(["country", "year", "antigen"], short_name="number_of_one_year_olds")
    tb_newborns = tb_newborns.format(["country", "year", "antigen"], short_name="number_of_newborns")
    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir,
        tables=[tb, tb_one_year_olds, tb_newborns],
        check_variables_metadata=True,
        default_metadata=ds_meadow.metadata,
    )

    # Save changes in the new garden dataset.
    ds_garden.save()


def get_population_of_age_group(ds_population: Dataset, age=str) -> Table:
    tb_pop = ds_population.read("population", reset_metadata="keep_origins")
    tb_pop = tb_pop[(tb_pop["age"] == age) & (tb_pop["variant"] == "estimates") & (tb_pop["sex"] == "all")]
    tb_pop = tb_pop[["country", "year", "sex", "age", "variant", "population"]]
    return tb_pop


def calculate_one_year_olds_vaccinated(tb: Table, ds_population: Dataset) -> Table:
    """
    Calculate the number of one-year-olds vaccinated for each antigen.
    """

    tb = tb[tb["denominator"] == "Surviving infants"]
    tb_pop = get_population_of_age_group(ds_population=ds_population, age="1")

    tb = pr.merge(tb, tb_pop, on=["country", "year"], how="left")
    tb = tb.assign(
        vaccinated_one_year_olds=tb["coverage"] / 100 * tb["population"],
        unvaccinated_one_year_olds=(100 - tb["coverage"]) / 100 * tb["population"],
    )
    tb = tb[["country", "year", "antigen", "vaccinated_one_year_olds", "unvaccinated_one_year_olds"]]
    return tb


def calculate_newborns_vaccinated(tb: Table, ds_population: Dataset) -> Table:
    """
    Calculate the number of one-year-olds vaccinated for each antigen.
    """

    tb = tb[tb["denominator"] == "Live births"]
    tb_pop = get_population_of_age_group(ds_population=ds_population, age="0")

    tb = pr.merge(tb, tb_pop, on=["country", "year"], how="left")
    tb = tb.assign(
        vaccinated_newborns=tb["coverage"] / 100 * tb["population"],
        unvaccinated_newborns=(100 - tb["coverage"]) / 100 * tb["population"],
    )
    tb = tb[["country", "year", "antigen", "vaccinated_newborns", "unvaccinated_newborns"]]
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
