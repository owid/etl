"""Load a meadow dataset and create a garden dataset."""

from owid.catalog import Table
from owid.catalog import processing as pr
from shared import add_variable_description_from_producer

from etl.data_helpers.geo import add_regions_to_table, harmonize_countries
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

REGIONS_TO_ADD = [
    "North America",
    "South America",
    "Europe",
    "Africa",
    "Asia",
    "Oceania",
    "Low-income countries",
    "Upper-middle-income countries",
    "Lower-middle-income countries",
    "High-income countries",
    "World",
]


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("laboratories")
    snap = paths.load_snapshot("data_dictionary.csv")
    ds_un_wpp = paths.load_dataset("un_wpp")
    ds_regions = paths.load_dependency("regions")
    ds_income_groups = paths.load_dependency("income_groups")

    ds_pop = ds_un_wpp["population"].reset_index()
    # Load data dictionary from snapshot.
    dd = snap.read()
    # Read table from meadow dataset.
    tb = ds_meadow["laboratories"].reset_index()

    #
    # Process data.
    #
    tb = harmonize_countries(df=tb, countries_file=paths.country_mapping_path)
    tb = tb[["country", "year", "culture", "m_wrd", "m_wrd_tests_performed", "m_wrd_tests_positive"]]
    tb = add_variable_description_from_producer(tb, dd)
    tb = tb.dropna(subset=["culture", "m_wrd"], how="all")
    tb = add_regions_to_table(
        tb=tb,
        ds_regions=ds_regions,
        ds_income_groups=ds_income_groups,
        regions=REGIONS_TO_ADD,
        min_num_values_per_year=1,
    )
    tb = calculate_positive_test_rate(tb)
    tb = add_population_and_rates(tb, ds_pop)
    tb = tb.set_index(["country", "year"], verify_integrity=True)

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=ds_meadow.metadata
    )

    # Save changes in the new garden dataset.
    ds_garden.save()


def add_population_and_rates(tb: Table, ds_pop: Table) -> Table:
    """
    Adding the total population of each country-year to the table and then calculating the rates per million people.

    """
    ds_pop = ds_pop[
        (ds_pop["variant"] == "estimates")
        & (ds_pop["age"] == "all")
        & (ds_pop["sex"] == "all")
        & (ds_pop["metric"] == "population")
    ]
    ds_pop = ds_pop.rename(columns={"location": "country", "value": "population"})
    ds_pop = ds_pop[["country", "year", "population"]]

    tb_pop = pr.merge(tb, ds_pop, on=["country", "year"], how="left")
    tb_pop["culture_rate"] = (tb_pop["culture"] / tb_pop["population"]) * 1000000
    tb_pop["m_wrd_rate"] = (tb_pop["m_wrd"] / tb_pop["population"]) * 1000000
    # Converting to float16 to reduce warnings
    tb_pop[["culture_rate", "m_wrd_rate"]] = tb_pop[["culture_rate", "m_wrd_rate"]].astype("float16")
    tb_pop = tb_pop.drop(columns=["population"])

    return tb_pop


def calculate_positive_test_rate(tb: Table) -> Table:
    """
    Calculating the positive test rate for each country-year.

    We divide the number of positive rapid WHO recommended tests by the total number of tests performed.
    """
    tb["m_wrd_tests_positive_rate"] = (tb["m_wrd_tests_positive"] / tb["m_wrd_tests_performed"]) * 100
    tb["m_wrd_tests_positive_rate"] = tb["m_wrd_tests_positive_rate"].astype(float)
    return tb
