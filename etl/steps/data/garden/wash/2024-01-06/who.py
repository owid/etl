"""Load a meadow dataset and create a garden dataset."""


from owid.catalog import Table
from owid.catalog import processing as pr
from structlog import get_logger

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)
log = get_logger()


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("who")
    # Add population
    ds_population = paths.load_dependency("population")
    tb = ds_meadow["who"].reset_index()
    tb = tb.rename(columns={"name": "country"})
    tb = geo.harmonize_countries(df=tb, countries_file=paths.country_mapping_path)
    # The population is given as 'population (thousands)
    tb["pop"] = tb["pop"].astype(float).multiply(1000)
    tb = add_population_to_regions(tb, ds_population)
    tb = calculate_population_with_each_category(tb)
    tb = calculate_population_without_service(tb)

    tb = tb.drop(columns=["pop"])
    tb = tb.set_index(["country", "year", "residence"], verify_integrity=True)
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=ds_meadow.metadata
    )

    # Save changes in the new garden dataset.
    ds_garden.save()


def calculate_population_with_each_category(tb: Table) -> Table:
    """
    Calculate the population living with each category and also by urban/rural.

    Multiply each column by the 'pop' column and append '_pop' to the column name.

    """
    columns = tb.columns.drop(["country", "year", "pop", "residence"])

    for col in columns:
        tb[f"{col}_pop"] = (tb[col] / 100) * tb["pop"]

    return tb


def calculate_population_without_service(tb: Table) -> Table:
    """
    Calculate the population _without_ given services for a selection of services, that we show in charts.

    """
    # * wat_basal
    # * wat_imp
    # * wat_sm
    # * san_imp
    # * san_sm
    # * hyg_bas

    without_cols = ["wat_basal", "wat_imp", "wat_sm", "san_imp", "san_sm", "hyg_bas"]

    for col in without_cols:
        tb[f"{col}_without"] = 100 - tb[col]
        tb[f"{col}_pop_without"] = (tb[f"{col}_without"] / 100) * tb["pop"]

    return tb


def add_population_to_regions(tb: Table, ds_population: Table) -> Table:
    tb_to_add_pop = tb[["country", "year", "residence"]][(tb["pop"].isna()) & (tb["residence"] == "Total")]

    tb_to_add_pop = geo.add_population_to_table(
        tb_to_add_pop, ds_population=ds_population, warn_on_missing_countries=False
    )

    tb_cols = tb_to_add_pop.columns.drop(["population"]).to_list()
    tb = pr.merge(tb, tb_to_add_pop, on=tb_cols, how="left")
    tb["pop"] = tb["pop"].combine_first(tb["population"])

    tb = tb.drop(columns=["population"])

    return tb
