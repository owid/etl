from owid import catalog
from owid.catalog import Dataset, Table, Variable

from etl.helpers import PathFinder, create_dataset

paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    ds_meadow = paths.load_dataset("vehicles")
    ds_population = paths.load_dataset("population")
    # Create and add table
    table = make_table(ds_population)

    # Set an appropriate index and sort conveniently
    tb = table.format(["country", "year"], sort_columns=True)

    ds = create_dataset(dest_dir, tables=[tb], default_metadata=ds_meadow.metadata)
    ds.save()


def make_table(ds_population: Dataset) -> Table:
    # Load WHO GHO data
    table_gho = load_gho()
    # Load population data
    tb_population = ds_population["population"].reset_index()
    tb_population = tb_population[["country", "year", "population"]]
    # Combine sources
    table = make_combined(table_gho, tb_population)
    return table


def load_gho() -> Table:
    gho = catalog.find_latest(dataset="gho", table="number_of_registered_vehicles").reset_index()
    gho = gho[["country", "year", "number_of_registered_vehicles"]]
    return gho


def make_combined(table_gho: Table, table_population: Table) -> Table:
    # Merge
    table = table_population.merge(table_gho, on=["country", "year"]).set_index(["country", "year"])

    # Calculate new variables
    table = table.assign(
        registered_vehicles_per_thousand=Variable(
            table.number_of_registered_vehicles / table.population * 1000,
            name="registered_vehicles_per_thousand",
        ),
    )
    # Filter columns and drop NA values
    table = table[["registered_vehicles_per_thousand"]].dropna(subset=["registered_vehicles_per_thousand"], how="all")
    return table.reset_index()
