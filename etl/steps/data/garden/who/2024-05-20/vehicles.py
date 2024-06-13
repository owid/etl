from pathlib import Path

from owid import catalog
from owid.catalog import Dataset, Table, Variable

CURRENT_DIR = Path(__file__).parent
METADATA_PATH = CURRENT_DIR / "vehicles.meta.yml"
SHORT_NAME = "vehicles"
NAMESPACE = "who"
VERSION = "2024-05-20"


def run(dest_dir: str) -> None:
    ds = Dataset.create_empty(dest_dir)

    # Create and add table
    table = make_table()

    # Set an appropriate index and sort conveniently
    table = table.set_index(["country", "year"], verify_integrity=True).sort_index().sort_index(axis=1)

    # Add table to dataset
    ds.add(table)

    # Add metadata to dataset.
    ds.metadata.update_from_yaml(METADATA_PATH, if_source_exists="replace")
    ds.metadata.short_name = SHORT_NAME
    ds.metadata.namespace = NAMESPACE
    ds.metadata.version = VERSION

    # Save
    ds.save()


def make_table() -> Table:
    # Load WHO GHO data
    table_gho = load_gho()
    # Load population data
    table_population = load_key_indicators()
    # Combine sources
    table = make_combined(table_gho, table_population)
    table.update_metadata_from_yaml(METADATA_PATH, "vehicles")
    return table


def load_gho() -> Table:
    gho = catalog.find_latest(dataset="gho", table="number_of_registered_vehicles").reset_index()
    gho = gho[["country", "year", "number_of_registered_vehicles"]]
    return gho


def load_key_indicators() -> Table:
    population = catalog.find_latest(dataset="population").reset_index()
    population = population[["country", "year", "population"]]
    return population


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
