from pathlib import Path

from owid import catalog
from owid.catalog import Dataset, Table, Variable

CURRENT_DIR = Path(__file__).parent
METADATA_PATH = CURRENT_DIR / "patents_articles.meta.yml"


def run(dest_dir: str) -> None:
    ds = Dataset.create_empty(dest_dir)
    ds.metadata.update_from_yaml(METADATA_PATH, if_source_exists="replace")

    # Create and add patents table
    table = make_table()

    # Set an appropriate index and sort conveniently
    table = table.set_index(["country", "year"], verify_integrity=True).sort_index().sort_index(axis=1)

    # Add table to dataset
    ds.add(table)

    # Save
    ds.save()


def make_table() -> Table:
    # Load internet data
    table_wdi = load_wdi()
    # Load population data
    table_population = load_key_indicators()
    # Combine sources
    table = make_combined(table_wdi, table_population)
    table.update_metadata_from_yaml(METADATA_PATH, "patents_articles")
    return table


def load_wdi() -> Table:
    wdi = catalog.find_latest(dataset="wdi").reset_index()
    wdi = wdi[["country", "year", "ip_pat_resd", "ip_jrn_artc_sc"]]
    return wdi


def load_key_indicators() -> Table:
    population = catalog.find_latest(dataset="population").reset_index()
    population = population[["country", "year", "population"]]
    return population


def make_combined(table_wdi: Table, table_population: Table) -> Table:
    # Merge
    table = (
        table_population.reset_index()
        .merge(table_wdi.reset_index(), on=["country", "year"])
        .set_index(["country", "year"])
    )

    # Calculate new variables
    patents_per_million = table.ip_pat_resd / table.population * 1000000
    articles_per_million = table.ip_jrn_artc_sc / table.population * 1000000

    # Add to table
    table = table.assign(
        patents_per_million=Variable(patents_per_million, name="patents_per_million"),
        articles_per_million=Variable(articles_per_million, name="articles_per_million"),
    )
    # Filter columns and drop NA values
    table = table[["patents_per_million", "articles_per_million"]].dropna(
        subset=["patents_per_million", "articles_per_million"], how="all"
    )
    return table.reset_index()
