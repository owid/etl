from pathlib import Path

from owid.catalog import Dataset, Table, Variable

from etl.paths import DATA_DIR

CURRENT_DIR = Path(__file__).parent
METADATA_PATH = CURRENT_DIR / "patents_articles.meta.yml"


def run(dest_dir: str) -> None:
    ds = Dataset.create_empty(dest_dir)
    ds.metadata.update_from_yaml(METADATA_PATH, if_source_exists="replace")

    # Create and add patents table
    table = make_table()
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
    d = Dataset(DATA_DIR / "garden/worldbank_wdi/2022-05-26/wdi")
    table = d["wdi"][["ip_pat_resd", "ip_jrn_artc_sc"]]
    return table


def load_key_indicators() -> Table:
    d = Dataset(DATA_DIR / "garden/demography/2023-03-31/population")
    return d["population"]


def make_combined(table_wdi: Table, table_population: Table) -> Table:
    # Merge
    table = table_population.merge(table_wdi, left_index=True, right_index=True)

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
