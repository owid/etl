from owid.catalog import Table, Variable

from etl.helpers import PathFinder, create_dataset

# naming conventions
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    # Create and add patents table
    table = make_table()

    # Set an appropriate index and sort conveniently
    table = table.format(["country", "year"], sort_columns=True, short_name=paths.short_name)

    create_dataset(dest_dir, tables=[table]).save()


def make_table() -> Table:
    # Load internet data
    table_wdi = load_wdi()
    # Load population data
    table_population = load_key_indicators()
    # Combine sources
    table = make_combined(table_wdi, table_population)
    return table


def load_wdi() -> Table:
    ds_garden_wdi = paths.load_dataset("wdi")
    wdi = ds_garden_wdi["wdi"].reset_index()
    wdi = wdi.loc[:, ["country", "year", "ip_pat_resd", "ip_jrn_artc_sc"]]
    return wdi


def load_key_indicators() -> Table:
    population = paths.load_dataset("population")["population"]
    return population.reset_index().loc[:, ["country", "year", "population"]]


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
