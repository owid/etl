"""Latest population and GDP per capita for each entity.

Published as CSV for use by Grapher codebase (entity sorting, peer countries).
Not meant to be imported to MySQL.
"""

from owid.catalog import Table

from etl.helpers import PathFinder, create_dataset

paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    # Load population dataset
    ds_pop = paths.load_dataset("population")
    tb_pop = ds_pop["population"].reset_index()

    # Get latest year per country for population
    idx_latest = tb_pop.groupby("country")["year"].idxmax()
    tb_pop = tb_pop.loc[idx_latest, ["country", "year", "population"]]

    # Load GDP per capita dataset
    ds_gdp = paths.load_dataset("maddison_project_database")
    tb_gdp = ds_gdp["maddison_project_database"].reset_index()

    # Get latest year per country for GDP
    tb_gdp = tb_gdp[tb_gdp["gdp_per_capita"].notna()]
    idx_latest = tb_gdp.groupby("country")["year"].idxmax()
    tb_gdp = tb_gdp.loc[idx_latest, ["country", "year", "gdp_per_capita"]]

    # Merge (outer join to include all entities, nulls allowed)
    tb = Table(tb_pop.merge(tb_gdp, on=["country", "year"], how="outer"))

    # Set index and name
    tb = tb.set_index(["country", "year"], verify_integrity=True)
    tb.metadata.short_name = "population_and_gdp"

    # Save as CSV
    ds = create_dataset(dest_dir, tables=[tb], formats=["csv"])
    ds.save()
