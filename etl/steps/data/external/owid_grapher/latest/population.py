"""Latest population for each entity.

Published as CSV for use by Grapher codebase (entity sorting, peer countries).
Not meant to be imported to MySQL.
"""

from etl.helpers import PathFinder

paths = PathFinder(__file__)


def run() -> None:
    # Load population dataset from grapher
    ds_pop = paths.load_dataset()
    tb_pop = ds_pop["historical"].reset_index()

    # Get latest year per country for population
    idx_latest = tb_pop.groupby("country")["year"].idxmax()
    tb = tb_pop.loc[idx_latest, ["country", "year", "population_historical"]]

    # Rename columns
    tb = tb.rename(columns={"country": "entityName", "population_historical": "value"})

    # Set index and name
    tb = tb.set_index(["entityName", "year"], verify_integrity=True)
    tb.metadata.short_name = "population"

    # Save as CSV
    ds = paths.create_dataset(tables=[tb], formats=["csv", "json"])
    ds.save()
