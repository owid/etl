"""Latest GDP per capita for each entity.

Published as CSV for use by Grapher codebase (entity sorting, peer countries).
Not meant to be imported to MySQL.
"""

from etl.helpers import PathFinder

paths = PathFinder(__file__)


def run() -> None:
    # Load GDP per capita dataset from grapher
    ds_gdp = paths.load_dataset("maddison_project_database")
    tb_gdp = ds_gdp["maddison_project_database"].reset_index()

    # Get latest year per country for GDP
    tb_gdp = tb_gdp[tb_gdp["gdp_per_capita"].notna()]
    idx_latest = tb_gdp.groupby("country")["year"].idxmax()
    tb = tb_gdp.loc[idx_latest, ["country", "year", "gdp_per_capita"]]

    # Rename columns
    tb = tb.rename(columns={"country": "entity", "gdp_per_capita": "value"})

    # Set index and name
    tb = tb.set_index(["entity", "year"], verify_integrity=True)
    tb.metadata.short_name = "gdp"

    # Save as CSV
    ds = paths.create_dataset(tables=[tb], formats=["csv", "json"])
    ds.save()
