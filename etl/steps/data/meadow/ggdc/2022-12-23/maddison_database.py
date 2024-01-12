"""Load a snapshot and create a meadow dataset."""

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("maddison_database.xlsx")

    # Load data from snapshot.
    tb_pop = snap.read(sheet_name="Population", skiprows=2)
    tb_gdp = snap.read(sheet_name="GDP", skiprows=2)
    tb_gdppc = snap.read(sheet_name="PerCapita GDP", skiprows=2)

    #
    # Process data.

    # As this is a bespoke dataset I am only keeping the World data for now
    # Population sheet
    tb_pop = tb_pop.rename(columns={"Unnamed: 0": "year", "World Total": "population"})
    tb_pop = tb_pop[["year", "population"]]
    tb_pop = tb_pop.dropna().reset_index(drop=True)
    tb_pop["year"] = tb_pop["year"].astype(int)

    # GDP sheet
    tb_gdp = tb_gdp.rename(columns={"Unnamed: 0": "year", "World Total": "gdp"})
    tb_gdp = tb_gdp[["year", "gdp"]]
    tb_gdp = tb_gdp.dropna().reset_index(drop=True)
    tb_gdp["year"] = tb_gdp["year"].astype(int)

    # GDP per capita sheet
    tb_gdppc = tb_gdppc.rename(columns={"Unnamed: 0": "year", "World Total": "gdp_per_capita"})
    tb_gdppc = tb_gdppc[["year", "gdp_per_capita"]]
    tb_gdppc = tb_gdppc.dropna().reset_index(drop=True)
    tb_gdppc["year"] = tb_gdppc["year"].astype(int)

    # Merge all these tables
    tb = tb_gdp.merge(tb_gdppc, on="year", how="outer", sort=True)
    tb = tb.merge(tb_pop, on="year", how="outer", sort=True)

    # Adjust country and population columns and reorder
    tb["country"] = "World"
    tb["population"] = tb["population"].astype(int)
    tb = tb[["year", "country", "gdp", "gdp_per_capita", "population"]]

    #
    # Ensure all columns are snake-case, set an appropriate index, and sort conveniently.
    tb = tb.underscore().set_index(["country", "year"], verify_integrity=True).sort_index()

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=snap.metadata)

    # Save changes in the new meadow dataset.
    ds_meadow.save()
