"""Load a meadow dataset and create a garden dataset."""

from etl.data_helpers import geo
from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("knoll_prices")
    ds_gdp = paths.load_dataset("maddison_project_database")

    # Read table from meadow dataset.
    tb = ds_meadow.read("knoll_prices")
    tb_gdp = ds_gdp.read("maddison_project_database")
    tb_gdp = tb_gdp[["country", "year", "gdp_per_capita"]]

    # Calculate real house prices from nominal house prices and CPI
    # This calculation is identical to the calculation used in the original paper which can be found here: https://www.aeaweb.org/articles?id=10.1257/aer.20150501
    tb["hpreal"] = tb["hpnom"] / tb["cpi"] * 100

    tb = tb[["country", "year", "hpreal"]]

    #
    # Process data.
    #
    # Harmonize country names.
    tb = geo.harmonize_countries(df=tb, countries_file=paths.country_mapping_path, warn_on_unused_countries=False)

    # calculate real house prices per gdp per capita
    tb = tb.merge(tb_gdp, on=["country", "year"], how="left")

    tb["hp_per_gdp"] = tb["hpreal"] / tb["gdp_per_capita"] * 1000

    # index time series to 1990 value for each country
    for country in tb["country"].unique():
        tb_c = tb.loc[tb["country"] == country]
        # check that the country has data for 1990
        if 1990 not in tb_c["year"].values:
            raise ValueError(f"Country {country} does not have data for 1990.")
        # normalize to 1990 value
        norm = tb_c.loc[tb_c["year"] == 1990, "hp_per_gdp"].iloc[0]
        tb.loc[tb["country"] == country, "hp_per_gdp_nom"] = (tb_c["hp_per_gdp"] / norm) * 100

    tb["hp_per_gdp"] = tb["hp_per_gdp_nom"].round(3)
    tb["hp_per_gdp"].m.origins = [tb["hpreal"].m.origins[0], tb["gdp_per_capita"].m.origins[0]]

    tb = tb[["country", "year", "hpreal", "hp_per_gdp"]]

    # Improve table format.
    tb = tb.format(["country", "year"])

    #
    # Save outputs.
    #
    # Initialize a new garden dataset.
    ds_garden = paths.create_dataset(tables=[tb], default_metadata=ds_meadow.metadata)

    # Save garden dataset.
    ds_garden.save()
