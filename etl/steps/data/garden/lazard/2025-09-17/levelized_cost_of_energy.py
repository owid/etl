"""Load a meadow dataset and create a garden dataset."""

from owid.catalog import Table

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Define the original base year of the deflator.
BASE_DOLLAR_YEAR_ORIGINAL = 2017
# Define the base year for the deflator.
BASE_DOLLAR_YEAR = 2024
# We use the WDI GDP deflator: linked series, but this is usually missing the latest year.
# We manually add the missing value here.
# To get this value, go to:
# https://fred.stlouisfed.org/series/GDPDEF
# click on "Edit Graph", and set:
# * Modify frequency: "Annual" if possible, otherwise "Semiannual" (if there's no annual data yet for the last year),
# * Aggregation method: "Average",
# * Everything else by default, e.g. in "Units", leave it on "Select".
DEFLATOR_MISSING_VALUES = {2025: 127.742}


def deflate_prices(tb: Table, tb_deflator: Table) -> Table:
    # Get the US GDP deflator.
    tb_deflator = tb_deflator[tb_deflator["country"] == "United States"][["year", "gdp_deflator_linked"]].rename(
        columns={"gdp_deflator_linked": "deflator"}
    )

    # Merge the deflator with the main table.
    tb = tb.merge(tb_deflator, on="year", how="left")

    error = "Deflator base year has changed. Simply redefine BASE_DOLLAR_ORIGINAL."
    assert tb[tb["year"] == BASE_DOLLAR_YEAR_ORIGINAL]["deflator"].item() == 100, error

    # Fill missing rows (caused by the deflator not having data for the latest year).
    error = f"Expected missing data in deflator for the years {set(DEFLATOR_MISSING_VALUES)}."
    for year in DEFLATOR_MISSING_VALUES:
        assert tb.loc[tb["year"] == year, "deflator"].isnull().item(), error
        tb.loc[tb["year"] == year, "deflator"] = DEFLATOR_MISSING_VALUES[year]

    # Deflate the prices for all technologies.
    deflator_on_base_year = tb[tb["year"] == BASE_DOLLAR_YEAR]["deflator"].item()
    for column in tb.drop(columns=["year", "deflator"]).columns:
        tb[column] = tb[column] * deflator_on_base_year / tb["deflator"]

    # Remove the deflator column.
    tb = tb.drop(columns="deflator", errors="raise")

    return tb


def run() -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset and read its main table.
    ds_meadow = paths.load_dataset("levelized_cost_of_energy")
    tb = ds_meadow.read("levelized_cost_of_energy")

    # Load OWID Deflator dataset.
    ds_deflator = paths.load_dataset("owid_deflator")
    tb_deflator = ds_deflator.read("owid_deflator")

    #
    # Process data.
    #
    # Rename technologies.
    tb = tb.rename(
        columns={
            "nuclear": "Nuclear",
            "gas_peaking": "Gas peaking",
            "coal": "Coal",
            "geothermal": "Geothermal",
            "gas_combined_cycle": "Gas combined cycle",
            "solar_pv": "Solar photovoltaic",
            "wind_onshore": "Onshore wind",
        }
    )

    # Create a new table of deflate prices.
    tb_deflated = deflate_prices(tb=tb, tb_deflator=tb_deflator)

    # Transpose tables.
    tb_deflated = tb_deflated.melt(id_vars="year", var_name="technology", value_name="lcoe")
    tb = tb.melt(id_vars="year", var_name="technology", value_name="lcoe_unadjusted")

    # Combine tables.
    tb = tb.merge(tb_deflated, on=["year", "technology"])

    # Improve tables format.
    tb = tb.format(["year", "technology"])

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = paths.create_dataset(
        tables=[tb],
        default_metadata=ds_meadow.metadata,
        yaml_params={"BASE_DOLLAR_YEAR": BASE_DOLLAR_YEAR, "BASE_DOLLAR_YEAR_ORIGINAL": BASE_DOLLAR_YEAR_ORIGINAL},
    )

    # Save changes in the new garden dataset.
    ds_garden.save()
