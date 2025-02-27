"""Load a meadow dataset and create a garden dataset."""

from owid.catalog import Table

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Define the original base year of the deflator.
BASE_DOLLAR_YEAR_ORIGINAL = 2015
# Define the base year for the deflator.
BASE_DOLLAR_YEAR = 2023
# We use the WDI GDP deflator, but this is usually missing for the latest year.
# We manually add the missing value here.
# To get this value, go to:
# https://fred.stlouisfed.org/series/GDPDEF
# click on "Edit Graph",
# set "Modify frequency" to "Annual",
# then below, in "Units", select from the dropdown "Index (Scale value to 100 for chosen date)",
# in the "or" field below, add the base year of the original WDI deflator (which, in the last update, was 2015).
# NOTE: I used 2015-07-01, unsure if it should be some other day in the year, but the retrieved numbers for previous years coincided well with the given WDI values.
DEFLATOR_MISSING_VALUES = {2024: 128.7}


def deflate_prices(tb: Table, tb_wdi: Table) -> Table:
    # Get the US GDP deflator.
    tb_deflator = tb_wdi[tb_wdi["country"] == "United States"][["year", "ny_gdp_defl_zs"]].rename(
        columns={"ny_gdp_defl_zs": "deflator"}
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
    tb = tb.drop(columns="deflator")

    return tb


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset and read its main table.
    ds_meadow = paths.load_dataset("levelized_cost_of_energy")
    tb = ds_meadow.read("levelized_cost_of_energy")

    # Load WDI dataset (used to deflate prices).
    ds_wdi = paths.load_dataset("wdi")
    tb_wdi = ds_wdi.read("wdi")

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
    tb_deflated = deflate_prices(tb=tb, tb_wdi=tb_wdi)

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
    ds_garden = create_dataset(
        dest_dir,
        tables=[tb],
        check_variables_metadata=True,
        default_metadata=ds_meadow.metadata,
        yaml_params={"BASE_DOLLAR_YEAR": BASE_DOLLAR_YEAR, "BASE_DOLLAR_YEAR_ORIGINAL": BASE_DOLLAR_YEAR_ORIGINAL},
    )

    # Save changes in the new garden dataset.
    ds_garden.save()
