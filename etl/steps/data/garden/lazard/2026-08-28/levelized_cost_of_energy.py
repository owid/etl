"""Load a meadow dataset and create a garden dataset."""

from owid.catalog import Table

from etl.helpers import PathFinder

paths = PathFinder(__file__)

# Year of the constant US dollars of the output (the latest year covered by the WDI deflator).
BASE_DOLLAR_YEAR = 2025
# The WDI deflator does not yet cover the year of the latest Lazard report, so it is extended using the growth of the US
# GDP implicit price deflator from the BEA (NIPA table 1.1.9, line 1, https://apps.bea.gov/iTable/?reqid=19&step=2):
# the annual value for the previous year, and the average of the quarters published so far for the latest year.
# NOTE: On the next update, move these two years forward (or remove them, if WDI already covers the latest report).
BEA_DEFLATOR = {2025: 128.979, 2026: 132.819}


def deflate_prices(tb: Table, tb_deflator: Table) -> Table:
    tb_deflator = tb_deflator[tb_deflator["country"] == "United States"][["year", "gdp_deflator_linked"]].rename(
        columns={"gdp_deflator_linked": "deflator"}
    )
    tb = tb.merge(tb_deflator, on="year", how="left")

    # Extend the deflator to the years not covered by WDI.
    missing_years = sorted(tb[tb["deflator"].isnull()]["year"])
    expected_missing_years = [year for year in BEA_DEFLATOR if year > BASE_DOLLAR_YEAR]
    assert missing_years == expected_missing_years, f"Deflator missing for unexpected years: {missing_years}."
    for year in missing_years:
        previous = tb.loc[tb["year"] == year - 1, "deflator"].item()
        tb.loc[tb["year"] == year, "deflator"] = previous * BEA_DEFLATOR[year] / BEA_DEFLATOR[year - 1]

    # Deflate the prices for all technologies.
    deflator_on_base_year = tb[tb["year"] == BASE_DOLLAR_YEAR]["deflator"].item()
    for column in tb.drop(columns=["year", "deflator"]).columns:
        tb[column] = tb[column] * deflator_on_base_year / tb["deflator"]

    tb = tb.drop(columns="deflator", errors="raise")

    return tb


def run() -> None:
    #
    # Load inputs.
    #
    ds_meadow = paths.load_dataset("levelized_cost_of_energy")
    tb = ds_meadow.read("levelized_cost_of_energy")

    ds_deflator = paths.load_dataset("owid_deflator")
    tb_deflator = ds_deflator.read("owid_deflator")

    #
    # Process data.
    #
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

    tb_deflated = deflate_prices(tb=tb, tb_deflator=tb_deflator)

    # Transpose tables and combine them.
    tb_deflated = tb_deflated.melt(id_vars="year", var_name="technology", value_name="lcoe")
    tb = tb.melt(id_vars="year", var_name="technology", value_name="lcoe_unadjusted")
    tb = tb.merge(tb_deflated, on=["year", "technology"])
    tb = tb.dropna(subset=["lcoe", "lcoe_unadjusted"], how="all")

    # Range of years (used in the metadata).
    year_lapse = int(tb["year"].max() - tb["year"].min())

    tb = tb.format(["year", "technology"])

    #
    # Save outputs.
    #
    ds_garden = paths.create_dataset(
        tables=[tb],
        default_metadata=ds_meadow.metadata,
        yaml_params={"BASE_DOLLAR_YEAR": BASE_DOLLAR_YEAR, "YEAR_LAPSE": year_lapse},
    )
    ds_garden.save()
