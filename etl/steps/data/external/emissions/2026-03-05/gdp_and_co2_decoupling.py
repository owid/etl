"""Export start and end year data for countries that have decoupled GDP growth from CO2 emissions."""

import owid.catalog.processing as pr

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    ds_garden = paths.load_dataset("gdp_and_co2_decoupling")
    tb = ds_garden.read("gdp_and_co2_decoupling")

    #
    # Process data.
    #
    # Filter to only years since peak emissions year.
    tb_since_peak = tb[tb["year"] >= tb["peak_emissions_year"]].reset_index(drop=True)

    # Get only the first (peak emissions year) and last year for each country.
    tb_result = pr.concat(
        [
            tb_since_peak.groupby("country", as_index=False).first(),
            tb_since_peak.groupby("country", as_index=False).last(),
        ],
        ignore_index=True,
    )

    # Compute percentage change in smoothed GDP and emissions from peak to latest year.
    # For each country, the first row is the peak year (reference), the second is the latest year.
    tb_result = tb_result.sort_values(["country", "year"]).reset_index(drop=True)
    ref_values = (
        tb_result.groupby("country", as_index=False)
        .first()[["country", "gdp_per_capita_smooth", "consumption_emissions_per_capita_smooth"]]
        .rename(
            columns={"gdp_per_capita_smooth": "ref_gdp", "consumption_emissions_per_capita_smooth": "ref_emissions"}
        )
    )
    tb_result = tb_result.merge(ref_values, on="country", how="left")
    tb_result["gdp_per_capita_change"] = (
        (tb_result["gdp_per_capita_smooth"] - tb_result["ref_gdp"]) / tb_result["ref_gdp"] * 100
    )
    tb_result["consumption_emissions_per_capita_change"] = (
        (tb_result["consumption_emissions_per_capita_smooth"] - tb_result["ref_emissions"])
        / tb_result["ref_emissions"]
        * 100
    )

    # Drop reference columns.
    tb_result = tb_result.drop(columns=["ref_gdp", "ref_emissions"])

    # Improve table format.
    tb_result = tb_result.format(["country", "year"], short_name=paths.short_name)

    #
    # Save outputs.
    #
    ds = paths.create_dataset(tables=[tb_result], formats=["csv"])
    ds.save()
