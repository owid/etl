"""Label the emissions year actually compared with future heat-related mortality."""

from etl.helpers import PathFinder

paths = PathFinder(__file__)


def run() -> None:
    # Load observed emissions, the separate future mortality series, and the list of countries.
    config = paths.load_config()
    tb_emissions = (
        paths.load_dataset("global_carbon_budget")
        .read("global_carbon_budget")[["country", "year", "emissions_total_per_capita"]]
        .dropna()
    )
    tb_heat = (
        paths.load_dataset("heat_deaths_undp_climate_horizons")
        .read("heat_deaths_undp_climate_horizons")[["country", "year", "heat_death_rate"]]
        .dropna()
    )
    tb_regions = paths.load_dataset("regions").read("regions")
    countries = tb_regions[tb_regions["owid_region"].notnull()]["country"]

    # Use the latest emissions year with country data, so the axis label holds for every displayed country.
    tb_emissions = tb_emissions[tb_emissions["country"].isin(countries)]
    emissions_year = int(tb_emissions["year"].max())
    # Preserve the chart's nearest future projection horizon, which Grapher previously selected implicitly.
    future_years = tb_heat[tb_heat["year"] > emissions_year]["year"]
    assert not future_years.empty, "No mortality projections after the latest emissions year."
    heat_year = int(future_years.min())
    countries = sorted(
        set(tb_emissions[tb_emissions["year"] == emissions_year]["country"])
        & set(tb_heat[tb_heat["year"] == heat_year]["country"])
    )
    assert countries, "No countries with both recent emissions and projected mortality."

    # Grapher carries emissions forward to the projection year; exclude older country observations.
    view = config["views"][0]
    view["config"].update(
        minTime=heat_year, maxTime=heat_year, timelineMaxTime=heat_year, includedEntityNames=countries
    )
    view["indicators"]["x"]["display"].update(
        name=f"Per capita CO₂ emissions in {emissions_year}", tolerance=heat_year - emissions_year
    )
    view["indicators"]["y"][0]["display"]["tolerance"] = 0

    # Save the existing chart with its data-derived year label.
    paths.create_chart(config=config).save()
