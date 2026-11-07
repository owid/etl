"""Label the emissions year actually compared with future heat-related mortality."""

from etl.helpers import PathFinder
from etl.steps.viz.chart.emissions.latest.shared import country_names, latest_common_observations, observations

paths = PathFinder(__file__)


def run() -> None:
    # Load observed emissions and the separate future mortality series.
    config = paths.load_config()
    emissions = observations(
        paths.load_dataset("global_carbon_budget").read("global_carbon_budget"), "emissions_total_per_capita"
    )
    heat = observations(
        paths.load_dataset("heat_deaths_undp_climate_horizons").read("heat_deaths_undp_climate_horizons"),
        "heat_death_rate",
    )

    # Require a common emissions year so the axis label holds for every displayed country.
    emissions = emissions[emissions["country"].isin(country_names(paths))]
    latest = latest_common_observations([emissions])
    emissions_year = int(latest["year"].iloc[0])
    # Preserve the chart's nearest future projection horizon, which Grapher previously selected implicitly.
    future_years = heat.loc[heat["year"] > emissions_year, "year"]
    assert not future_years.empty, "No mortality projections after the latest emissions year."
    heat_year = int(future_years.min())
    countries = sorted(set(latest["country"]) & set(heat.loc[heat["year"] == heat_year, "country"]))
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
