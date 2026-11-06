"""Choose a scatter year supported by emissions, energy and historical population."""

from etl.helpers import PathFinder
from etl.steps.viz.chart.emissions.latest.shared import (
    country_names,
    latest_common_observations,
    observations,
    pin_year,
)

paths = PathFinder(__file__)


def run() -> None:
    # Load each numeric axis, including the population used for bubble sizes.
    config = paths.load_config()
    emissions = paths.load_dataset("global_carbon_budget").read("global_carbon_budget")
    energy = paths.load_dataset("energy_mix").read("energy_mix")
    population = paths.load_dataset("population").read("historical")
    tables = [
        observations(emissions, "emissions_total_per_capita"),
        observations(energy, "total_energy_supply_per_capita_kwh"),
        observations(population, "population_historical"),
    ]

    # Use exact-year observations; do not substitute projections for historical population.
    countries = country_names(paths)
    latest = latest_common_observations([tb[tb["country"].isin(countries)] for tb in tables])
    assert (latest["total_energy_supply_per_capita_kwh"] >= 0).all(), "Energy use must be non-negative."
    # Grapher omits zero-energy observations on the log axis, but can show them on a linear axis.
    assert (latest["total_energy_supply_per_capita_kwh"] > 0).any(), "No observations for the log axis."
    assert (latest["population_historical"] > 0).all(), "Bubble sizes require positive population."
    view = config["views"][0]
    pin_year(view["config"], int(latest["year"].iloc[0]))
    for indicator in [view["indicators"]["y"][0], view["indicators"]["x"], view["indicators"]["size"]]:
        indicator.setdefault("display", {})["tolerance"] = 0

    # Save the existing chart with the calculated reporting year.
    paths.create_chart(config=config).save()
