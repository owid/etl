"""Choose a scatter year supported by emissions, energy and historical population."""

from owid.catalog import processing as pr

from etl.helpers import PathFinder

paths = PathFinder(__file__)


def run() -> None:
    # Load each numeric axis, including the population used for bubble sizes, and the list of countries.
    config = paths.load_config()
    tb_emissions = (
        paths.load_dataset("global_carbon_budget")
        .read("global_carbon_budget")[["country", "year", "emissions_total_per_capita"]]
        .dropna()
    )
    tb_energy = (
        paths.load_dataset("energy_mix")
        .read("energy_mix")[["country", "year", "total_energy_supply_per_capita_kwh"]]
        .dropna()
    )
    tb_population = (
        paths.load_dataset("population").read("historical")[["country", "year", "population_historical"]].dropna()
    )
    tb_regions = paths.load_dataset("regions").read("regions")
    countries = tb_regions[tb_regions["owid_region"].notnull()]["country"]

    # Use the latest year with all three indicators; do not substitute projections for historical population.
    tb = pr.multi_merge([tb_emissions, tb_energy, tb_population], on=["country", "year"], how="inner")
    tb = tb[tb["country"].isin(countries)]
    year = int(tb["year"].max())
    tb_latest = tb[tb["year"] == year]
    assert (tb_latest["total_energy_supply_per_capita_kwh"] >= 0).all(), "Energy use must be non-negative."
    # Grapher omits zero-energy observations on the log axis, but can show them on a linear axis.
    assert (tb_latest["total_energy_supply_per_capita_kwh"] > 0).any(), "No observations for the log axis."
    assert (tb_latest["population_historical"] > 0).all(), "Bubble sizes require positive population."

    # Pin the chart to that year.
    view = config["views"][0]
    view["config"].update(minTime=year, maxTime=year, timelineMaxTime=year)
    for indicator in [view["indicators"]["y"][0], view["indicators"]["x"], view["indicators"]["size"]]:
        indicator.setdefault("display", {})["tolerance"] = 0

    # Save the existing chart with the calculated reporting year.
    paths.create_chart(config=config).save()
