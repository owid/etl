"""Compare countries with the World average for the same reporting year."""

from etl.helpers import PathFinder

paths = PathFinder(__file__)


def run() -> None:
    # Load the chart, its emissions data, and the list of countries.
    config = paths.load_config()
    column = "emissions_total_per_capita"
    tb = paths.load_dataset("global_carbon_budget").read("global_carbon_budget")[["country", "year", column]].dropna()
    tb_regions = paths.load_dataset("regions").read("regions")
    countries = tb_regions[tb_regions["owid_region"].notnull()]["country"]

    # Use the latest year with country data, and the World average of that same year as the threshold.
    tb_countries = tb[tb["country"].isin(countries)]
    year = int(tb_countries["year"].max())
    world = tb[(tb["country"] == "World") & (tb["year"] == year)][column]
    assert len(world) == 1, f"Missing World average in {year}."
    threshold = float(world.iloc[0])
    assert threshold > 0, "Expected a positive World average."
    largest = float(tb_countries[tb_countries["year"] == year][column].max())

    # Pin the chart and its map to that year.
    view = config["views"][0]
    view["config"].update(minTime=year, maxTime=year, timelineMaxTime=year)
    view["config"]["map"].update(time=year, timeTolerance=0)
    # The final bracket must contain even the largest country's value.
    view["config"]["map"]["colorScale"]["customNumericValues"] = [0, threshold, max(threshold, largest) + 1]
    view["indicators"]["y"][0].setdefault("display", {})["tolerance"] = 0

    # Save the existing chart with the calculated settings.
    paths.create_chart(config=config).save()
