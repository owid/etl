"""Compare population and territorial emissions shares in their latest complete year."""

from owid.catalog import processing as pr

from etl.helpers import PathFinder

paths = PathFinder(__file__)


def run() -> None:
    # Load the chart and both indicators.
    config = paths.load_config()
    column = "emissions_total_as_share_of_global"
    tb_emissions = (
        paths.load_dataset("global_carbon_budget").read("global_carbon_budget")[["country", "year", column]].dropna()
    )
    tb_population = paths.load_dataset("population").read("population")[["country", "year", "world_pop_share"]].dropna()

    # Find the latest year with both shares for every selected income group.
    view = config["views"][0]
    entities = view["config"]["selectedEntityNames"]
    tb = pr.merge(tb_emissions, tb_population, on=["country", "year"], how="inner")
    tb = tb[tb["country"].isin(entities)]
    n_entities = tb.groupby("year", observed=True)["country"].nunique()
    complete_years = n_entities[n_entities == len(entities)].index
    assert len(complete_years) > 0, "No year with both shares for every selected income group."
    year = int(max(complete_years))

    # Pin the chart to that year.
    view["config"].update(minTime=year, maxTime=year, timelineMaxTime=year)
    for indicator in view["indicators"]["y"]:
        indicator.setdefault("display", {})["tolerance"] = 0

    # Save the existing chart with the calculated reporting year.
    paths.create_chart(config=config).save()
