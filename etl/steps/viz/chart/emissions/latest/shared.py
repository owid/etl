"""Coverage rules for CO₂ charts whose settings depend on their data."""

import math
from typing import Any

from owid.catalog import Table
from owid.catalog import processing as pr

from etl.helpers import PathFinder


def observations(tb: Table, column: str) -> Table:
    """Return unique country-year observations, without treating missing values as zeros."""
    tb = tb.reset_index()[["country", "year", column]].dropna(subset=[column])
    assert not tb.empty, f"No observations for {column}."
    assert not tb.duplicated(["country", "year"]).any(), f"Duplicate observations for {column}."
    assert tb[column].map(math.isfinite).all(), f"Non-finite observations for {column}."
    return tb


def latest_common_observations(tables: list[Table], entities: list[str] | None = None) -> Table:
    """Select the latest exact-year overlap, requiring every requested entity.

    For example, population through 2100 and emissions through 2023 must select 2023.
    If one of the four selected income groups lacks emissions in 2023, use the latest
    earlier year with both indicators for all four groups.
    """
    assert tables, "At least one indicator is required."
    tb = tables[0]
    for other in tables[1:]:
        tb = pr.merge(tb, other, on=["country", "year"], how="inner", validate="one_to_one")
    if entities is not None:
        assert entities and len(set(entities)) == len(entities), "Expected distinct selected entities."
        tb = tb[tb["country"].isin(entities)]
        complete_years = tb.groupby("year", observed=True)["country"].nunique()
        tb = tb[tb["year"].isin(complete_years[complete_years == len(entities)].index)]
    assert not tb.empty, "No common reporting year with data for every required indicator and entity."
    return tb[tb["year"] == tb["year"].max()].copy()


def country_names(paths: PathFinder) -> list[str]:
    tb = paths.load_dataset("regions").read("regions").reset_index()
    countries = sorted(tb.loc[tb["owid_region"].notna(), "country"].unique())
    assert countries and "World" not in countries, "Expected countries, not regional aggregates."
    return countries


def pin_year(config: dict[str, Any], year: int) -> None:
    """Keep both the initial view and the timeline within the supported data coverage."""
    config.update(minTime=year, maxTime=year, timelineMaxTime=year)


def configure_average_map(config: dict[str, Any], tb: Table, column: str, countries: list[str]) -> None:
    """Use the World value for the latest year with country data, not a World-only year."""
    tb = observations(tb, column)
    latest = latest_common_observations([tb[tb["country"].isin(countries)]])
    year = int(latest["year"].iloc[0])
    world = tb.loc[(tb["country"] == "World") & (tb["year"] == year), column]
    assert len(world) == 1, f"Missing World threshold for {column} in {year}."
    threshold = float(world.iloc[0])
    assert threshold > 0 and (latest[column] >= 0).all(), "Expected non-negative per capita emissions."

    view = config["views"][0]
    pin_year(view["config"], year)
    map_config = view["config"]["map"]
    map_config.update(time=year, timeTolerance=0)
    # The final bracket must contain even the largest country's value.
    map_config["colorScale"]["customNumericValues"] = [0, threshold, max(threshold, float(latest[column].max())) + 1]
    view["indicators"]["y"][0].setdefault("display", {})["tolerance"] = 0


def configure_income_comparison(config: dict[str, Any], emissions: Table, population: Table, column: str) -> None:
    view = config["views"][0]
    latest = latest_common_observations(
        [observations(emissions, column), observations(population, "world_pop_share")],
        entities=view["config"]["selectedEntityNames"],
    )
    assert latest[[column, "world_pop_share"]].ge(0).all().all(), "Shares must be non-negative."
    assert latest[[column, "world_pop_share"]].le(100).all().all(), "Shares must not exceed 100%."
    pin_year(view["config"], int(latest["year"].iloc[0]))
    for indicator in view["indicators"]["y"]:
        indicator.setdefault("display", {})["tolerance"] = 0
