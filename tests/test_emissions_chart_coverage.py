"""Regression cases for charts whose emissions coverage differs from other inputs."""

import pytest
from owid.catalog import processing as pr

from etl.steps.viz.chart.emissions.latest.shared import (
    configure_average_map,
    configure_income_comparison,
    latest_common_observations,
    observations,
)


def test_population_projections_do_not_advance_income_comparison():
    entities = ["High", "Upper middle", "Lower middle", "Low"]
    emissions = pr.read_from_records(
        [{"country": c, "year": y, "emissions": 0 if c == "Low" else 30} for c in entities for y in [2022, 2023]]
        + [{"country": "High", "year": 2024, "emissions": 30}]
    )
    population = pr.read_from_records(
        [{"country": c, "year": y, "world_pop_share": 25} for c in entities for y in [2022, 2023, 2024, 2100]]
    )
    config = {"views": [{"indicators": {"y": [{}, {}]}, "config": {"selectedEntityNames": entities}}]}

    configure_income_comparison(config, emissions, population, "emissions")

    settings = config["views"][0]["config"]
    assert settings["minTime"] == settings["maxTime"] == settings["timelineMaxTime"] == 2023
    assert all(indicator["display"]["tolerance"] == 0 for indicator in config["views"][0]["indicators"]["y"])


def test_common_year_requires_overlap_for_the_same_country():
    emissions = pr.read_from_records(
        [{"country": "A", "year": 2023, "emissions": 1}, {"country": "A", "year": 2024, "emissions": 2}]
    )
    energy = pr.read_from_records(
        [{"country": "A", "year": 2023, "energy": 1}, {"country": "B", "year": 2024, "energy": 2}]
    )
    latest = latest_common_observations([emissions, energy])
    assert latest[["country", "year"]].values.tolist() == [["A", 2023]]


def test_missing_selected_entity_fails_instead_of_showing_partial_comparison():
    tb = pr.read_from_records([{"country": "A", "year": 2023, "emissions": 1}])
    with pytest.raises(AssertionError, match="No common reporting year"):
        latest_common_observations([tb], entities=["A", "B"])


def test_average_map_uses_country_year_and_unrounded_world_threshold():
    tb = pr.read_from_records(
        [
            {"country": "A", "year": 2023, "emissions": 0},
            {"country": "B", "year": 2023, "emissions": 12},
            {"country": "World", "year": 2023, "emissions": 4.712345},
            {"country": "World", "year": 2024, "emissions": 4.9},
        ]
    )
    config = {"views": [{"indicators": {"y": [{}]}, "config": {"map": {"colorScale": {}}}}]}

    configure_average_map(config, tb, "emissions", ["A", "B"])

    settings = config["views"][0]["config"]
    assert settings["minTime"] == settings["maxTime"] == settings["map"]["time"] == 2023
    assert settings["map"]["timeTolerance"] == 0
    assert settings["map"]["colorScale"]["customNumericValues"] == [0, 4.712345, 13]


def test_average_map_rejects_world_threshold_from_another_year():
    tb = pr.read_from_records(
        [{"country": "A", "year": 2023, "emissions": 1}, {"country": "World", "year": 2024, "emissions": 5}]
    )
    with pytest.raises(AssertionError, match="Missing World threshold"):
        configure_average_map({}, tb, "emissions", ["A"])


def test_missing_values_are_not_treated_as_zero():
    tb = pr.read_from_records(
        [{"country": "A", "year": 2023, "emissions": 0}, {"country": "A", "year": 2024, "emissions": None}]
    )
    latest = latest_common_observations([observations(tb, "emissions")])
    assert latest["year"].tolist() == [2023]
