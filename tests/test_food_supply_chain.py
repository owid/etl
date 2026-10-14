"""Food nutrients must survive rounded tonnages; each step decides where food falls back to its density.

The FBS step derives a density per cell and keeps a reported food nutrient only where the item's density is the one
derived directly from it. The SCL step reads one factor per country and item over all years, keeps every food
nutrient FAO reports, whatever density the item's other elements use, and converts food from tonnes only where FAO
reports no nutrient.
"""

import importlib

import numpy as np
import pytest
from owid.catalog import Table

FBS = importlib.import_module("etl.steps.data.garden.faostat.2026-09-04.food_supply_chain_fbs")
SCL = importlib.import_module("etl.steps.data.garden.faostat.2026-09-04.food_supply_chain_scl")
# FBS: six rows of one item: the first two food quantities have been rounded from 400 and 600 tonnes to 0 and 1,000
# tonnes; then a reported zero, a missing nutrient, a nutrient above the physical ceiling (a rejected density), and
# unrounded SCL food. The accepted densities are all 200 kcal per 100 g (20 g of protein at the protein scale), so
# the median that replaces the zero, the missing and the rejected density is 200 too.
NUTRIENT_REPORTED = np.array([8e8, 1.2e9, 0, np.nan, 2e10, 2.46e8])
SOURCES = ["direct", "direct", "country_median", "country_median", "country_median", "direct"]
PRODUCTION = np.array([0, 2e9, 2e9, 2e9, 2e9, 2.46e8])
# FBS: the reported nutrient only where the density is direct; tonnes times the median density elsewhere.
FOOD_FBS = np.array([8e8, 1.2e9, 2e9, 2e9, 2e9, 2.46e8])


def _six_rows(step, nutrient: str, scale: float) -> Table:
    tb = Table(
        {
            "country": ["Example"] * 6,
            "year": list(range(2000, 2006)),
            "item_code": ["00001"] * 6,
            "fao_item": ["Example food"] * 6,
            "role": ["crop"] * 6,
            "food_tonnes_for_density": [400, 600, 1000, 1000, 1000, 123],
            step.NUTRIENTS[nutrient]["numerator"]: NUTRIENT_REPORTED * scale,
        },
        index=[2, 4, 6, 8, 10, 12],
    )
    for element in step.BALANCE_ELEMENTS:
        tb[element] = 0.0
    tb["food"] = [0.0, 1000.0, 1000.0, 1000.0, 1000.0, 123.0]
    tb["production"] = tb["food"]
    return tb


def _food_and_adjustments(step, tb: Table, nutrient: str, **kwargs):
    tb = step.add_densities(tb, nutrient=nutrient, **kwargs)
    assert tb["density_source"].tolist() == SOURCES
    converted = step.convert_elements_to_nutrient(tb, nutrient=nutrient)
    population = tb[["country", "year"]].copy()
    population["population"] = 1000.0
    stages = step.sum_items_into_stages(converted.copy(), population)
    stages = step.move_rounding_gap_to_adjustments(stages)
    # The chain lands on food whatever food was set to: the difference is in data adjustments.
    np.testing.assert_allclose(stages["crop_production"] - stages["data_adjustments"], stages["food"])
    return converted, stages


@pytest.mark.parametrize("nutrient,scale", [("energy", 1), ("protein", 0.1)])
def test_fbs_reported_food_survives_rounding_but_fallbacks_are_kept(nutrient, scale):
    converted, stages = _food_and_adjustments(FBS, _six_rows(FBS, nutrient, scale), nutrient)
    np.testing.assert_allclose(converted["food"], FOOD_FBS * scale)
    np.testing.assert_allclose(converted["production"], PRODUCTION * scale)
    np.testing.assert_allclose(stages["data_adjustments"], (PRODUCTION - FOOD_FBS) * scale, atol=1e-6)


@pytest.mark.parametrize("nutrient,scale", [("energy", 1), ("protein", 0.1)])
def test_scl_reads_one_factor_per_country_and_item(nutrient, scale):
    numerator = SCL.NUTRIENTS[nutrient]["numerator"]
    # Country A eats 1,000 tonnes of the item over the years with a usable ratio (200 kcal per 100 g); its other
    # years have food but no tonnes, no reported nutrient, or a ratio above the physical ceiling. Country B eats only
    # 30 tonnes (at 300), so it takes the item's factor over all countries. Fish keep the density of each year.
    tb = Table(
        {
            "country": ["A"] * 5 + ["B"] * 2 + ["A"] * 3,
            "year": [2000, 2001, 2002, 2003, 2004, 2000, 2001, 2000, 2001, 2002],
            "item_code": ["00000001"] * 7 + ["00002763"] * 3,
            "fao_item": ["Example food"] * 7 + ["Pelagic Fish"] * 3,
            "role": ["crop"] * 7 + ["animal"] * 3,
            "food_tonnes_for_density": [400.0, 600.0, 0.0, 1000.0, 1000.0, 10.0, 20.0, 100.0, 100.0, 0.0],
            numerator: np.array([8e8, 1.2e9, 5e7, np.nan, 2e10, 3e7, 6e7, 8e7, 9e7, 0.0]) * scale,
        }
    )
    for element in SCL.BALANCE_ELEMENTS:
        tb[element] = 0.0
    tb["food"] = tb["food_tonnes_for_density"]
    tb["production"] = tb["food"]
    manual = {
        "fixed_densities": {"families": [], "items": []},
        "never_food": [],
        "output_implied_densities": [],
        "fish_from_fbs": [{"code": 2763}],
    }
    tb = SCL.add_densities(tb, manual=manual, nutrient=nutrient)
    assert tb["density_source"].tolist() == ["own_factor"] * 5 + ["item_factor"] * 2 + ["fish_year"] * 2 + [
        "own_factor"
    ]
    item_factor = (2e9 + 9e7) / (1030 * 1e4)
    np.testing.assert_allclose(tb["density"], np.array([200] * 5 + [item_factor] * 2 + [80, 90, 85]) * scale)
    converted = SCL.convert_elements_to_nutrient(tb, nutrient=nutrient)
    # Food keeps every nutrient FAO reports, including the one with no tonnes and the one above the ceiling; tonnes
    # times the factor only where the nutrient is missing.
    np.testing.assert_allclose(
        converted["food"], np.array([8e8, 1.2e9, 5e7, 2e9, 2e10, 3e7, 6e7, 8e7, 9e7, 0.0]) * scale
    )
    np.testing.assert_allclose(converted["production"], tb["production"] * 1e4 * tb["density"])


def _replaced_densities(step, nutrient: str, sources: list[str]) -> Table:
    # A reported nutrient next to each kind of density that is not the cell's own.
    tb = Table(
        {
            "country": ["Example"] * len(sources),
            "year": [2023] * len(sources),
            "role": ["crop"] * len(sources),
            "density_source": sources,
            "density": [10.0, 20.0, 30.0, 40.0],
            step.NUTRIENTS[nutrient]["numerator"]: [9e6] * len(sources),
        }
    )
    for element in step.BALANCE_ELEMENTS:
        tb[element] = 0.0
    tb["food"] = 1.0
    return tb


@pytest.mark.parametrize("nutrient", ["energy", "protein"])
def test_fbs_replacement_densities_still_determine_food(nutrient):
    tb = _replaced_densities(FBS, nutrient, ["country_median", "item_median", "fixed", "implied_by_products"])
    converted = FBS.convert_elements_to_nutrient(tb, nutrient=nutrient)
    np.testing.assert_array_equal(converted["food"], [1e5, 2e5, 3e5, 4e5])


@pytest.mark.parametrize("nutrient", ["energy", "protein"])
def test_scl_food_keeps_reported_nutrient_whatever_the_density_source(nutrient):
    tb = _replaced_densities(SCL, nutrient, ["own_factor", "item_factor", "fixed", "implied_by_products"])
    converted = SCL.convert_elements_to_nutrient(tb, nutrient=nutrient)
    np.testing.assert_array_equal(converted["food"], [9e6] * 4)
    # Food still uses the density where FAO reports no nutrient.
    tb[SCL.NUTRIENTS[nutrient]["numerator"]] = np.nan
    converted = SCL.convert_elements_to_nutrient(tb, nutrient=nutrient)
    np.testing.assert_array_equal(converted["food"], [1e5, 2e5, 3e5, 4e5])
