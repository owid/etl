"""Food nutrients must survive rounded tonnages without undoing density fallbacks or overrides."""

import importlib

import numpy as np
import pytest
from owid.catalog import Table


@pytest.fixture(params=["fbs", "scl"])
def chain_step(request):
    return importlib.import_module(f"etl.steps.data.garden.faostat.2026-09-04.food_supply_chain_{request.param}")


@pytest.mark.parametrize("nutrient,scale", [("energy", 1), ("protein", 0.1)])
def test_reported_food_survives_rounding_but_fallbacks_are_kept(chain_step, nutrient, scale):
    # The first two food quantities have been rounded from 400 and 600 tonnes to 0 and 1,000 tonnes.
    # Other rows exercise a reported zero, a missing nutrient, a rejected density and unrounded SCL food.
    tb = Table(
        {
            "country": ["Example"] * 6,
            "year": list(range(2000, 2006)),
            "item_code": ["00001"] * 6,
            "fao_item": ["Example food"] * 6,
            "role": ["crop"] * 6,
            "food_tonnes_for_density": [400, 600, 1000, 1000, 1000, 123],
            chain_step.NUTRIENTS[nutrient]["numerator"]: np.array([8e8, 1.2e9, 0, np.nan, 2e10, 2.46e8]) * scale,
        },
        index=[2, 4, 6, 8, 10, 12],
    )
    for element in chain_step.BALANCE_ELEMENTS:
        tb[element] = 0.0
    tb["food"] = [0.0, 1000.0, 1000.0, 1000.0, 1000.0, 123.0]
    tb["production"] = tb["food"]
    kwargs = {}
    if chain_step.paths.short_name.endswith("scl"):
        kwargs["manual"] = {
            "fixed_densities": {"families": [], "items": []},
            "never_food": [],
            "output_implied_densities": [],
        }
    tb = chain_step.add_densities(tb, nutrient=nutrient, **kwargs)
    assert tb["density_source"].tolist() == [
        "direct",
        "direct",
        "country_median",
        "country_median",
        "country_median",
        "direct",
    ]
    converted = chain_step.convert_elements_to_nutrient(tb, nutrient=nutrient)
    np.testing.assert_allclose(converted["food"], np.array([8e8, 1.2e9, 2e9, 2e9, 2e9, 2.46e8]) * scale)
    np.testing.assert_allclose(converted["production"], np.array([0, 2e9, 2e9, 2e9, 2e9, 2.46e8]) * scale)

    population = tb[["country", "year"]].copy()
    population["population"] = 1000.0
    stages = chain_step.sum_items_into_stages(converted, population)
    stages = chain_step.move_rounding_gap_to_adjustments(stages)
    np.testing.assert_allclose(stages["data_adjustments"], np.array([-8e8, 8e8, 0, 0, 0, 0]) * scale, atol=1e-6)
    np.testing.assert_allclose(stages["crop_production"] - stages["data_adjustments"], stages["food"])


@pytest.mark.parametrize("nutrient", ["energy", "protein"])
def test_replacement_densities_still_determine_food(chain_step, nutrient):
    # A reported nutrient can exist even when its density was overridden. It must not override that decision.
    sources = ["country_median", "item_median", "fixed", "implied_by_products"]
    tb = Table(
        {
            "country": ["Example"] * len(sources),
            "year": [2023] * len(sources),
            "role": ["crop"] * len(sources),
            "density_source": sources,
            "density": [10.0, 20.0, 30.0, 40.0],
            chain_step.NUTRIENTS[nutrient]["numerator"]: [9e6] * len(sources),
        }
    )
    for element in chain_step.BALANCE_ELEMENTS:
        tb[element] = 0.0
    tb["food"] = 1.0
    converted = chain_step.convert_elements_to_nutrient(tb, nutrient=nutrient)
    np.testing.assert_array_equal(converted["food"], [1e5, 2e5, 3e5, 4e5])
