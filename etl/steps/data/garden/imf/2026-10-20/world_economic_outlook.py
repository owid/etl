"""Load a meadow dataset and create a garden dataset."""

from owid.catalog import Table

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

RELEASE_YEAR = int(paths.version.split("-")[0])

# Indicators, each split into observations and forecasts.
INDICATORS = ["gross_domestic_product__gdp__constant_prices__percent_change", "unemployment_rate"]

# Minimum number of countries and country groups expected (207 in the April and October 2026 releases).
MIN_ENTITIES = 200


def run() -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("world_economic_outlook")

    # Read table from meadow dataset.
    tb = ds_meadow.read("world_economic_outlook")

    #
    # Process data.
    #
    # Harmonize country names.
    tb = paths.regions.harmonize_names(tb=tb)

    # Check the output is plausible.
    sanity_check_outputs(tb)

    tb = tb.format(["country", "year"])

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = paths.create_dataset(tables=[tb], default_metadata=ds_meadow.metadata)

    # Save changes in the new garden dataset.
    ds_garden.save()


def sanity_check_outputs(tb: Table) -> None:
    expected_columns = {f"{indicator}_{kind}" for indicator in INDICATORS for kind in ["observation", "forecast"]}
    assert set(tb.columns) - {"country", "year"} == expected_columns, f"Unexpected columns: {set(tb.columns)}."
    assert tb["country"].nunique() >= MIN_ENTITIES, f"Only {tb['country'].nunique()} countries and country groups."
    assert not tb.duplicated(subset=["country", "year"]).any(), "Duplicate (country, year) rows."

    for indicator in INDICATORS:
        observation, forecast = tb[f"{indicator}_observation"], tb[f"{indicator}_forecast"]
        # A year is either an observation or a forecast, never both.
        assert not (observation.notna() & forecast.notna()).any(), f"{indicator}: years that are both kinds."
        # Observations always come before forecasts.
        last_observation = tb[observation.notna()].groupby("country")["year"].max()
        first_forecast = tb[forecast.notna()].groupby("country")["year"].min()
        both = last_observation.index.intersection(first_forecast.index)
        assert (last_observation[both] < first_forecast[both]).all(), f"{indicator}: forecasts before observations."

    # Projections start in the release year for the world.
    world = tb[tb["country"] == "World"]
    gdp = INDICATORS[0]
    assert world.loc[world[f"{gdp}_observation"].notna(), "year"].max() == RELEASE_YEAR - 1, "World: wrong last year."
    assert world.loc[world[f"{gdp}_forecast"].notna(), "year"].min() == RELEASE_YEAR, "World: wrong first projection."

    # Value ranges (growth reached 148% in Equatorial Guinea in 1997, and -54% in Macao in 2020).
    for kind in ["observation", "forecast"]:
        growth = tb[f"{gdp}_{kind}"].dropna()
        assert growth.between(-70, 200).all(), f"GDP growth out of range: {growth.min()} to {growth.max()}."
        unemployment = tb[f"unemployment_rate_{kind}"].dropna()
        assert unemployment.between(0, 100).all(), f"Unemployment rate out of range: {unemployment.max()}."
