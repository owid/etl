"""Load FAOSTAT food balances and FOFA 2050 projections, and create a garden dataset of meat consumption.

Historical meat (and egg) consumption comes from FAOSTAT Food Balance Sheets (element "Food", i.e. the quantity
available for human consumption). Projected consumption comes from FAO's 2018 report "The future of food and
agriculture - Alternative pathways to 2050", under its Business As Usual scenario.

The two sources are combined without any adjustment: the projections' model (FAO GAPS) is calibrated to FAOSTAT Food
Balance Sheets in its 2012 base year, and both sources agree within ~1% in that year (which is asserted below).
"""

from owid.catalog import Table
from owid.catalog import processing as pr

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Element code for "Food" (quantity available for human consumption, in tonnes) in faostat_fbsc.
ELEMENT_CODE_FOOD = "005142"

# Item codes in faostat_fbsc, and names of the output columns.
# NOTE: There is no FOFA projection for eggs, so eggs are informed only for the observed period.
FBSC_ITEM_CODES = {
    "00002731": "beef_and_buffalo",  # Bovine Meat
    "00002732": "sheep_and_goat",  # Mutton & Goat Meat
    "00002733": "pigmeat",  # Pigmeat
    "00002734": "poultry",  # Poultry Meat
    "00002744": "eggs",  # Eggs
}

# Items in the FOFA 2050 projections, and names of the output columns.
FOFA_ITEMS = {
    "Beef and veal": "beef_and_buffalo",
    "Sheep and goat meat": "sheep_and_goat",
    "Pigmeat": "pigmeat",
    "Poultry meat": "poultry",
}

# Scenario of the FOFA 2050 projections to adopt.
FOFA_SCENARIO = "Business As Usual"

# Maximum accepted relative deviation between the observed consumption and the projections' base year (2012).
# NOTE: The projections were calibrated to the Food Balance Sheets available in 2018, and FAOSTAT has revised its
# historical data since then; deviations are currently between 1.7% and 3.6%.
BASE_YEAR_RELATIVE_TOLERANCE = 0.05


def prepare_observed(tb_fbsc: Table) -> Table:
    observed = tb_fbsc[
        (tb_fbsc["country"] == "World")
        & (tb_fbsc["element_code"] == ELEMENT_CODE_FOOD)
        & (tb_fbsc["item_code"].isin(FBSC_ITEM_CODES))
    ][["country", "year", "item_code", "unit", "value"]].reset_index(drop=True)

    # Sanity checks.
    assert set(observed["item_code"]) == set(FBSC_ITEM_CODES), "Missing items in faostat_fbsc."
    assert set(observed["unit"]) == {"tonnes"}, "Units in faostat_fbsc have changed."
    assert (observed["value"] > 0).all(), "Non-positive consumption in faostat_fbsc."

    # Create a column for the consumption of each item.
    observed = observed.pivot(
        index=["country", "year"], columns="item_code", values="value", join_column_levels_with="_"
    )
    observed = observed.rename(columns=FBSC_ITEM_CODES, errors="raise")

    return observed


def prepare_projections(tb_fofa: Table) -> Table:
    projections = tb_fofa[
        (tb_fofa["region"] == "World")
        & (tb_fofa["indicator"] == "Commodity balances, volume")
        & (tb_fofa["element"] == "Food use")
        & (tb_fofa["scenario"] == FOFA_SCENARIO)
        & (tb_fofa["item"].isin(FOFA_ITEMS))
    ][["region", "year", "item", "units", "value"]].reset_index(drop=True)

    # Sanity checks.
    assert set(projections["item"]) == set(FOFA_ITEMS), "Missing items in FOFA projections."
    assert set(projections["units"]) == {"1000 tonnes"}, "Units in FOFA projections have changed."
    assert (projections["value"] > 0).all(), "Non-positive consumption in FOFA projections."

    # Convert from thousand tonnes to tonnes.
    projections["value"] *= 1000

    # Create a column for the consumption of each item.
    projections = projections.rename(columns={"region": "country"}).pivot(
        index=["country", "year"], columns="item", values="value", join_column_levels_with="_"
    )
    projections = projections.rename(columns=FOFA_ITEMS, errors="raise")

    return projections


def sanity_check_base_year(observed: Table, projections: Table) -> None:
    for column in FOFA_ITEMS.values():
        observed_2012 = observed.loc[observed["year"] == 2012, column].item()
        projected_2012 = projections.loc[projections["year"] == 2012, column].item()
        assert abs(projected_2012 - observed_2012) / observed_2012 < BASE_YEAR_RELATIVE_TOLERANCE, (
            f"Consumption of {column} in 2012 deviates more than expected between FAOSTAT (observed) and FOFA "
            f"(projections' base year)."
        )


def run() -> None:
    #
    # Load inputs.
    #
    # Load FAOSTAT food balances dataset and read its main table.
    ds_fbsc = paths.load_dataset("faostat_fbsc")
    tb_fbsc = ds_fbsc.read("faostat_fbsc")

    # Load FOFA 2050 projections dataset and read its main table.
    ds_fofa = paths.load_dataset("fofa_2050_regions")
    tb_fofa = ds_fofa.read("fofa_2050_regions")

    #
    # Process data.
    #
    # Prepare observed consumption (from FAOSTAT) and projected consumption (from FOFA).
    observed = prepare_observed(tb_fbsc=tb_fbsc)
    projections = prepare_projections(tb_fofa=tb_fofa)

    # Check that the projections' base year agrees with the observed data.
    sanity_check_base_year(observed=observed, projections=projections)

    # Combine observed data with the projected years that come after the last observed year.
    projections = projections[projections["year"] > observed["year"].max()].reset_index(drop=True)
    tb = pr.concat([observed, projections], ignore_index=True)

    # Improve table format.
    tb = tb.format(["country", "year"], short_name=paths.short_name)

    #
    # Save outputs.
    #
    # Initialize a new garden dataset.
    ds_garden = paths.create_dataset(tables=[tb], default_metadata=ds_fofa.metadata)

    # Save garden dataset.
    ds_garden.save()
