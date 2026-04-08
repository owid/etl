"""Load a meadow dataset and create a garden dataset."""

from typing import List

from owid.catalog import Table

from etl.data_helpers import geo
from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


# Expected classifications, sorted from oldest to newest.
EXPECTED_CLASSIFICATIONS = ["FPN 4.0"]
# Classification to adopt (by default, the latest one).
CLASSIFICATION = EXPECTED_CLASSIFICATIONS[-1]

# Since the 2025 update, all prices are given in both local currency unit and "ppp dollars".
# It's not specified whether the latter mean constant 2021 PPP$, or current PPP$.
# Previous updates were reported in current PPP$, but we confirmed that they are now constant PPP$.
# This clarification is relevant specifically for the cost of a healthy diet (since all other diets are informed for just a single year).
# Year assumed for constant PPP$.
PPP_YEAR = 2021

# Alternative attribution for share and number who cannot afford a healthy diet.
ATTRIBUTION_CANNOT_AFFORD = "FAO and World Bank (2025), using data and methods from Bai et al. (2024)"


def adapt_units(tb: Table) -> Table:
    # Change units from million people to people.
    for column in [column for column in tb.columns if column.startswith("millions_of_people")]:
        tb[column] *= 1e6
        tb = tb.rename(columns={column: column.replace("millions_of_people", "people")}, errors="raise")

    # Convert units expressed as fractions to percentages.
    for column in [
        column
        for column in tb.columns
        if (column.startswith(("cost_share_", "affordability_")) or "relative_to_the_" in column)
    ]:
        tb[column] *= 100

    return tb


def sanity_check_outputs(tb: Table) -> None:
    # Check data coverage is as expected.
    # All cost columns are given for just a single year, except the cost of a healthy diet, which is given for multiple years.
    for column in tb.columns:
        if ("cost_of_a" in column) and ("relative" not in column):
            if "healthy" not in column:
                assert set(tb.dropna(subset=column)["year"]) == {PPP_YEAR}
            else:
                assert len(set(tb.dropna(subset=column)["year"])) > 1

    # Other sanity checks.
    for column in tb.columns:
        if column.startswith("cost_of_"):
            assert (tb[column] > 0).all(), f"{column} has non-positive values"

    error = "The cost of a healthy diet was expected to be larger than the cost of a nutrient-adequate diet."
    assert (
        tb["cost_of_a_healthy_diet_in_ppp_dollars"] >= tb["cost_of_a_nutrient_adequate_diet_in_ppp_dollars"]
    ).all(), error

    error = "The cost of a nutrient-adequate diet was expected to be larger than the cost of an energy-sufficient diet."
    assert (
        tb["cost_of_a_nutrient_adequate_diet_in_ppp_dollars"] >= tb["cost_of_an_energy_sufficient_diet_in_ppp_dollars"]
    ).all()

    column = "cost_of_a_healthy_diet_in_ppp_dollars"
    check = tb.dropna(subset=column).sort_values(["country", "year"]).reset_index(drop=True)
    check["pct_change"] = abs(check.groupby("country")[column].pct_change())
    abrupt_changes = check[check["pct_change"].abs() > 0.3]
    error = "Abrupt variation in the cost of a healthy diet in unexpected countries."
    assert set(abrupt_changes["country"]) == {"Lebanon", "South Sudan", "Syria"}, error


def change_attribution(tb: Table, columns: List[str], attribution_text: str) -> Table:
    """
    Change attribution for some indicators as suggested by authors.
    """
    for col in columns:
        tb[col].m.origins[0].attribution = attribution_text

    return tb


def run() -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset and read its main table.
    ds_meadow = paths.load_dataset("food_prices_for_nutrition")
    tb = ds_meadow.read("food_prices_for_nutrition")

    #
    # Process data.
    #
    # Sanity check.
    error = "Expected classifications have changed."
    assert set(tb["classification"]) == set(EXPECTED_CLASSIFICATIONS), error

    # Select the latest classification.
    tb = (
        tb[tb["classification"] == CLASSIFICATION]
        .drop(columns=["classification"], errors="raise")
        .reset_index(drop=True)
    )

    # Rename columns conveniently.
    tb = tb.rename(columns={"economy": "country"}, errors="raise")

    # Harmonize country names.
    tb = geo.harmonize_countries(df=tb, countries_file=paths.country_mapping_path)

    # Adapt units.
    tb = adapt_units(tb=tb)

    # Change attributions for share and number of people who cannot afford a healthy diet.
    tb = change_attribution(
        tb=tb,
        columns=[
            "people_who_cannot_afford_a_healthy_diet",
            "percent_of_the_population_who_cannot_afford_a_healthy_diet",
        ],
        attribution_text=ATTRIBUTION_CANNOT_AFFORD,
    )

    # Remove attributions for cost_of_an_energy_sufficient_diet and cost_of_a_nutrient_adequate_diet.
    for diet in ["an_energy_sufficient", "a_nutrient_adequate"]:
        for currency in ["in_ppp_dollars", "in_local_currency_unit"]:
            tb = change_attribution(
                tb=tb,
                columns=[f"cost_of_{diet}_diet_{currency}"],
                attribution_text=None,
            )

    # Sanity check outputs.
    sanity_check_outputs(tb=tb)

    # Set an appropriate index and sort conveniently.
    tb = tb.format()

    # The costs in the original data (since the 2025 update) was given in local currency unit, and in constant PPP$.
    # For simplicity, since we are only using costs in constant PPP$, remove local currency unit.
    columns_to_drop = [
        column
        for column in tb.columns
        if (("in_local_currency_unit" in column) or ("in_current_ppp_dollars" in column))
    ]
    tb = tb.drop(columns=columns_to_drop, errors="raise")

    #
    # Save outputs.
    #
    # Create a new garden dataset.
    ds_garden = paths.create_dataset(tables=[tb])
    ds_garden.save()
