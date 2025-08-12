"""Load a meadow dataset and create a garden dataset."""

from typing import List

from owid.catalog import Table

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


# Expected classifications, sorted from oldest to newest.
EXPECTED_CLASSIFICATIONS = ["FPN 1.0", "FPN 1.1", "FPN 2.0", "FPN 2.1", "FPN 3.0"]
# Classification to adopt (by default, the latest one).
CLASSIFICATION = EXPECTED_CLASSIFICATIONS[-1]

# All costs are given in constant 2021 PPP$ per person per day, except for the cost of a healthy diet,
# which is reported in current PPP$ per person per day for each year.
# To express the healthy diet costs in constant 2021 PPP$, we need to adjust for inflation.
# This is done by multiplying the cost in a given YEAR by the ratio CPI(BASE_YEAR) / CPI(YEAR).
# Base year for CPI corrections.
CPI_BASE_YEAR = 2021

# Alternative attribution for share and number who cannot afford a healthy diet.
ATTRIBUTION_CANNOT_AFFORD = "FAO and World Bank (2024), using data and methods from Bai et al. (2024)"


def adapt_units(tb: Table, tb_wdi: Table) -> Table:
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

    # Get CPI from WDI.
    tb_cpi = tb_wdi[tb_wdi["country"] == "United States"][["year", "fp_cpi_totl"]].reset_index(drop=True)
    # Get the value of CPI for the base year.
    cpi_base_value = tb_cpi[tb_cpi["year"] == CPI_BASE_YEAR]["fp_cpi_totl"].item()
    # Create an adjustment factor.
    tb_cpi["cpi_adjustment_factor"] = cpi_base_value / tb_cpi["fp_cpi_totl"]
    # Add CPI column to main table.
    tb = tb.merge(tb_cpi[["year", "cpi_adjustment_factor"]], on=["year"], how="left")
    # Multiply the cost of a healthy diet (given in current PPP$) by the adjustment factor, to correct for inflation
    # and express the values in constant 2021 PPP$.
    tb["cost_of_a_healthy_diet"] *= tb["cpi_adjustment_factor"]
    # Drop unnecessary column.
    tb = tb.drop(columns=["cpi_adjustment_factor"], errors="raise")

    return tb


def change_attribution(tb: Table, columns: List[str], attribution_text: str) -> Table:
    """
    Change attribution for some indicators as suggested by authors.
    """
    for col in columns:
        tb[col].m.origins[0].attribution = attribution_text

    return tb


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset and read its main table.
    ds_meadow = paths.load_dataset("food_prices_for_nutrition")
    tb = ds_meadow.read("food_prices_for_nutrition")

    # Load the World Development Indicators (WDI) dataset to get the U.S. Consumer Price Index (CPI),
    # which will be used to correct for inflation and express costs in constant 2021 PPP$.
    ds_wdi = paths.load_dataset("wdi")
    tb_wdi = ds_wdi.read("wdi")

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

    # Adapt units and correct for inflation.
    tb = adapt_units(tb=tb, tb_wdi=tb_wdi)

    # Change attributions for share and number of people who cannot afford a healthy diet.
    tb = change_attribution(
        tb=tb,
        columns=[
            "people_who_cannot_afford_a_healthy_diet",
            "percent_of_the_population_who_cannot_afford_a_healthy_diet",
        ],
        attribution_text=ATTRIBUTION_CANNOT_AFFORD,
    )

    # Remove attributions for cost_of_an_energy_sufficient_diet and cost_of_a_nutrient_adequate_diet
    tb = change_attribution(
        tb=tb,
        columns=[
            "cost_of_an_energy_sufficient_diet",
            "cost_of_a_nutrient_adequate_diet",
        ],
        attribution_text=None,
    )

    # Set an appropriate index and sort conveniently.
    tb = tb.format()

    #
    # Save outputs.
    #
    # Create a new garden dataset.
    ds_garden = create_dataset(dest_dir, tables=[tb], check_variables_metadata=True)
    ds_garden.save()
