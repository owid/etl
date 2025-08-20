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

# All costs are given in constant 2021 PPP$ per person per day, except for the cost of a healthy diet,
# which is reported in current PPP$ per person per day for each year.
# To express the healthy diet costs in constant 2021 PPP$, we need to adjust for inflation.
# This is done by multiplying the cost in a given YEAR by the ratio CPI(BASE_YEAR) / CPI(YEAR).
# Base year for CPI corrections.
CPI_BASE_YEAR = 2021
# In the latest update, all prices are given in both local currency unit and "ppp dollars". However, it's not specified whether the latter mean constant 2021 PPP$, or current PPP$. Later on we confirm that they are indeed current PPP$, and convert them to constant. This is relevant, specifically, for the cost of a healthy diet (since all other diets are informed for just a single year).
# Specify the constant PPP year.
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


def adjust_currencies(tb: Table, tb_wdi: Table) -> Table:
    ####################################################################################################################
    # For Sierra Leone, LCU costs seem to be a factor of 1000 too high in the new update.
    # This may be due to the 2022 change in the value of the leone:
    # https://en.wikipedia.org/wiki/Sierra_Leonean_leone
    # As of 1 July 2022, the ISO 4217 code is SLE due to a redenomination of the old leone (SLL) at a rate of SLL 1000 to SLE 1.
    # Correct those costs by dividing by 1000.
    for column in tb.columns:
        if column.startswith("cost_of_") and ("in_local_currency_unit" in column):
            tb.loc[(tb["country"].isin(["Sierra Leone"])), column] /= 1000

    # For Liberia, the resulting cost of a healthy diet is also much higher than other countries (over 500 PPP$).
    # In this case, it seems to me that the PPP conversion factor (from https://data.worldbank.org/indicator/PA.NUS.PRVT.PP?locations=LR ) may not be given in LCU per international-$, but rather in USD per international-$.
    # To correct for this, convert those USD to LCU using a conversion factor from another WDI indicator (https://data.worldbank.org/indicator/PA.NUS.FCRF?locations=LR).
    tb_wdi.loc[(tb_wdi["country"].isin(["Liberia"])), "pa_nus_prvt_pp"] *= tb_wdi.loc[
        (tb_wdi["country"].isin(["Liberia"])), "pa_nus_fcrf"
    ]
    ####################################################################################################################

    # From WDI, get CPI.
    tb_cpi = tb_wdi[["country", "year", "fp_cpi_totl"]].reset_index(drop=True)
    # Get the value of CPI for the base year.
    cpi_base_value = tb_cpi[tb_cpi["year"] == CPI_BASE_YEAR][["country", "fp_cpi_totl"]].dropna().reset_index(drop=True)
    # Create an adjustment factor for each country.
    tb_cpi = tb_cpi.merge(cpi_base_value, on=["country"], how="left", suffixes=("", "_base"))
    tb_cpi["cpi_adjustment_factor"] = tb_cpi["fp_cpi_totl_base"] / tb_cpi["fp_cpi_totl"]
    # Add CPI column to main table.
    tb = tb.merge(tb_cpi[["country", "year", "cpi_adjustment_factor"]], on=["country", "year"], how="left")

    # From WDI, get PPP conversion factor for private consumption.
    tb_ppp = tb_wdi[tb_wdi["year"] == PPP_YEAR][["country", "pa_nus_prvt_pp"]].dropna()
    # Add PPP conversion factors to main table.
    tb = tb.merge(tb_ppp, on="country", how="left")

    # Multiply costs given in local currency units by the adjustment factor, to correct for inflation.
    # Then convert to (constant) PPP dollars.
    for column in tb.columns:
        if "in_current_ppp_dollars" in column:
            column_lcu = column.replace("in_current_ppp_dollars", "in_local_currency_unit")
            column_constant_ppp = column.replace("in_current_ppp_dollars", "in_constant_ppp_dollars")
            tb[column_constant_ppp] = tb[column_lcu] * tb["cpi_adjustment_factor"] / tb["pa_nus_prvt_pp"]

    ####################################################################################################################
    # Sanity checks.

    # Check data coverage is as expected.
    # All cost columns are given for just a single year (as of the 2025 update, that year is 2021), except the cost of a healthy diet, which is given for multiple years.
    for column in tb.columns:
        if ("cost_of_a" in column) and ("relative" not in column):
            if "healthy" not in column:
                assert set(tb.dropna(subset=column)["year"]) == {PPP_YEAR}
            else:
                assert len(set(tb.dropna(subset=column)["year"])) > 1

    # Check that PPP dollars are as expected.
    for diet in ["a_nutrient_adequate", "an_energy_sufficient", "a_healthy"]:
        # Check that the costs originally given in the data in "ppp_dollars" coincide (within a certain percentage) with the ones calculated by me (for the specific year that is informed for all three diets).
        columns = [f"cost_of_{diet}_diet_in_constant_ppp_dollars", f"cost_of_{diet}_diet_in_current_ppp_dollars"]
        check = tb[tb["year"] == PPP_YEAR].dropna(subset=columns)
        check["pct"] = 100 * abs(check[columns[0]] - check[columns[1]]) / check[columns[0]]
        # Uncomment to inspect values:
        # check.sort_values("pct", ascending=False)[["country", "year"] + columns + ["pct"]].head(20)
        # Indeed, costs calculated in 2021 PPP$ coincide reasonably well with the ones given originally in "ppp_dollars".
        # However, this is not the case for specific countries.
        # TODO: Mention to data providers the following exceptions, where the check fails.
        assert set(check[check["pct"] > 10]["country"]) == {"Palestine"}

    # The cost of a healthy diet, however, is given for multiple years, and it seems that the cost in "ppp_dollars" corresponds to **current*** PPP$ (not constant PPP$).
    # To confirm this, convert costs in local currency units into PPP$.
    check = tb.copy()
    # Drop the old PPP conversion factors for 2021, and add PPP conversion factors for all years.
    check = check.drop(columns=["pa_nus_prvt_pp"]).merge(
        tb_wdi[["country", "year", "pa_nus_prvt_pp"]].dropna(), on=["country", "year"], how="left"
    )
    check["cost_of_a_healthy_diet_in_current_ppp_dollars_owid"] = (
        check["cost_of_a_healthy_diet_in_local_currency_unit"] / check["pa_nus_prvt_pp"]
    )
    columns = ["cost_of_a_healthy_diet_in_current_ppp_dollars_owid", "cost_of_a_healthy_diet_in_current_ppp_dollars"]
    check = check.dropna(subset=columns)
    check["pct"] = 100 * abs(check[columns[0]] - check[columns[1]]) / check[columns[0]]
    # Uncomment to inspect values.
    # check.sort_values("pct", ascending=False)[["country", "year"] + columns + ["pct"]].head(60)
    # Indeed, the cost converted from LCU into current PPP$ coincides reasonably well with the original cost in "ppp_dollars".
    # However, this is not the case for specific countries.
    # TODO: Mention to data providers the following exceptions, where the check fails.
    assert set(check[check["pct"] > 10]["country"]) == {"Palestine", "Somalia", "Zimbabwe"}
    ####################################################################################################################

    # For the PPP year, given that current and constant costs should be identical, fill nans in constant PPP costs with current PPP costs.
    # NOTE: This recovers data points that were lost during the conversion from LCU to constant PPP$.
    for column in tb.columns:
        if "in_constant_ppp_dollars" in column:
            column_current_ppp = column.replace("in_constant_ppp_dollars", "in_current_ppp_dollars")
            _mask = tb["year"] == PPP_YEAR
            tb.loc[_mask, column] = tb[_mask][column].fillna(tb[_mask][column_current_ppp])

    # For the cost of a healthy diet (for which we have data for multiple years) of income groups and the World, we only have data in current PPP$. This happens because there is no local currency for those regions.
    # So, to avoid missing all that data, use the US CPI and convert current PPP dollars to constant PPP dollars.
    # Get CPI from WDI.
    tb_cpi = tb_wdi[tb_wdi["country"] == "United States"][["year", "fp_cpi_totl"]].reset_index(drop=True)
    # Get the value of CPI for the base year.
    cpi_base_value = tb_cpi[tb_cpi["year"] == CPI_BASE_YEAR]["fp_cpi_totl"].item()
    # Create an adjustment factor.
    tb_cpi["cpi_adjustment_factor_us"] = cpi_base_value / tb_cpi["fp_cpi_totl"]
    # Add CPI column to main table.
    tb = tb.merge(tb_cpi[["year", "cpi_adjustment_factor_us"]], on=["year"], how="left")
    # For income groups and the World, multiply the cost of a healthy diet (given in current PPP$) by the adjustment factor, to correct for inflation, and express the values in constant 2021 PPP$.
    _mask = tb["country"].isin(
        [
            "Low-income countries",
            "Lower-middle-income countries",
            "Upper-middle-income countries",
            "High-income countries",
            "World",
        ]
    )
    tb.loc[_mask, "cost_of_a_healthy_diet_in_constant_ppp_dollars"] = (
        tb.loc[_mask, "cost_of_a_healthy_diet_in_current_ppp_dollars"] * tb.loc[_mask]["cpi_adjustment_factor_us"]
    )

    # Drop unnecessary columns.
    tb = tb.drop(columns=["cpi_adjustment_factor", "cpi_adjustment_factor_us", "pa_nus_prvt_pp"], errors="raise")

    return tb


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

    # Rename PPP columns to clarify their meaning.
    # NOTE: The cost of a nutrient adequate and an energy sufficient diets are given only for a single year, and therefore it is irrelevant that PPP are current PPP$. However, for the cost of a healthy diet, this distinction is relevant. We sanity check these assumptions later on.
    tb = tb.rename(
        columns={
            column: column.replace("in_ppp_dollars", "in_current_ppp_dollars")
            for column in tb.columns
            if "ppp" in column
        },
        errors="raise",
    )

    # Harmonize country names.
    tb = geo.harmonize_countries(df=tb, countries_file=paths.country_mapping_path)

    # Adapt units.
    tb = adapt_units(tb=tb)

    # Correct for inflation.
    tb = adjust_currencies(tb=tb, tb_wdi=tb_wdi)

    # TODO: Consider forward filling missing PPP correction factors (and CPI?) to avoid losing all cost data for 2024.

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
        for currency in ["in_current_ppp_dollars", "in_constant_ppp_dollars", "in_local_currency_unit"]:
            tb = change_attribution(
                tb=tb,
                columns=[f"cost_of_{diet}_diet_{currency}"],
                attribution_text=None,
            )

    # Set an appropriate index and sort conveniently.
    tb = tb.format()

    # The costs in the original data (since the 2025 update) was given in local currency unit, and in current PPP$.
    # In this step, we added costs in constant PPP$.
    # For simplicity, since we are only using costs in constant PPP$, remove other cost columns.
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
