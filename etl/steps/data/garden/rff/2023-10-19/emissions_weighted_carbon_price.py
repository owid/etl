"""Combine dataset on coverage of emissions with the average prices of emissions covered by an ETS or a carbon tax.

"""

from typing import Set

from owid.catalog import Table
from structlog import get_logger

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Initialize logger.
log = get_logger()

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# It may happen that the data for the most recent year is incomplete.
# If so, define the following to be last year fully informed.
# LAST_INFORMED_YEAR = 2021
LAST_INFORMED_YEAR = None

# Columns to keep from raw dataset and how to rename them.
COLUMNS = {
    "jurisdiction": "country",
    "year": "year",
    # Emissions-weighted average price on emissions covered by either a carbon tax or an ETS.
    # Weights: share of jurisdiction total CO2 emissions.
    "ecp_all_jurco2_usd_k": "price_with_tax_or_ets_weighted_by_share_of_co2",
    # Emissions-weighted average price on emissions covered by either a carbon tax or an ETS.
    # Weights: share of jurisdiction total GHG emissions.
    "ecp_all_jurghg_usd_k": "price_with_tax_or_ets_weighted_by_share_of_ghg",
    # Emissions-weighted average price on emissions covered by an ETS.
    # Weights: share of jurisdiction total CO2 emissions.
    "ecp_ets_jurco2_usd_k": "price_with_ets_weighted_by_share_of_co2",
    # Emissions-weighted average price on emissions covered by an ETS.
    # Weights: share of jurisdiction total GHG emissions.
    "ecp_ets_jurghg_usd_k": "price_with_ets_weighted_by_share_of_ghg",
    # Emissions-weighted average price on emissions covered by a carbon tax.
    # Weights: share of jurisdiction total CO2 emissions.
    "ecp_tax_jurco2_usd_k": "price_with_tax_weighted_by_share_of_co2",
    # Emissions-weighted average price on emissions covered by a carbon tax.
    # Weights: share of jurisdiction total GHG emissions.
    "ecp_tax_jurghg_usd_k": "price_with_tax_weighted_by_share_of_ghg",
    # CO2 emissions covered by either a carbon tax or an ETS as a share of jurisdiction total CO2 emissions.
    "cov_all_co2_jurco2": "co2_with_tax_or_ets_as_share_of_co2",
    # CO2 emissions covered by either a carbon tax or an ETS as a share of jurisdiction total GHG emissions.
    "cov_all_co2_jurghg": "co2_with_tax_or_ets_as_share_of_ghg",
    # CO2 emissions covered by either carbon taxes or an ETS as a share of world total CO2 emissions.
    "cov_all_co2_wldco2": "co2_with_tax_or_ets_as_share_of_world_co2",
    # CO2 emissions covered by either carbon taxes or an ETS as a share of world total GHG emissions.
    "cov_all_co2_wldghg": "co2_with_tax_or_ets_as_share_of_world_ghg",
    # CO2 emissions covered by an ETS as a share of jurisdiction total CO2 emissions.
    "cov_ets_co2_jurco2": "co2_with_ets_as_share_of_co2",
    # CO2 emissions covered by an ETS as a share of jurisdiction total GHG emissions.
    "cov_ets_co2_jurghg": "co2_with_ets_as_share_of_ghg",
    # CO2 emissions covered by an ETS as a share of world total CO2 emissions.
    "cov_ets_co2_wldco2": "co2_with_ets_as_share_of_world_co2",
    # CO2 emissions covered by an ETS as a share of world total GHG emissions.
    "cov_ets_co2_wldghg": "co2_with_ets_as_share_of_world_ghg",
    # CO2 emissions covered by a carbon tax as a share of jurisdiction total CO2 emissions.
    "cov_tax_co2_jurco2": "co2_with_tax_as_share_of_co2",
    # CO2 emissions covered by a carbon tax as a share of jurisdiction total GHG emissions.
    "cov_tax_co2_jurghg": "co2_with_tax_as_share_of_ghg",
    # CO2 emissions covered by a carbon tax as a share of world total CO2 emissions.
    "cov_tax_co2_wldco2": "co2_with_tax_as_share_of_world_co2",
    # CO2 emissions covered by a carbon tax as a share of world total GHG emissions.
    "cov_tax_co2_wldghg": "co2_with_tax_as_share_of_world_ghg",
    # # Other variables that are only relevant when considering sub-country regions (that we ignore for now):
    # # Emissions-weighted average price on emissions covered by either a carbon tax or an ETS.
    # # Weights: share of national jurisdiction total CO2 emissions.
    # 'ecp_all_supraco2_usd_k': 'price_with_tax_or_ets_weighted_by_share_of_country_co2',
    # # Emissions-weighted average price on emissions covered by either a carbon tax or an ETS.
    # # Weights: share of national jurisdiction total GHG emissions.
    # 'ecp_all_supraghg_usd_k': 'price_with_tax_or_ets_weighted_by_share_of_country_ghg',
    # # Emissions-weighted average price on emissions covered by an ETS.
    # # Weights: share of national jurisdiction total CO2 emissions.
    # 'ecp_ets_supraco2_usd_k': 'price_with_ets_weighted_by_share_of_country_co2',
    # # Emissions-weighted average price on emissions covered by an ETS.
    # # Weights: share of national jurisdiction total GHG emissions.
    # 'ecp_ets_supraghg_usd_k': 'price_with_ets_weighted_by_share_of_country_ghg',
    # # Emissions-weighted average price on emissions covered by a carbon tax.
    # # Weights: share of national jurisdiction total CO2 emissions.
    # 'ecp_tax_supraco2_usd_k': 'price_with_tax_weighted_by_share_of_country_co2',
    # # Emissions-weighted average price on emissions covered by a carbon tax.
    # # Weights: share of national jurisdiction total GHG emissions.
    # 'ecp_tax_supraghg_usd_k': 'price_with_tax_weighted_by_share_of_country_ghg',
    # # CO2 emissions covered by either carbon taxes or an ETS as a share of national jurisdiction CO2 emissions.
    # 'cov_all_co2_supraco2': 'co2_with_tax_or_ets_as_share_of_country_co2',
    # # CO2 emissions covered by either carbon taxes or an ETS as a share of national jurisdiction GHG emissions.
    # 'cov_all_co2_supraghg': 'co2_with_tax_or_ets_as_share_of_country_ghg',
    # # CO2 emissions covered by an ETS as a share of national jurisdiction total CO2 emissions.
    # 'cov_ets_co2_supraco2': 'co2_with_ets_as_share_of_country_co2',
    # # CO2 emissions covered by an ETS as a share of national jurisdiction total GHG emissions.
    # 'cov_ets_co2_supraghg': 'co2_with_ets_as_share_of_country_ghg',
    # # CO2 emissions covered by a carbon tax as a share of national jurisdiction total CO2 emissions.
    # 'cov_tax_co2_supraco2': 'co2_with_tax_as_share_of_country_co2',
    # # CO2 emissions covered by a carbon tax as a share of national jurisdiction total GHG emissions.
    # 'cov_tax_co2_supraghg': 'co2_with_tax_as_share_of_country_ghg',
}


def run_sanity_checks_on_inputs(tb_economy: Table, tb_coverage: Table) -> None:
    """Sanity checks on the raw data from meadow.

    Parameters
    ----------
    tb_economy : Table
        Raw data from meadow on prices.
    tb_coverage : Table
        Raw data from meadow on coverage.

    """
    error = "Both tables were expected to have the same jurisdictions (although this may not be necessary)."
    assert set(tb_economy["jurisdiction"]) == set(tb_coverage["jurisdiction"]), error
    error = "Coverage should have the same (or less) years than economy (current year may be missing in coverage)."
    assert set(tb_coverage["year"]) <= set(tb_economy["year"]), error

    # If the last year in the data is the current year, or if the data for the last year is missing, raise a warning.
    for tb in [tb_economy, tb_coverage]:
        column = tb.columns[2]
        if (
            tb["year"].max() == int(paths.version.split("-")[0])
            or tb[["year", column]].groupby(["year"], observed=True).sum(min_count=1)[column].isnull().iloc[-1]
        ):
            log.warning("The last year in the data may be incomplete. Define LAST_INFORMED_YEAR.")


def run_sanity_checks_on_outputs(tb_combined: Table, expected_countries_dropping_taxes: Set) -> None:
    """Sanity checks on the output table.

    Parameters
    ----------
    tb_combined : Table
        Output table

    """
    error = "There should be no columns with only nans."
    assert tb_combined.columns[tb_combined.isna().all()].empty, error
    error = "Country named 'World' should be included in the countries file."
    assert "World" in set(tb_combined["country"]), error

    # Warn if any country suddenly drops its carbon prices to zero, which may be spurious.
    countries_to_inspect_for_any_carbon_mechanism = []
    for column in tb_combined.drop(columns=["country", "year"]).columns:
        countries_without_taxes_now = set(
            tb_combined[(tb_combined["year"] == tb_combined["year"].max()) & (tb_combined[column] == 0)]["country"]
        )
        countries_that_had_taxes = set(
            tb_combined[(tb_combined["year"] == (tb_combined["year"].max() - 1)) & (tb_combined[column] > 0)]["country"]
        )
        countries_to_inspect = countries_without_taxes_now & countries_that_had_taxes
        if len(countries_without_taxes_now & countries_that_had_taxes - expected_countries_dropping_taxes) > 0:
            log.warning(
                f"Some countries unexpectedly dropped '{column}' to zero in the last year, inspect them: "
                f"{countries_to_inspect}"
            )
        countries_to_inspect_for_any_carbon_mechanism += list(countries_to_inspect)
    # Check if the list of countries to inspect has changed.
    if set(countries_to_inspect_for_any_carbon_mechanism) != expected_countries_dropping_taxes:
        log.warning(
            "The list of countries that dropped their carbon prices to zero in the last year has changed."
            "Remove temporary solution where spurious zeros are removed."
        )


def run(dest_dir: str) -> None:
    #
    # Load data.
    #
    # Load dataset from meadow and read its main tables.
    ds_meadow = paths.load_dataset("emissions_weighted_carbon_price")
    tb_economy = ds_meadow["emissions_weighted_carbon_price_economy"].reset_index()
    tb_coverage = ds_meadow["emissions_weighted_carbon_price_coverage"].reset_index()

    #
    # Process data.
    #
    # Sanity checks on raw data.
    run_sanity_checks_on_inputs(tb_economy=tb_economy, tb_coverage=tb_coverage)

    # Convert all values in coverage to percentages (instead of fractions).
    tb_coverage.loc[:, [column for column in tb_coverage.columns if column not in ["jurisdiction", "year"]]] *= 100

    # Combine both tables.
    tb_combined = tb_economy.merge(tb_coverage, how="outer", on=["jurisdiction", "year"], short_name=paths.short_name)

    # Select and rename columns.
    tb_combined = tb_combined[list(COLUMNS)].rename(columns=COLUMNS, errors="raise")

    # Harmonize country names.
    # NOTE: In the file of excluded countries we add all sub-national regions. This way, if an actual country is added
    # or removed, we will be warned. But, if many sub-national regions are added and including them in the excluded
    # countries file becomes a problem, we can remove that file and impose below make_missing_countries_nan=True, and
    # drop nans.
    tb_combined = geo.harmonize_countries(
        df=tb_combined,
        countries_file=paths.country_mapping_path,
        excluded_countries_file=paths.excluded_countries_path,
        warn_on_unused_countries=True,
        warn_on_missing_countries=True,
    )

    # Remove sub-regions within a country.
    tb_combined = tb_combined.dropna(subset=["country"]).reset_index(drop=True)

    if LAST_INFORMED_YEAR is not None:
        # Keep only data points prior to (or at) a certain year.
        tb_combined = tb_combined[tb_combined["year"] <= LAST_INFORMED_YEAR].reset_index(drop=True)

    # Sanity checks.
    # Some countries suddenly dropped their carbon mechanisms to zero.
    # expected_countries_dropping_taxes = set()
    expected_countries_dropping_taxes = {"Kazakhstan", "Denmark", "Norway", "Iceland"}
    run_sanity_checks_on_outputs(tb_combined, expected_countries_dropping_taxes=expected_countries_dropping_taxes)
    ####################################################################################################################
    # Geoffroy Dolphin confirmed that Denmark, Norway and Iceland drop to zero in 2021 is spurious.
    # I will remove those points.
    # Kazakhstan is not spurious (I did not ask about this one, but looking at the data it is not the first time it is
    # zero in recent years).
    for column in ["price_with_tax_weighted_by_share_of_co2", "price_with_tax_weighted_by_share_of_ghg"]:
        tb_combined.loc[
            (tb_combined["country"].isin(["Denmark", "Iceland", "Norway"])) & (tb_combined["year"] == 2021), column
        ] = None
    ####################################################################################################################

    # Set an appropriate index and sort conveniently.
    tb_combined = tb_combined.set_index(["country", "year"], verify_integrity=True).sort_index().sort_index(axis=1)

    #
    # Save outputs.
    #
    # Create a new garden dataset.
    ds_garden = create_dataset(
        dest_dir=dest_dir, tables=[tb_combined], default_metadata=ds_meadow.metadata, check_variables_metadata=True
    )
    ds_garden.save()
