"""Load a meadow dataset and create a garden dataset."""

import owid.catalog.processing as pr
from owid.catalog import Table

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("military_expenditure")
    ds_wdi = paths.load_dataset("wdi")

    # Read table from meadow dataset.
    tb_constant_usd = ds_meadow["constant_usd"].reset_index()
    tb_constant_usd_regions = ds_meadow["constant_usd_regions"].reset_index()
    tb_share_gdp = ds_meadow["share_gdp"].reset_index()
    tb_per_capita = ds_meadow["per_capita"].reset_index()
    tb_share_govt_spending = ds_meadow["share_govt_spending"].reset_index()

    tb_wdi = ds_wdi["wdi"].reset_index()

    #
    # Process data.
    #
    # Merge all these tables
    tb = pr.multi_merge(
        tables=[tb_constant_usd, tb_constant_usd_regions, tb_share_gdp, tb_per_capita, tb_share_govt_spending],
        on=["country", "year"],
        how="outer",
        short_name=paths.short_name,
    )

    tb = adjust_units(tb)
    tb = move_regional_data_to_constant_usd(tb)
    tb = geo.harmonize_countries(
        df=tb,
        countries_file=paths.country_mapping_path,
    )

    tb = calculate_milex_per_military_personnel(tb, tb_wdi)
    tb = tb.format(["country", "year"])

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=ds_meadow.metadata
    )

    # Save changes in the new garden dataset.
    ds_garden.save()


def adjust_units(tb: Table) -> Table:
    """
    Adjust units of the table. Multiply percentages by 100, monetary values by 1e6 in the case of constant_usd and bt 1e9 in the case of constant_usd_regions.
    """
    tb["share_gdp"] *= 100
    tb["share_govt_spending"] *= 100
    tb["constant_usd"] *= 1e6
    tb["constant_usd_regions"] *= 1e9

    return tb


def move_regional_data_to_constant_usd(tb: Table) -> Table:
    """
    Move regional data (constant_usd_regions) to the constant_usd column.
    """

    tb["constant_usd"] = tb["constant_usd"].combine_first(tb["constant_usd_regions"])
    tb = tb.drop(columns=["constant_usd_regions"])

    return tb


def calculate_milex_per_military_personnel(tb: Table, tb_wdi: Table) -> Table:
    """
    Calculate military expenditure per military personnel (from WDI).
    """

    tb_wdi["ms_mil_totl_p1"] = tb_wdi["ms_mil_totl_p1"].astype(float)

    # Merge the two tables
    tb = pr.merge(tb, tb_wdi[["country", "year", "ms_mil_totl_p1"]], on=["country", "year"], how="left")

    # Calculate military expenditure per military personnel
    tb["milex_per_mil_personnel"] = tb["constant_usd"] / tb["ms_mil_totl_p1"]

    # Make infinite values missing
    tb["milex_per_mil_personnel"] = tb["milex_per_mil_personnel"].replace([float("inf"), float("-inf")], float("nan"))

    # Drop columns
    tb = tb.drop(columns=["ms_mil_totl_p1"])

    return tb
