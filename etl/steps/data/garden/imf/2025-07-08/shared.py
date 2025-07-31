import numpy as np
import pandas as pd
from owid.catalog import Table


def clean_historical_overlaps(tb: Table, country_col="country") -> Table:
    """
    Remove historical regions from Table after their dissolution dates and exclude members of historical regions before their dissolution dates.

    Args:
        tb: Table with country and year columns
        country_col: Name of the country column (default: 'country')

    Returns:
        Table with historical overlaps removed
    """

    # Define historical regions and their dissolution years
    # These regions were dissolved so having them in the dataset would be wrong for any regional aggregate.
    historical_dissolutions = {
        "East Germany": 1990,  # German reunification
        "USSR": 1991,  # Soviet Union dissolution
        "Yugoslavia": 1992,  # Yugoslavia breakup began
        "Czechoslovakia": 1993,  # Velvet Divorce
        "Serbia and Montenegro": 2006,  # Montenegro independence
        "Netherlands Antilles": 2010,  # Dissolution of Netherlands Antilles
    }

    # Define countries that were part of historical regions and their dissolution years.
    # These need to be excluded. Numbers for these member countries are the same as the historical region they were part of before their dissolution which isn't informative.
    exclude_members_of_historical_regions = {
        "Germany": 1990,  # Germany was not a country before East Germany reunified with West Germany.
        "Latvia": 1991,  # Latvia declared independence from USSR
        "Aruba": 2010,  # Aruba became a separate country within the Kingdom of the Netherlands
        "Serbia": 2006,  # Serbia declared independence from State Union of Serbia and Montenegro
        "Montenegro": 2006,  # Montenegro declared independence from State Union of Serbia and Montenegro
        "Curacao": 2010,  # Curacao became a separate country within the Kingdom of the Netherlands
        "Sint Maarten (Dutch part)": 2010,  # Sint Maarten became a separate country within the Kingdom of the Netherlands
        "Czechia": 1993,  # Czech Republic was part of Czechoslovakia until its dissolution
        "Slovakia": 1993,  # Slovakia was part of Czechoslovakia until its dissolution
    }

    tb_cleaned = tb.copy()

    # Remove historical regions after their dissolution dates
    for historical_region, dissolution_year in historical_dissolutions.items():
        mask = (tb_cleaned[country_col] == historical_region) & (tb_cleaned["year"] >= dissolution_year)
        tb_cleaned = tb_cleaned[~mask]
    # Remove independent countries that were part of historical regions before their dissolution dates
    for country, year in exclude_members_of_historical_regions.items():
        mask = (tb_cleaned[country_col] == country) & (tb_cleaned["year"] < year)
        tb_cleaned = tb_cleaned[~mask]

    return tb_cleaned


# Column name constants
EXPORT_COL = "Exports of goods, Free on board (FOB), US dollar"
IMPORT_COL = "Imports of goods, Cost insurance freight (CIF), US dollar"

SHARE_COLUMNS = [
    "country",
    "year",
    "counterpart_country",
    "exports_of_goods__free_on_board__fob__share",
    "imports_of_goods__cost_insurance_freight__cif__share",
    "trade_balance_goods__share",
    "share_of_total_trade",
]


def process_table_subset(tb: Table) -> Table:
    """Process a subset of the table."""
    tb = tb.pivot(
        index=["country", "year", "counterpart_country"],
        columns="indicator",
        values="value",
    ).reset_index()

    tb = calculate_trade_shares(tb)

    # Select only needed columns
    tb = tb[SHARE_COLUMNS]

    return tb


def calculate_trade_shares(tb: Table) -> Table:
    """Calculate trade shares for a given table."""
    # Calculate total trade for each country-year to compute shares
    totals = tb.groupby(["country", "year"])[[EXPORT_COL, IMPORT_COL]].sum().reset_index()

    # Rename total columns
    totals = totals.rename(
        columns={
            EXPORT_COL: "total_exports",
            IMPORT_COL: "total_imports",
        }
    )

    # Merge totals back to main table
    tb = tb.merge(totals, on=["country", "year"], how="left")

    # Calculate shares
    tb["exports_of_goods__free_on_board__fob__share"] = tb[EXPORT_COL] / tb["total_exports"] * 100
    tb["imports_of_goods__cost_insurance_freight__cif__share"] = tb[IMPORT_COL] / tb["total_imports"] * 100

    # Calculate trade volume and balance shares
    tb["total_trade_volume"] = tb["total_exports"] + tb["total_imports"]
    tb["bilateral_trade_volume"] = tb[EXPORT_COL] + tb[IMPORT_COL]

    tb["trade_balance_goods__share"] = (tb[EXPORT_COL] - tb[IMPORT_COL]) / tb["bilateral_trade_volume"] * 100
    tb["share_of_total_trade"] = tb["bilateral_trade_volume"] / tb["total_trade_volume"] * 100

    return tb


def calculate_trade_relationship_shares(tb: Table) -> Table:
    EXPORT = "Exports of goods, Free on board (FOB), US dollar"
    IMPORT = "Imports of goods, Cost insurance freight (CIF), US dollar"
    THRESHOLD = 0.01  # million USD

    # 1. Filter to just export/import, mark “has_trade”
    df = tb[tb.indicator.isin([EXPORT, IMPORT])].assign(has_trade=lambda d: d.value.fillna(0) > THRESHOLD)

    # 2. Create a symmetric “pair” column by sorting the two country names
    #    This uses vectorized minimum/maximum on object‑dtypes:
    c1 = df[["country", "counterpart_country"]]
    df["pair"] = pd.Series(
        np.where(
            c1.country.values < c1.counterpart_country.values,
            c1.country.values + "--" + c1.counterpart_country.values,
            c1.counterpart_country.values + "--" + c1.country.values,
        ),
        index=df.index,
    )

    # 3. Count how many directions have trade per (year, pair)
    active = df[df.has_trade]
    dir_counts = (
        active.groupby(["year", "pair"])["country"]
        .nunique()  # how many unique “origins” per pair-year (1 or 2)
        .reset_index(name="n_dirs")
    )

    # 4. Build full set of pairs for every year (so we include non‑trading)
    all_pairs = df[["year", "pair"]].drop_duplicates()

    status = all_pairs.merge(dir_counts, on=["year", "pair"], how="left").fillna({"n_dirs": 0})

    # 5. Classify and aggregate
    status["relationship"] = pd.cut(
        status.n_dirs, bins=[-0.1, 0.1, 1.1, 2.1], labels=["non_trading", "unilateral", "bilateral"]
    )

    counts = status.groupby(["year", "relationship"]).size().unstack(fill_value=0)

    # 6. Compute shares
    total = counts.sum(axis=1)
    shares = counts.divide(total, axis=0).multiply(100)
    shares = shares.rename(
        columns={
            "bilateral": "share_bilateral",
            "unilateral": "share_unilateral",
            "non_trading": "share_non_trading",
        }
    ).reset_index()
    for col in ["share_bilateral", "share_unilateral", "share_non_trading"]:
        shares[col] = shares[col].copy_metadata(tb["value"])

    return shares[["year", "share_bilateral", "share_unilateral", "share_non_trading"]]


def calculate_income_level_trade_shares(tb: Table) -> Table:
    """
    Calculate share of total trade that happens between different income levels.
    Assumes tb is already filtered to income groups only.

    Args:
        tb: Table with columns ['country', 'counterpart_country', 'year', 'indicator', 'value']
            where country and counterpart_country are income group names

    Returns:
        Table with columns for each income level combination share
    """
    EXPORT = "Exports of goods, Free on board (FOB), US dollar"
    THRESHOLD = 0.01  # million USD

    tb = tb.copy()

    # Filter to exports only (to avoid double counting trade flows)
    exports = tb[(tb["indicator"] == EXPORT) & (tb["value"].fillna(0) > THRESHOLD)]

    # Function to convert income group names to short labels
    def shorten_income_group(name):
        if "High-income" in name:
            return "High"
        elif "Upper-middle-income" in name:
            return "Upper middle"
        elif "Lower-middle-income" in name:
            return "Lower middle"
        elif "Low-income" in name:
            return "Low"
        else:
            return name

    # Create income level combinations with shortened names
    exports["origin_short"] = exports["country"].apply(shorten_income_group)
    exports["dest_short"] = exports["counterpart_country"].apply(shorten_income_group)
    exports["income_flow"] = exports["origin_short"] + " to " + exports["dest_short"]
    # Calculate total trade value by year and income flow
    trade_by_flow = exports.groupby(["year", "income_flow"])["value"].sum().reset_index()
    # Calculate total trade by year
    total_trade = exports.groupby("year")["value"].sum().reset_index(name="total_value")

    # Merge and calculate shares
    trade_shares = trade_by_flow.merge(total_trade, on="year")
    trade_shares["share_of_total_trade"] = (trade_shares["value"] / trade_shares["total_value"]) * 100

    trade_shares = trade_shares[["year", "income_flow", "share_of_total_trade"]]

    return trade_shares


def calculate_top_import_destination_share(tb) -> Table:
    """
    Calculate the share of imports going to the top import destination for each country.

    Args:
        tb: Table with columns ['country', 'counterpart_country', 'year', 'indicator', 'value']

    Returns:
        Table with columns ['country', 'year', 'counterpart_country', 'share_to_top_import_destination']
    """
    IMPORT = "Imports of goods, Cost insurance freight (CIF), US dollar"
    THRESHOLD = 0.01  # million USD

    # Filter to imports only and above threshold
    imports = tb[(tb["indicator"] == IMPORT) & (tb["value"].fillna(0) > THRESHOLD)].copy()

    if imports.empty:
        return Table()

    # Calculate total imports by country-year
    total_imports = imports.groupby(["country", "year"])["value"].sum().reset_index(name="total_imports")

    # Find the top import destination for each country-year
    # Handle potential ties by sorting and taking first occurrence
    imports_sorted = imports.sort_values(["country", "year", "value"], ascending=[True, True, False])
    top_destinations = (
        imports_sorted.groupby(["country", "year"])
        .first()
        .reset_index()[["country", "year", "counterpart_country", "value"]]
        .rename(columns={"value": "imports_to_top_destination"})
    )

    # Merge with total imports to calculate share
    result = top_destinations.merge(total_imports, on=["country", "year"])
    result["share_to_top_import_destination"] = (result["imports_to_top_destination"] / result["total_imports"]) * 100

    # Copy metadata from original value column
    result["share_to_top_import_destination"] = result["share_to_top_import_destination"].copy_metadata(tb["value"])
    result["counterpart_country"] = "top exporter"
    return result[["country", "year", "counterpart_country", "share_to_top_import_destination"]]


def calculate_top_export_destination_share(tb) -> Table:
    """
    Calculate the share of exports going to the top export destination for each country.

    Args:
        tb: Table with columns ['country', 'counterpart_country', 'year', 'indicator', 'value']

    Returns:
        Table with columns ['country', 'year', 'counterpart_country', 'share_to_top_export_destination']
    """
    EXPORT = "Exports of goods, Free on board (FOB), US dollar"
    THRESHOLD = 0.01  # million USD

    # Filter to exports only and above threshold
    exports = tb[(tb["indicator"] == EXPORT) & (tb["value"].fillna(0) > THRESHOLD)].copy()

    if exports.empty:
        return Table()

    # Calculate total exports by country-year
    total_exports = exports.groupby(["country", "year"])["value"].sum().reset_index(name="total_exports")

    # Find the top export destination for each country-year
    # Handle potential ties by sorting and taking first occurrence
    exports_sorted = exports.sort_values(["country", "year", "value"], ascending=[True, True, False])
    top_destinations = (
        exports_sorted.groupby(["country", "year"])
        .first()
        .reset_index()[["country", "year", "counterpart_country", "value"]]
        .rename(columns={"value": "exports_to_top_destination"})
    )

    # Merge with total exports to calculate share
    result = top_destinations.merge(total_exports, on=["country", "year"])
    result["share_to_top_export_destination"] = (result["exports_to_top_destination"] / result["total_exports"]) * 100

    # Copy metadata from original value column
    result["share_to_top_export_destination"] = result["share_to_top_export_destination"].copy_metadata(tb["value"])
    result["counterpart_country"] = "top importer"

    return result[["country", "year", "counterpart_country", "share_to_top_export_destination"]]
