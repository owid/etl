import owid.catalog.processing as pr
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
TRADE_BALANCE_COL = "Trade balance goods, US dollar"

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
    totals = tb.groupby(["counterpart_country", "year"])[[EXPORT_COL, IMPORT_COL]].sum().reset_index()

    # Rename total columns
    totals = totals.rename(
        columns={
            EXPORT_COL: "total_exports",
            IMPORT_COL: "total_imports",
        }
    )

    # Merge totals back to main table
    tb = tb.merge(totals, on=["counterpart_country", "year"], how="left")

    # Calculate shares
    tb["exports_of_goods__free_on_board__fob__share"] = tb[EXPORT_COL] / tb["total_exports"] * 100
    tb["imports_of_goods__cost_insurance_freight__cif__share"] = tb[IMPORT_COL] / tb["total_imports"] * 100

    # Calculate trade volume and balance shares
    tb["total_trade_volume"] = tb["total_exports"] + tb["total_imports"]
    tb["bilateral_trade_volume"] = tb[EXPORT_COL] + tb[IMPORT_COL]

    tb["trade_balance_goods__share"] = (tb[TRADE_BALANCE_COL]) / tb["bilateral_trade_volume"] * 100
    tb["share_of_total_trade"] = tb["bilateral_trade_volume"] / tb["total_trade_volume"] * 100

    return tb
