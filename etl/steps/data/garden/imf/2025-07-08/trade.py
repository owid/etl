"""Load a meadow dataset and create a garden dataset for IMF trade data."""

from __future__ import annotations

import numpy as np
import owid.catalog.processing as pr
import pandas as pd
from owid.catalog import Dataset, Table

from etl.data_helpers import geo
from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Regional definitions
REGIONS_OF_INTEREST = [
    "European Union (IMF)",
    "China",
    "United States",
    "North America",
    "South America",
    "Africa",
    "Asia",
    "Europe",
    "Asia (excl. China)",
    "Oceania",
]
REGIONS_OWID = ["North America", "South America", "Europe", "Africa", "Asia", "Oceania"]

# Column name constants for imports and exports
EXPORT_COL = "Exports of goods, Free on board (FOB), US dollar"
IMPORT_COL = "Imports of goods, Cost insurance freight (CIF), US dollar"

# Historical region dissolution dates
HISTORICAL_DISSOLUTIONS = {
    "East Germany": 1990,
    "USSR": 1991,
    "Yugoslavia": 1992,
    "Czechoslovakia": 1993,
    "Serbia and Montenegro": 2006,
    "Netherlands Antilles": 2010,
}

# Countries to exclude before their independence from historical regions
EXCLUDE_MEMBERS_OF_HISTORICAL_REGIONS = {
    "Germany": 1990,
    "Latvia": 1991,
    "Aruba": 2010,
    "Serbia": 2006,
    "Montenegro": 2006,
    "Curacao": 2010,  # dataset spelling varies; harmonization usually fixes diacritics
    "Sint Maarten (Dutch part)": 2010,
    "Czechia": 1993,
    "Slovakia": 1993,
}


def run() -> None:
    # Load inputs
    ds_meadow = paths.load_dataset("trade")
    ds_regions = paths.load_dataset("regions")
    ds_wdi = paths.load_dataset("wdi")

    # Read table from meadow dataset (LONG format with 'indicator' & 'value')
    tb_long = ds_meadow.read("trade")

    # --- Harmonize & keep only the two indicators we actually use ----------------
    tb_long = _harmonize_countries(tb_long)
    tb_long = tb_long[tb_long["indicator"].isin([EXPORT_COL, IMPORT_COL])].copy()
    tb_long = tb_long.dropna(subset=["value"])

    # --- Historical overlaps: remove both on exporter side and partner side -----
    tb_long = clean_historical_overlaps(tb_long, country_col="country")
    tb_long = clean_historical_overlaps(tb_long, country_col="counterpart_country")

    # --- Member countries (for “all-countries” analyses) which exclude regional aggregates ------------------------
    members: set[str] = set()
    for region in REGIONS_OWID:
        members.update(geo.list_members_of_region(region=region, ds_regions=ds_regions))

    tb_all_countries = tb_long[
        (tb_long["country"].isin(members)) & (tb_long["counterpart_country"].isin(members))
    ].copy()
    tb_partnerships = calculate_trade_relationship_shares(tb_all_countries)

    # --- Add regional aggregates on both axes (plus Asia excl. China) -----------
    tb_long = _add_regional_aggregates(tb_long, ds_regions)

    # Keep a focused slice: countries = members or the specific regions of interest;
    # counterpart = the regions of interest + World.
    tb_long = tb_long[
        (tb_long["country"].isin(list(members) + REGIONS_OF_INTEREST))
        & (tb_long["counterpart_country"].isin(REGIONS_OF_INTEREST + ["World"]))
    ]

    # --- Compute shares vs. each country's world totals -------
    tb = calculate_trade_shares_as_share_world(tb_long)

    # Relabel self-trade as “Intraregional” *only* for regions.
    _regions_for_intraregional = set(REGIONS_OWID + ["European Union (IMF)", "Asia (excl. China)"])
    mask_intraregional = (
        tb["country"].isin(_regions_for_intraregional)
        & (tb["counterpart_country"] == tb["country"])
        & (tb["country"] != "World")
    )
    tb.loc[mask_intraregional, "counterpart_country"] = "Intraregional"

    # --- GDP --------------------------------------------------------------
    gdp_data = ds_wdi.read("wdi")[["country", "year", "ny_gdp_mktp_cd"]].copy()

    # China imports as share of GDP (attached to counterpart=World rows)
    tb = add_china_imports_share_of_gdp(tb, gdp_data)

    # Total imports as share of GDP (only for counterpart=World rows)
    tb_world = tb[(tb["country"].isin(members)) & (tb["counterpart_country"] == "World")].copy()
    total_imports = add_total_imports_share_of_gdp(tb_world, gdp_data)

    tb = pr.merge(
        tb,
        total_imports,
        on=["country", "year", "counterpart_country"],
        how="left",
    )

    # --- Top partner classification (based on imports) --------------------
    tb_top_partners = get_top_partner(tb_all_countries)
    tb = pr.merge(
        tb,
        tb_top_partners,
        on=["country", "year", "counterpart_country"],
        how="left",
    )

    # --- Importer ranking for China & US ---------------------------------
    tb_china = get_country_import_ranking(tb_all_countries, "China")
    tb_us = get_country_import_ranking(tb_all_countries, "United States")
    tb_china_us = pr.concat([tb_us, tb_china], ignore_index=True)

    tb = pr.merge(
        tb,
        tb_china_us,
        on=["country", "year", "counterpart_country"],
        how="left",
    )

    tb = pr.concat([tb, tb_partnerships], ignore_index=True)
    # --- Format, add origins & save ----------------------------------------------------
    tb = tb.format(["country", "year", "counterpart_country"])
    for column in tb.columns:
        if column in ["total_imports_share_of_gdp", "china_imports_share_of_gdp"]:
            tb[column].metadata.origins = [
                tb_long["value"].metadata.origins[0],
                gdp_data["ny_gdp_mktp_cd"].metadata.origins[0],
            ]
        else:
            tb[column].metadata.origins = tb_long["value"].metadata.origins

    ds_garden = paths.create_dataset(tables=[tb], default_metadata=ds_meadow.metadata)
    ds_garden.save()


def _harmonize_countries(tb: Table) -> Table:
    """Harmonize country names for both country and counterpart_country columns."""
    tb = geo.harmonize_countries(
        df=tb,
        countries_file=paths.country_mapping_path,
        excluded_countries_file=paths.excluded_countries_path,
        warn_on_unknown_excluded_countries=False,
    )
    tb = geo.harmonize_countries(
        df=tb,
        country_col="counterpart_country",
        countries_file=paths.country_mapping_path,
        excluded_countries_file=paths.excluded_countries_path,
        warn_on_unknown_excluded_countries=False,
    )
    return tb


def _add_regional_aggregates(tb: Table, ds_regions: Dataset) -> Table:
    """Add regional aggregates for both country and counterpart_country columns."""
    # Add standard OWID regions plus World
    tb = geo.add_regions_to_table(
        tb,
        ds_regions,
        index_columns=["country", "year", "indicator", "counterpart_country"],
        country_col="counterpart_country",
        regions=REGIONS_OWID + ["World"],
    )
    tb = geo.add_regions_to_table(
        tb,
        ds_regions,
        index_columns=["country", "year", "indicator", "counterpart_country"],
        country_col="country",
        regions=REGIONS_OWID + ["World"],
    )

    # Create "Asia (excl. China)" region
    asia_members = geo.list_members_of_region(region="Asia", ds_regions=ds_regions)
    asia_minus_china_members = [c for c in asia_members if c != "China"]

    tb = geo.add_region_aggregates(
        tb,
        region="Asia (excl. China)",
        countries_in_region=asia_minus_china_members,
        index_columns=["country", "year", "indicator", "counterpart_country"],
        country_col="counterpart_country",
    )
    tb = geo.add_region_aggregates(
        tb,
        region="Asia (excl. China)",
        countries_in_region=asia_minus_china_members,
        index_columns=["country", "year", "indicator", "counterpart_country"],
        country_col="country",
    )
    return tb


def calculate_trade_shares_as_share_world(tb_long: Table) -> Table:
    """
    Calculate trade shares for a given (LONG) table, using each country's
    counterpart='World' row as the total.

    This function:
    1. Pivots long format data to wide format (exports/imports as columns)
    2. Converts values from millions USD to USD
    3. Calculates shares by dividing bilateral trade by total trade with World
    4. Adds bilateral trade volume metrics

    Args:
        tb_long: Table in long format with 'indicator' and 'value' columns

    Returns:
        Table with share columns added and absolute values in USD (not millions)
    """
    wide = (
        tb_long.pivot_table(
            index=["country", "year", "counterpart_country"],
            columns="indicator",
            values="value",
        )
        .reset_index()
        .rename_axis(None, axis=1)
    )

    # Convert from millions of USD to USD
    if EXPORT_COL in wide:
        wide[EXPORT_COL] = wide[EXPORT_COL] * 1_000_000
    if IMPORT_COL in wide:
        wide[IMPORT_COL] = wide[IMPORT_COL] * 1_000_000

    # Totals by (country, year) from counterpart='World'
    world_totals = (
        wide[wide["counterpart_country"] == "World"]
        .loc[:, ["country", "year", EXPORT_COL, IMPORT_COL]]
        .rename(columns={EXPORT_COL: "total_exports", IMPORT_COL: "total_imports"})
    )

    # Split world vs. non-world
    non_world = wide[wide["counterpart_country"] != "World"].copy()
    world = wide[wide["counterpart_country"] == "World"].copy()

    # Attach totals to non-world rows (for shares)
    non_world = pr.merge(non_world, world_totals, on=["country", "year"], how="left")

    # Shares
    non_world["exports_of_goods__free_on_board__fob__share"] = non_world[EXPORT_COL] / non_world["total_exports"] * 100
    non_world["imports_of_goods__cost_insurance_freight__cif__share"] = (
        non_world[IMPORT_COL] / non_world["total_imports"] * 100
    )
    non_world["total_trade_volume"] = non_world["total_exports"] + non_world["total_imports"]
    non_world["bilateral_trade_volume"] = non_world[EXPORT_COL] + non_world[IMPORT_COL]
    non_world["share_of_total_trade"] = non_world["bilateral_trade_volume"] / non_world["total_trade_volume"] * 100

    # Combine back (world rows keep their level values; shares only on non-world)
    combined = pr.concat([non_world, world], ignore_index=True)
    # Drop helper columns
    combined = combined.drop(
        columns=[c for c in ["total_exports", "total_imports", "total_trade_volume"] if c in combined],
        errors="ignore",
    )
    return combined


def add_china_imports_share_of_gdp(tb: Table, gdp_data: Table) -> Table:
    """Add China imports as share of GDP (as %) for each country-year; attach to counterpart=World rows."""

    china_imports = tb[(tb["counterpart_country"] == "China")][["country", "year", IMPORT_COL]].copy()
    china_imports = china_imports.rename(columns={IMPORT_COL: "china_imports_value"})

    china_gdp = pr.merge(gdp_data, china_imports, on=["country", "year"], how="inner")
    china_gdp["china_imports_share_of_gdp"] = china_gdp["china_imports_value"] / china_gdp["ny_gdp_mktp_cd"] * 100
    china_gdp = china_gdp[["country", "year", "china_imports_share_of_gdp"]].copy()
    china_gdp["counterpart_country"] = "World"

    tb = pr.merge(
        tb,
        china_gdp,
        on=["country", "year", "counterpart_country"],
        how="left",
    )
    return tb


def add_total_imports_share_of_gdp(tb_world: Table, gdp_data: Table) -> Table:
    """Add total imports as share of GDP (as %) for counterpart=World rows."""
    total_imports = tb_world[["country", "year", IMPORT_COL]].rename(columns={IMPORT_COL: "total_imports_value"})
    total_imports = pr.merge(gdp_data, total_imports, on=["country", "year"], how="inner")
    total_imports["total_imports_share_of_gdp"] = (
        total_imports["total_imports_value"] / total_imports["ny_gdp_mktp_cd"] * 100
    )
    out = total_imports[["country", "year", "total_imports_share_of_gdp"]].copy()
    out["counterpart_country"] = "World"
    return out


def get_top_partner(tb: Table) -> Table:
    """
    Get the top trading partner (by imports) for each country-year and classify based on global frequency.

    This function:
    1. Finds the largest import partner for each country-year
    2. Counts how frequently each partner appears as #1 globally
    3. Keeps the top 10 most frequent partners, groups others as 'Other'
    4. Returns with counterpart_country='World' for easy merging

    Args:
        tb: Trade data table

    Returns:
        Table with top_partner_category column, attached to counterpart='World' rows
    """
    import_data = tb[tb["indicator"] == IMPORT_COL].dropna(subset=["value"]).copy()
    idx = import_data.groupby(["country", "year"])["value"].idxmax()
    top_df = import_data.loc[idx, ["country", "year", "counterpart_country"]].reset_index(drop=True)

    partner_counts = top_df.groupby("counterpart_country")["country"].nunique().sort_values(ascending=False)
    top_10 = set(partner_counts.head(10).index.tolist())

    top_df["top_partner_category"] = top_df["counterpart_country"].apply(lambda x: x if x in top_10 else "Other")
    out = Table(top_df[["country", "year", "top_partner_category"]]).copy_metadata(tb)
    out = out.drop_duplicates(subset=["country", "year"])  # safety

    out["counterpart_country"] = "World"
    return out


def get_country_import_ranking(tb: Table, target_country: str) -> Table:
    """
    Analyze a target country's ranking as an importer for each country.

    This function calculates where a target country (e.g., China) ranks among
    each country's import partners. Rank 1 = largest import partner.
    Uses vectorized pandas ranking instead of manual groupby loops for efficiency.

    Args:
        tb: Trade data table
        target_country: Country to analyze ranking for (e.g., "China", "United States")

    Returns:
        Table with import_rank column, filtered to target_country rows only
    """
    import_data = tb[tb["indicator"] == IMPORT_COL].copy()

    # Calculate rank using pandas ranking (1-based, descending by value)
    import_data["import_rank"] = (
        import_data.groupby(["country", "year"])["value"].rank(method="min", ascending=False).astype(int)
    )

    # Filter for target country only
    out = import_data[import_data["counterpart_country"] == target_country][["country", "year", "import_rank"]].copy()

    out = Table(out).copy_metadata(tb)
    out["counterpart_country"] = target_country
    return out


def clean_historical_overlaps(tb: Table, country_col: str = "country") -> Table:
    """
    Remove historical regions from Table after their dissolution dates and exclude members of historical regions before their dissolution dates.

    This prevents double-counting by ensuring we don't have overlapping data from:
    1. Historical regions (like USSR) after they dissolved
    2. Their member countries (like Latvia) before they became independent

    Args:
        tb: Input table with trade data
        country_col: Column name to apply the cleaning to (usually 'country' or 'counterpart_country')

    Returns:
        Cleaned table with historical overlaps removed
    """
    tb_cleaned = tb.copy()

    # Remove historical regions after their dissolution year
    # E.g., remove USSR data from 1991 onwards
    for region, diss_year in HISTORICAL_DISSOLUTIONS.items():
        tb_cleaned = tb_cleaned[~((tb_cleaned[country_col] == region) & (tb_cleaned["year"] >= diss_year))]

    # Remove member countries before they became independent
    # E.g., remove Latvia data before 1991 (when it was part of USSR)
    for country, year in EXCLUDE_MEMBERS_OF_HISTORICAL_REGIONS.items():
        tb_cleaned = tb_cleaned[~((tb_cleaned[country_col] == country) & (tb_cleaned["year"] < year))]

    return tb_cleaned


def calculate_trade_relationship_shares(tb: Table) -> Table:
    THRESHOLD = 0.01  # million USD

    df = tb[tb.indicator.isin([EXPORT_COL, IMPORT_COL])].assign(has_trade=lambda d: d.value.fillna(0) > THRESHOLD)

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

    active = df[df.has_trade]
    dir_counts = (
        active.groupby(["year", "pair"])["country"]
        .nunique()  # how many unique “origins” per pair-year (1 or 2)
        .reset_index(name="n_dirs")
    )

    all_pairs = df[["year", "pair"]].drop_duplicates()

    status = all_pairs.merge(dir_counts, on=["year", "pair"], how="left").fillna({"n_dirs": 0})

    status["relationship"] = pd.cut(
        status.n_dirs, bins=[-0.1, 0.1, 1.1, 2.1], labels=["non_trading", "unilateral", "bilateral"]
    )

    counts = status.groupby(["year", "relationship"]).size().unstack(fill_value=0)

    total = counts.sum(axis=1)
    shares = counts.divide(total, axis=0).multiply(100)
    shares = shares.rename(
        columns={
            "bilateral": "share_bilateral",
            "unilateral": "share_unilateral",
            "non_trading": "share_non_trading",
        }
    ).reset_index()

    shares["country"] = "World"

    return shares[["country", "year", "share_bilateral", "share_unilateral", "share_non_trading"]]
