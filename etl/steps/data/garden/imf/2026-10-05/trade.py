"""Load a meadow dataset and create a garden dataset for IMF trade data."""

from __future__ import annotations

import owid.catalog.processing as pr
import pandas as pd
from owid.catalog import Table, VariablePresentationMeta

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

# Categories of the importer ranking of China and the United States.
IMPORT_RANK_CATEGORIES = {"1st - Top source", "2nd", "3rd", "4th", "5th", "Not in top 5"}

# Map colors of the top-partner categories, from grapher's OWID categorical map palette (OwidMapColors). The same
# partner gets the same color on the import and export maps. China is the only strong, warm color, so keep warm hues
# (oranges, sands, cherries) away from partners whose countries can border China's.
# NOTE: The top ten partners are recomputed on every update. If a new one enters, the build fails until it gets a
# color here; pick an unused, cool one from OwidMapColors (owid-grapher, color/CustomSchemes.ts).
TOP_PARTNER_COLORS = {
    "China": "#D94C3F",  # Tomato
    "United States": "#526F9B",  # Muted denim
    "Germany": "#238A84",  # Muted teal
    "India": "#77538F",  # Soft purple
    "Italy": "#A07AB8",  # Light purple
    "France": "#5B6D35",  # Olive
    "United Kingdom": "#6FA54F",  # Leaf green
    "Russia": "#5FB8C8",  # Sky turquoise
    "Japan": "#8E97C7",  # Lavender
    "Saudi Arabia": "#D9BC54",  # Mustard
    "Netherlands": "#92D3DE",  # Light denim
    "Switzerland": "#9DCA7B",  # Light green
    "Other": "#B9B2A6",  # Taupe
}

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
    ds_wdi = paths.load_dataset("wdi")

    # Read table from meadow dataset (LONG format with 'indicator' & 'value'), keeping categoricals for the ~5M rows.
    tb_long = ds_meadow.read("trade", safe_types=False)
    sanity_check_inputs(tb_long)

    # --- Keep only the two indicators we actually use, then harmonize ----------------
    tb_long = tb_long[tb_long["indicator"].isin([EXPORT_COL, IMPORT_COL])]
    # Downstream steps rely on string dtypes (pair labels, new region labels), so convert after filtering.
    tb_long = tb_long.astype(
        {
            "country": "string[pyarrow]",
            "indicator": "string[pyarrow]",
            "counterpart_country": "string[pyarrow]",
            "year": "Int64",
            "value": "Float64",
        }
    )
    tb_long = _harmonize_countries(tb_long)

    # --- Give every series a row for every year (empty where there is no observation) ---------------
    tb_long = complete_series_years(tb_long)

    # --- Historical overlaps: remove both on exporter side and partner side -----
    tb_long = clean_historical_overlaps(tb_long, country_col="country")
    tb_long = clean_historical_overlaps(tb_long, country_col="counterpart_country")

    # --- Member countries (for “all-countries” analyses) which exclude regional aggregates ------------------------
    members: set[str] = set()
    for region in REGIONS_OWID:
        members.update(paths.regions.get_region(region)["members"])

    tb_all_countries = tb_long[
        (tb_long["country"].isin(members)) & (tb_long["counterpart_country"].isin(members))
    ].copy()

    # --- Trade relationships (bilateral, unilateral, non-trading) - needs to be calculated before NaNs are excluded   --------------
    tb_partnerships = calculate_trade_relationship_shares(tb_all_countries)

    # Drop nans in value column to avoid issues when doing certain calculations later
    tb_long = tb_long.dropna(subset=["value"])
    tb_all_countries = tb_all_countries.dropna(subset=["value"])

    # --- Add regional aggregates on both axes (plus Asia excl. China) -----------
    tb_long = _add_regional_aggregates(tb_long)

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

    # --- Top export partner classification (based on exports) -------------
    tb_top_export_partners = get_top_export_partner(tb_all_countries)
    tb = pr.merge(
        tb,
        tb_top_export_partners,
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
        how="outer",
    )

    tb = pr.concat([tb, tb_partnerships], ignore_index=True)
    sanity_check_outputs(tb)

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

    # Order and color the top-partner categories for maps: most common in the latest year first, "Other" last.
    for column in ["top_partner_category", "top_export_partner_category"]:
        order = _top_partner_order(tb, column)
        missing = set(order) - set(TOP_PARTNER_COLORS)
        assert not missing, f"Add a map color to TOP_PARTNER_COLORS for the new top partner(s): {sorted(missing)}"
        tb[column].metadata.type = "ordinal"
        tb[column].metadata.sort = order
        if tb[column].metadata.presentation is None:
            tb[column].metadata.presentation = VariablePresentationMeta()
        tb[column].metadata.presentation.grapher_config = {
            **(tb[column].metadata.presentation.grapher_config or {}),
            "map": {
                "colorScale": {
                    "baseColorScheme": "OwidCategoricalMap",
                    "customCategoryColors": {category: TOP_PARTNER_COLORS[category] for category in order},
                }
            },
        }

    # Keep float64 values, as in meadow (repacking would round totals in the trillions to float32).
    ds_garden = paths.create_dataset(tables=[tb], default_metadata=ds_meadow.metadata, repack=False)
    ds_garden.save()


def sanity_check_inputs(tb: Table) -> None:
    expected_indicators = {
        EXPORT_COL,
        IMPORT_COL,
        "Imports of goods, Free on board (FOB), US dollar",
        "Trade balance goods, US dollar",
    }
    assert set(tb["indicator"]) == expected_indicators, f"Unexpected indicators: {set(tb['indicator'])}"

    # Coverage: almost every reporter has total exports in the latest year (227 in the 2026-10-05 release).
    latest = tb[
        (tb["year"] == tb["year"].max()) & (tb["counterpart_country"] == "G001") & (tb["indicator"] == EXPORT_COL)
    ]
    assert latest["country"].nunique() >= 220, f"Only {latest['country'].nunique()} reporters in {tb['year'].max()}."

    # Magnitude: values are in US dollars, not millions (US exports of goods are in the low trillions).
    us_exports = latest.loc[latest["country"] == "USA", "value"]
    assert len(us_exports) == 1 and 1e12 < us_exports.item() < 1e13, f"Unexpected US exports: {us_exports.tolist()}"


def sanity_check_outputs(tb: Table) -> None:
    # Shares are non-negative. They can exceed 100% in some early years, where IMF partner estimates add up to more
    # than the reported total (e.g. Austria's trade with the European Union in the 1960s).
    for column in [
        "exports_of_goods__free_on_board__fob__share",
        "imports_of_goods__cost_insurance_freight__cif__share",
        "share_of_total_trade",
    ]:
        assert (tb[column].dropna() >= 0).all(), f"Negative values in {column}."

    # Trade-relationship shares add up to 100% each year.
    tb_partnerships = tb[["share_bilateral", "share_unilateral", "share_non_trading"]].dropna()
    assert not tb_partnerships.empty, "No trade-relationship shares."
    assert ((tb_partnerships.sum(axis=1) - 100).abs() < 0.01).all(), "Trade-relationship shares don't add up to 100%."

    # Categorical indicators take only the expected values.
    categories = set(tb["import_rank_category"].dropna())
    assert categories <= IMPORT_RANK_CATEGORIES, (
        f"Unexpected import rank categories: {categories - IMPORT_RANK_CATEGORIES}"
    )
    for column in ["top_partner_category", "top_export_partner_category"]:
        assert tb[column].dropna().nunique() <= 11, f"{column} should have the top 10 partners plus 'Other'."

    # Every partner of interest is present.
    missing = set(REGIONS_OF_INTEREST + ["World"]) - set(tb["counterpart_country"].dropna())
    assert not missing, f"Missing partners: {missing}"

    # Magnitude: US exports of goods to the world are in the low trillions of US dollars.
    us_exports = tb.loc[
        (tb["country"] == "United States") & (tb["counterpart_country"] == "World") & (tb["year"] == tb["year"].max()),
        EXPORT_COL,
    ]
    assert len(us_exports) == 1 and 1e12 < us_exports.item() < 1e13, f"Unexpected US exports: {us_exports.tolist()}"


def _harmonize_countries(tb: Table) -> Table:
    """Harmonize country codes for both country and counterpart_country columns."""
    tb = paths.regions.harmonize_names(tb, warn_on_unknown_excluded_countries=False)
    tb = paths.regions.harmonize_names(tb, country_col="counterpart_country", warn_on_unknown_excluded_countries=False)
    return tb


def _add_regional_aggregates(tb: Table) -> Table:
    """Add regional aggregates (OWID continents, World, and Asia excluding China) on both trade axes."""
    regions = {region: {} for region in REGIONS_OWID + ["World"]}
    regions["Asia (excl. China)"] = {"additional_regions": ["Asia"], "excluded_members": ["China"]}
    index_columns = ["country", "year", "indicator", "counterpart_country"]

    # Aggregate partners first, so that aggregating reporters also yields region-to-region trade.
    tb = paths.regions.add_aggregates(
        tb, regions=regions, index_columns=index_columns, country_col="counterpart_country"
    )
    tb = paths.regions.add_aggregates(tb, regions=regions, index_columns=index_columns, country_col="country")
    return tb


def complete_series_years(tb: Table) -> Table:
    """Give every (country, indicator, counterpart_country) series a row for every year in the data.

    The IMF API only returns observations that have a value. The trade-relationship shares count every pair of
    countries with a series as a potential partnership in every year (non-trading when there is no trade), so they need
    the empty years too. This reproduces the panel of the IMF's wide download, used in previous versions.
    """
    series = tb[["country", "indicator", "counterpart_country"]].drop_duplicates()
    years = pr.read_from_records([(year,) for year in range(tb["year"].min(), tb["year"].max() + 1)], columns=["year"])
    grid = pr.merge(series, years, how="cross")
    tb = pr.merge(grid, tb, on=["country", "indicator", "counterpart_country", "year"], how="left")

    return tb


def calculate_trade_shares_as_share_world(tb_long: Table) -> Table:
    """
    Calculate trade shares for a given (LONG) table, using each country's
    counterpart='World' row as the total.

    This function:
    1. Pivots long format data to wide format (exports/imports as columns)
    2. Calculates shares by dividing bilateral trade by total trade with World
    3. Adds bilateral trade volume metrics

    Args:
        tb_long: Table in long format with 'indicator' and 'value' columns

    Returns:
        Table with share columns added and absolute values in USD
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

    # China's imports from itself (re-imports) are not applicable here; maps mark China as such.
    china_gdp = china_gdp[china_gdp["country"] != "China"]

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


def _top_partner_order(tb: Table, column: str) -> list[str]:
    """Categories ordered by how many countries have them as top partner in the latest year (ties broken by the count
    over all years, as in the top-10 selection), with "Other" last."""
    categories = tb.reset_index()[["country", "year", column]].dropna()
    assert "Other" in set(categories[column]), f"Expected an 'Other' category in {column}."
    categories = categories[categories[column] != "Other"]
    counts = pd.DataFrame(
        {
            "latest": categories[categories["year"] == categories["year"].max()].groupby(column)["country"].nunique(),
            "all_years": categories.groupby(column)["country"].nunique(),
        }
    ).fillna(0)
    order = counts.sort_values(["latest", "all_years"], ascending=False).index.tolist()
    return order + ["Other"]


def get_top_export_partner(tb: Table) -> Table:
    """
    Get the top export destination (by exports) for each country-year and classify based on global frequency.

    This function:
    1. Finds the largest export destination for each country-year
    2. Counts how frequently each destination appears as #1 globally
    3. Keeps the top 10 most frequent destinations, groups others as 'Other'
    4. Returns with counterpart_country='World' for easy merging

    Args:
        tb: Trade data table

    Returns:
        Table with top_export_partner_category column, attached to counterpart='World' rows
    """
    export_data = tb[tb["indicator"] == EXPORT_COL].dropna(subset=["value"]).copy()
    idx = export_data.groupby(["country", "year"])["value"].idxmax()
    top_df = export_data.loc[idx, ["country", "year", "counterpart_country"]].reset_index(drop=True)

    partner_counts = top_df.groupby("counterpart_country")["country"].nunique().sort_values(ascending=False)
    top_10 = set(partner_counts.head(10).index.tolist())

    top_df["top_export_partner_category"] = top_df["counterpart_country"].apply(lambda x: x if x in top_10 else "Other")
    out = Table(top_df[["country", "year", "top_export_partner_category"]]).copy_metadata(tb)
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

    # Keep the target country's rank among each other country's partners. Its rank among its own partners (e.g.
    # China's re-imports from China) is not applicable; maps mark the target country as such.
    out = import_data[
        (import_data["counterpart_country"] == target_country) & (import_data["country"] != target_country)
    ][["country", "year", "import_rank"]].copy()

    # Add color category based on ranking
    rank_categories = {1: "1st - Top source", 2: "2nd", 3: "3rd", 4: "4th", 5: "5th"}
    out["import_rank_category"] = out["import_rank"].map(lambda rank: rank_categories.get(rank, "Not in top 5"))

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
    # NOTE: Every country pair with data in any year counts in every year from 1948, including years before one of
    # the countries existed (e.g. South Sudan before 2011). Those pairs count as non-trading, which inflates the
    # non-trading share before 2000 (by about 33 points in 1950, 12-15 points in 1960-1991, none from 2015 on). The
    # chart using these shares (share-of-bilateral-trade-partnerships) is unpublished. Before publishing it, count each
    # pair only from the year both countries exist (first year in the IMF data, or independence dates) and describe
    # the choice in the metadata.
    THRESHOLD = 10_000  # USD

    df = tb[tb.indicator.isin([EXPORT_COL, IMPORT_COL])].assign(has_trade=lambda d: d.value.fillna(0) > THRESHOLD)

    # Label each pair of countries the same way in both directions (names in alphabetical order).
    country = df["country"].astype(str)
    counterpart = df["counterpart_country"].astype(str)
    df["pair"] = counterpart + "--" + country
    country_first = country < counterpart
    df.loc[country_first, "pair"] = country[country_first] + "--" + counterpart[country_first]

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
