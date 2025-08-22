"""Load a meadow dataset and create a garden dataset for IMF trade data focusing on Asia and China."""

import numpy as np
import owid.catalog.processing as pr
import pandas as pd
from owid.catalog import Table

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

# Column name constants
EXPORT_COL = "Exports of goods, Free on board (FOB), US dollar"
IMPORT_COL = "Imports of goods, Cost insurance freight (CIF), US dollar"


def run() -> None:
    """Main ETL function to process IMF trade data for Asia and China analysis."""
    # Load inputs
    ds_meadow = paths.load_dataset("trade")
    ds_regions = paths.load_dataset("regions")
    ds_wdi = paths.load_dataset("wdi")
    ds_population = paths.load_dataset("population")

    # Read table from meadow dataset
    tb = ds_meadow.read("trade")

    # Process data
    tb = _harmonize_countries(tb)
    # Filter to relevant countries and regions
    members = set()
    for region in REGIONS_OWID:
        members.update(geo.list_members_of_region(region=region, ds_regions=ds_regions))

    # Remove historical regions after their dissolution dates
    tb = clean_historical_overlaps(tb, country_col="country")
    tb = clean_historical_overlaps(tb, country_col="counterpart_country")
    # Calculate trading relationship shares for all countries
    tb_all_countries = tb[(tb["country"].isin(members)) & (tb["counterpart_country"].isin(members))]

    tb = tb.dropna(subset=["value"])

    # Add regional aggregates
    tb = _add_regional_aggregates(tb, ds_regions)

    tb = tb[
        (tb["country"].isin(list(members) + REGIONS_OF_INTEREST))
        & (tb["counterpart_country"].isin(REGIONS_OF_INTEREST + ["World"]))
    ]

    # Calculate trade shares and additional metrics
    tb = calculate_trade_shares_as_share_world(tb)
    tb.loc[tb["country"] == tb["counterpart_country"], "counterpart_country"] = "Intraregional"
    tb = add_china_imports_share_of_gdp(tb, ds_wdi, ds_population)

    trading_partners = calculate_trade_relationship_shares(tb_all_countries)
    trading_partners["country"] = "World"

    # Select relevant columns and merge additional data
    tb = tb[
        [
            "country",
            "year",
            "counterpart_country",
            "exports_of_goods__free_on_board__fob__share",
            "imports_of_goods__cost_insurance_freight__cif__share",
            "share_of_total_trade",
            "china_imports_share_of_gdp",
            EXPORT_COL,
            IMPORT_COL,
        ]
    ].copy()
    tb = pr.concat([tb, trading_partners], ignore_index=True)

    # Add import rankings for China and US
    tb_china = get_country_import_ranking(tb_all_countries, "China")
    tb_us = get_country_import_ranking(tb_all_countries, "United States")

    tb_china_us = pr.concat([tb_us, tb_china], ignore_index=True)
    tb = pr.merge(tb, tb_china_us, on=["country", "year", "counterpart_country"], how="outer")

    # Format and save dataset
    tb = tb.format(["country", "year", "counterpart_country"])
    ds_garden = paths.create_dataset(tables=[tb], default_metadata=ds_meadow.metadata)
    ds_garden.save()


def _harmonize_countries(tb: Table) -> Table:
    """Harmonize country names for both country and counterpart_country columns."""

    tb = geo.harmonize_countries(
        df=tb, countries_file=paths.country_mapping_path, excluded_countries_file=paths.excluded_countries_path
    )
    tb = geo.harmonize_countries(
        df=tb,
        country_col="counterpart_country",
        countries_file=paths.country_mapping_path,
        excluded_countries_file=paths.excluded_countries_path,
    )
    return tb


def _add_regional_aggregates(tb: Table, ds_regions) -> Table:
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
    asia_minus_china_members = [country for country in asia_members if country != "China"]

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


def calculate_trade_shares_as_share_world(tb: Table) -> Table:
    """Calculate trade shares for a given table, using the 'World' rows as totals."""

    tb = tb.pivot(
        index=["country", "year", "counterpart_country"],
        columns="indicator",
        values="value",
    ).reset_index()
    # Convert from millions-of-dollars units to actual dollars
    tb[EXPORT_COL] = tb[EXPORT_COL] * 1_000_000
    tb[IMPORT_COL] = tb[IMPORT_COL] * 1_000_000

    # 1) Extract the world‐total rows and rename their export/import columns
    world_totals = (
        tb[tb["counterpart_country"] == "World"]
        .loc[:, ["country", "year", EXPORT_COL, IMPORT_COL]]
        .rename(
            columns={
                EXPORT_COL: "total_exports",
                IMPORT_COL: "total_imports",
            }
        )
    )

    # Remove world totals since we're not using them
    tb = tb[tb["counterpart_country"] != "World"]
    tb = tb.merge(world_totals, on=["country", "year"], how="left")

    # 3) Now compute shares exactly as before
    tb["exports_of_goods__free_on_board__fob__share"] = tb[EXPORT_COL] / tb["total_exports"] * 100
    tb["imports_of_goods__cost_insurance_freight__cif__share"] = tb[IMPORT_COL] / tb["total_imports"] * 100
    tb["total_trade_volume"] = tb["total_exports"] + tb["total_imports"]
    tb["bilateral_trade_volume"] = tb[EXPORT_COL] + tb[IMPORT_COL]

    tb["share_of_total_trade"] = tb["bilateral_trade_volume"] / tb["total_trade_volume"] * 100

    # Verify that shares now sum to approximately 100%
    _verify_shares_sum_to_100(tb)

    return tb


def _verify_shares_sum_to_100(tb: Table) -> None:
    """Verify that trade shares sum to approximately 100% for each country-year using assertions."""

    # Expected counterpart regions after deduplication
    expected_counterparts = {
        "China",
        "Asia (excl. China)",
        "Europe",
        "North America",
        "South America",
        "Africa",
        "Oceania",
    }

    # Check all countries in the data
    all_countries = tb["country"].unique()
    tolerance = 1e-10  # Very small tolerance for floating point precision errors

    for country in all_countries:
        country_data = tb[tb["country"] == country]

        # Check all years for this country
        for year in country_data["year"].unique():
            year_data = country_data[country_data["year"] == year]

            actual_counterparts = set(year_data["counterpart_country"].unique())

            # Special case: China should have "Asia" instead of "Asia (excl. China)"
            if country == "China":
                expected_for_china = expected_counterparts.copy()
                expected_for_china.remove("Asia (excl. China)")
                expected_for_china.add("Asia")
                expected_set = expected_for_china
            else:
                expected_set = expected_counterparts

            # Only verify shares for countries with complete regional coverage
            if actual_counterparts == expected_set:
                export_sum = year_data["exports_of_goods__free_on_board__fob__share"].sum()
                import_sum = year_data["imports_of_goods__cost_insurance_freight__cif__share"].sum()

                # Skip validation if all values are zero/null (indicates missing data)
                if export_sum > 0:
                    assert abs(export_sum - 100) <= tolerance, (
                        f"Export shares for {country} in {year} sum to {export_sum:.2f}% "
                        f"(expected 100% ± {tolerance}%). Complete regional coverage confirmed."
                    )

                if import_sum > 0:
                    assert abs(import_sum - 100) <= tolerance, (
                        f"Import shares for {country} in {year} sum to {import_sum:.2f}% "
                        f"(expected 100% ± {tolerance}%). Complete regional coverage confirmed."
                    )
            else:
                # Just warn about incomplete coverage, don't assert
                missing = expected_set - actual_counterparts
                extra = actual_counterparts - expected_set
                if missing or extra:
                    print(f"⚠️  {country} {year}: Incomplete regional coverage.")
                    if missing:
                        print(f"   Missing: {missing}")
                    if extra:
                        print(f"   Extra: {extra}")


def add_china_imports_share_of_gdp(tb: Table, ds_wdi, ds_population) -> Table:
    """Add China imports as share of GDP for all countries."""

    # Load GDP per capita and population data
    tb_gdp_pc = ds_wdi.read("wdi")
    tb_population = ds_population.read("population")
    # Filter for GDP per capita indicator
    tb_gdp_pc = tb_gdp_pc[["country", "year", "ny_gdp_pcap_cd"]].copy()

    # Filter population data
    tb_population = tb_population[["country", "year", "population"]].copy()

    # Calculate total GDP = GDP per capita * population
    gdp_data = tb_gdp_pc.merge(tb_population, on=["country", "year"], how="inner")
    gdp_data["gdp_total"] = gdp_data["ny_gdp_pcap_cd"] * gdp_data["population"]
    gdp_data = gdp_data[["country", "year", "gdp_total"]].copy()
    # Get China imports for each country-year
    china_imports = tb[
        (tb["counterpart_country"] == "China") & (tb["imports_of_goods__cost_insurance_freight__cif__share"].notna())
    ][["country", "year", IMPORT_COL]].copy()
    china_imports = china_imports.rename(columns={IMPORT_COL: "china_imports_value"})

    # Merge GDP data with China imports
    china_gdp_data = gdp_data.merge(china_imports, on=["country", "year"], how="left")

    # Calculate China imports as share of GDP (as percentage)
    china_gdp_data["china_imports_share_of_gdp"] = (
        china_gdp_data["china_imports_value"] / china_gdp_data["gdp_total"] * 100
    )

    # Keep only the calculated share
    china_gdp_data = china_gdp_data[["country", "year", "china_imports_share_of_gdp"]]
    china_gdp_data["counterpart_country"] = "China"

    # Merge back to main table
    tb = tb.merge(china_gdp_data, on=["country", "year", "counterpart_country"], how="left")
    return tb


def calculate_trade_relationship_shares(tb: Table) -> Table:
    THRESHOLD = 0.01  # million USD
    # 1. Filter to just export/import, mark “has_trade”
    df = tb[tb.indicator.isin([EXPORT_COL, IMPORT_COL])].assign(has_trade=lambda d: d.value.fillna(0) > THRESHOLD)

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
    return shares


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


def get_country_import_ranking(tb: Table, target_country: str) -> Table:
    """
    Analyze a target country's ranking as an importer for each country.
    Returns a table with the target country's ranking (top 3, top 5, or lower) for each country-year.

    Args:
        tb: Trade data table
        target_country: Country to analyze rankings for (e.g., "China", "United States")
    """
    # Filter for import data only
    import_data = tb[tb["indicator"] == IMPORT_COL].copy()

    # Create ranking table
    ranking_results = []

    # Group by country and year to rank importers
    for (country, year), group in import_data.groupby(["country", "year"]):
        # Sort by import value descending to get rankings
        ranked_partners = group.sort_values("value", ascending=False).reset_index(drop=True)

        # Find target country's position
        target_row = ranked_partners[ranked_partners["counterpart_country"] == target_country]

        if not target_row.empty:
            target_rank = target_row.index[0] + 1  # +1 because index is 0-based
            ranking_results.append(
                {
                    "country": country,
                    "year": year,
                    "import_rank": target_rank,
                }
            )

    # Convert to Table
    ranking_tb = Table(ranking_results).copy_metadata(tb)
    for col in ranking_tb.columns:
        if col not in ["country", "year"]:
            ranking_tb[col] = ranking_tb[col].copy_metadata(tb["value"])

    ranking_tb["counterpart_country"] = target_country
    return ranking_tb
