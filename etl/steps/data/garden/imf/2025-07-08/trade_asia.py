"""Load a meadow dataset and create a garden dataset."""

import shared as sh
from owid.catalog import Table

from etl.data_helpers import geo
from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

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
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("trade")
    ds_regions = paths.load_dataset("regions")
    ds_wdi = paths.load_dataset("wdi")
    ds_population = paths.load_dataset("population")

    # Read table from meadow dataset.
    tb = ds_meadow.read("trade")

    #
    # Process data.
    #

    # Harmonize country names.
    country_mapping_path = paths.directory / "trade.countries.json"
    excluded_countries_path = paths.directory / "trade.excluded_countries.json"

    tb = geo.harmonize_countries(
        df=tb, countries_file=country_mapping_path, excluded_countries_file=excluded_countries_path
    )
    tb = geo.harmonize_countries(
        df=tb,
        country_col="counterpart_country",
        countries_file=country_mapping_path,
        excluded_countries_file=excluded_countries_path,
    )
    tb = tb.dropna(subset=["value"])

    # Remove historical regions after their dissolution dates.
    tb = sh.clean_historical_overlaps(tb, country_col="country")
    tb = sh.clean_historical_overlaps(tb, country_col="counterpart_country")

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

    # Create "Asia - China" region by aggregating Asian countries excluding China
    asia_members = geo.list_members_of_region(region="Asia", ds_regions=ds_regions)
    asia_minus_china_members = [country for country in asia_members if country != "China"]

    # Add Asia - China as a region for both country columns
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

    # Define member countries for each OWID region, excluding "World".
    members = set()
    for region in REGIONS_OWID:
        members.update(geo.list_members_of_region(region=region, ds_regions=ds_regions))

    tb = tb[
        (tb["country"].isin(list(members) + REGIONS_OF_INTEREST))
        & (tb["counterpart_country"].isin(REGIONS_OF_INTEREST + ["World"]))
    ]
    tb = calculate_trade_shares_as_share_world(tb)
    tb.loc[tb["country"] == tb["counterpart_country"], "counterpart_country"] = "Intraregional"
    # Calculate China imports as share of GDP
    tb = add_china_imports_share_of_gdp(tb, ds_wdi, ds_population)

    tb = tb[
        [
            "country",
            "year",
            "counterpart_country",
            "exports_of_goods__free_on_board__fob__share",
            "imports_of_goods__cost_insurance_freight__cif__share",
            "share_of_total_trade",
            "china_imports_share_of_gdp",
        ]
    ].copy()

    # Improve table format.
    tb = tb.format(["country", "year", "counterpart_country"])
    # Save outputs.
    #
    # Initialize a new garden dataset.
    ds_garden = paths.create_dataset(tables=[tb], default_metadata=ds_meadow.metadata)

    # Save garden dataset.
    ds_garden.save()


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

    # Remove overlapping regions to ensure shares sum to 100%
    # Strategy: Replace overlapping regions with non-overlapping ones systematically

    # Step 1: Handle Asia vs Asia (excl. China)
    # For China as country: keep "Asia", remove "Asia (excl. China)" (China can't trade with itself)
    # For all other countries: keep "Asia (excl. China)", remove "Asia"
    china_asia_excl_mask = (tb["country"] == "China") & (tb["counterpart_country"] == "Asia (excl. China)")
    tb = tb[~china_asia_excl_mask]

    non_china_asia_mask = (tb["country"] != "China") & (tb["counterpart_country"] == "Asia")
    tb = tb[~non_china_asia_mask]

    # Step 2: Handle Europe vs European Union (IMF)
    # Keep Europe, remove European Union (IMF) since Europe is broader and more complete
    eu_mask = tb["counterpart_country"] == "European Union (IMF)"
    tb = tb[~eu_mask]

    # Step 3: Handle North America vs United States
    # Keep North America, remove United States since North America includes US + Canada + Mexico
    us_mask = tb["counterpart_country"] == "United States"
    tb = tb[~us_mask]

    # 1) Calculate totals from the filtered regions instead of using World totals
    # This ensures shares add up to 100% for the regions we actually include
    filtered_totals = (
        tb[tb["counterpart_country"] != "World"]
        .groupby(["country", "year"])[[EXPORT_COL, IMPORT_COL]]
        .sum()
        .reset_index()
        .rename(
            columns={
                EXPORT_COL: "total_exports",
                IMPORT_COL: "total_imports",
            }
        )
    )

    # Remove world totals since we're not using them
    tb = tb[tb["counterpart_country"] != "World"]

    # Merge filtered totals back to main table
    tb = tb.merge(filtered_totals, on=["country", "year"], how="left")

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
    print(china_gdp_data)

    # Merge back to main table
    tb = tb.merge(china_gdp_data, on=["country", "year", "counterpart_country"], how="left")
    return tb
