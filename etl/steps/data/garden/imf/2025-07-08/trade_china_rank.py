"""Load a meadow dataset and create a garden dataset."""

import owid.catalog.processing as pr
import shared as sh
from owid.catalog import Table

from etl.data_helpers import geo
from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

REGIONS_OF_INTEREST = ["European Union (IMF)", "Europe", "China", "United States", "Asia"]
REGIONS_OWID = ["North America", "South America", "Europe", "Africa", "Asia", "Oceania"]
# Column name constants
EXPORT_COL = "Exports of goods, Free on board (FOB), US dollar"
IMPORT_COL = "Imports of goods, Cost insurance freight (CIF), US dollar"


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

            # Categorize ranking
            if target_rank <= 3:
                ranking_category = "top_3"
            elif target_rank <= 5:
                ranking_category = "top_5"
            else:
                ranking_category = "lower"

            # Create column names based on target country
            country_prefix = target_country.lower().replace(" ", "_")

            ranking_results.append(
                {
                    "country": country,
                    "year": year,
                    f"{country_prefix}_import_rank": target_rank,
                }
            )

    # Convert to Table
    ranking_tb = Table(ranking_results).copy_metadata(tb)
    for col in ranking_tb.columns:
        if col not in ["country", "year"]:
            ranking_tb[col] = ranking_tb[col].copy_metadata(tb["value"])
    return ranking_tb


def get_china_import_ranking(tb: Table) -> Table:
    """
    Analyze China's ranking as an importer for each country.
    Returns a table with China's ranking (top 3, top 5, or lower) for each country-year.
    """
    return get_country_import_ranking(tb, "China")


def get_us_import_ranking(tb: Table) -> Table:
    """
    Analyze United States' ranking as an importer for each country.
    Returns a table with US ranking (top 3, top 5, or lower) for each country-year.
    """
    return get_country_import_ranking(tb, "United States")


def run() -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("trade")
    ds_regions = paths.load_dataset("regions")

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
    regions_without_world = [region for region in REGIONS_OWID if region != "World"]
    # Define member countries for each OWID region, excluding "World".
    members = set()
    for region in regions_without_world:
        members.update(geo.list_members_of_region(region=region, ds_regions=ds_regions))

    tb_all_countries = tb[(tb["country"].isin(members)) & (tb["counterpart_country"].isin(members))]

    tb_china = get_country_import_ranking(tb_all_countries, "China")
    tb_us = get_country_import_ranking(tb_all_countries, "United States")
    tb = tb_us.merge(tb_china, on=["country", "year"], how="left")

    # Improve table format.
    tb = tb.format(["country", "year"])
    # Save outputs.
    #
    # Initialize a new garden dataset.
    ds_garden = paths.create_dataset(tables=[tb], default_metadata=ds_meadow.metadata)

    # Save garden dataset.
    ds_garden.save()
