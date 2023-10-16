"""Load a meadow dataset and create a garden dataset."""

import owid.catalog.processing as pr
from owid.catalog import Dataset, Table
from structlog import get_logger

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)
# Logger
log = get_logger()

REGIONS = [
    "North America",
    "South America",
    "Europe",
    "European Union (27)",
    "Africa",
    "Asia",
    "Oceania",
    "Low-income countries",
    "Upper-middle-income countries",
    "Lower-middle-income countries",
    "High-income countries",
    "World",
]


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("plastic_waste")

    # Load regions dataset.
    ds_regions = paths.load_dataset("regions")
    ds_income_groups = paths.load_dataset("income_groups")

    # Read table from meadow dataset.
    tb = ds_meadow["plastic_waste"].reset_index()

    #
    # Process data.
    #

    # Keep relevant columns
    paths.log.info("keep relevant columns")

    #  Extract relevatn columns
    #    - Year ("RefYear")
    #    - Type of flow (export/import, denoted by "flowdesc")
    #    - Reporting country ("reporterDesc")
    #    - Partner country ("partnerDesc")
    #     - Quantity of plastic exported/imported ("cmddesc")
    #    - Mode of transport ("motDesc")
    COLUMNS_RELEVANT = ["refyear", "flowdesc", "reporterdesc", "partnerdesc", "qty", "motdesc"]
    tb = tb[COLUMNS_RELEVANT]

    # Rename year and country column fields
    paths.log.info("rename columns")
    tb = tb.rename(
        columns={
            "refyear": "year",
            "reporterdesc": "country",
            "flowdesc": "export_vs_import",
            "partnerdesc": "partner_country",
            "motdesc": "mode_of_transport",
        }
    )

    # Keep exports/imports to/from World only - could be interesting in the future to create flow diagrams in which case will need to change this
    tb = tb[tb["partner_country"] == "World"]
    tb = tb.drop("partner_country", axis=1)

    # Pivot to get exports and imports by each mode of transport
    tb = pr.pivot(
        tb, index=["year", "country"], columns=["export_vs_import", "mode_of_transport"], values="qty"
    ).reset_index()
    # Rename hiearachical columns after pivoting
    tb.columns = [f"{col[0]}_{col[1]}" if col[0] not in ["year", "country"] else col[0] for col in tb.columns]
    # Check that there is no intersection between former and current countries
    paths.log.info("un.comtrade: handle former countries West Germany and Sudan (former)")
    assert (
        tb[tb["country"].str.contains("Germany")].groupby("year").size().max() == 1
    ), "There are some years with data for both Germany and West Germany"
    assert (
        tb[tb["country"].str.contains("Sudan")].groupby("year").size().max() == 1
    ), "There are some years with data for both Sudan and Sudan (former)"
    # West Germany and Sudan (former) are both mapped to current countries (Germany and Sudan).
    # This is to ease the region aggregate estimations.
    # However, later, we undo this for the specific regions. That's why we need the year range.
    YEAR_GERMANY = tb.loc[tb["country"] == "Fed. Rep. of Germany (...1990)", "year"].max()
    YEAR_SUDAN = tb.loc[tb["country"] == "Sudan (...2011)", "year"].max()

    # Harmonize country names
    paths.log.info("harmonize country names")
    tb: Table = geo.harmonize_countries(
        df=tb, countries_file=paths.country_mapping_path, excluded_countries_file=paths.excluded_countries_path
    )
    tb["country"] = tb["country"].astype(str)

    # Add regions
    paths.log.info("add regions and income groups")
    tb = add_data_for_regions(tb, ds_regions, ds_income_groups)

    # Correct former country names
    paths.log.info("finish handling former countries West Germany and Sudan (former)")
    tb.loc[(tb["country"] == "Germany") & (tb["year"] <= YEAR_GERMANY), "country"] = "West Germany"
    tb.loc[(tb["country"] == "Sudan") & (tb["year"] <= YEAR_SUDAN), "country"] = "Sudan (former)"

    tb = tb[tb["country"] != "West Germany"]

    # Add net exports
    paths.log.info("add net exports")
    tb["net_export"] = tb["Export_TOTAL MOT"] - tb["Import_TOTAL MOT"]
    tb["net_export"].metadata.origins = tb["Import_TOTAL MOT"].metadata.origins

    # Add per capita metrics
    paths.log.info("add per capita")
    tb = add_per_capita_variables(tb)

    # Add share from total for total exports, imports, total exports by air and total imports by air
    paths.log.info("add share from total exports and imports")
    tb = add_share_from_total(tb)

    # Set index
    tb = tb.underscore().set_index(["year", "country"], verify_integrity=True).sort_index()

    # Convert columns that are not per capita/ share of to tonnes
    for col in tb.columns:
        if "per_capita" not in col and "share" not in col:
            tb[col] = tb[col] / 1000
    # Set table's short_name
    tb.metadata.short_name = paths.short_name

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(dest_dir, tables=[tb], default_metadata=ds_meadow.metadata)

    # Save changes in the new garden dataset.
    ds_garden.save()


def add_data_for_regions(tb: Table, ds_regions: Dataset, ds_income_groups: Dataset) -> Table:
    tb_with_regions = tb.copy()
    for region in REGIONS:
        # Find members of current region.
        members = geo.list_members_of_region(
            region=region,
            ds_regions=ds_regions,
            ds_income_groups=ds_income_groups,
        )

        tb_with_regions = geo.add_region_aggregates(
            df=tb_with_regions,
            region=region,
            countries_in_region=members,
            countries_that_must_have_data=[],
            num_allowed_nans_per_year=None,
            frac_allowed_nans_per_year=0.3,
        )

    return tb_with_regions


def add_per_capita_variables(tb: Table) -> Table:
    """
    Add per-capita variables for total exports and imports of plastic waste.

    Parameters
    ----------
    tb : Table
        Primary data.
    ds_population : Dataset
        Population dataset.
    Returns
    -------
    tb : Table
        Data after adding per-capita variables.
    """
    tb_with_per_capita = tb.copy()

    ds_population = paths.load_dataset("population")
    # Estimate per-capita variables.
    ## Add population variable
    tb_with_per_capita = geo.add_population_to_dataframe(
        tb_with_per_capita,
        ds_population,
        expected_countries_without_population=[],
    )
    ## Calculate per capita indicators
    for col in tb_with_per_capita.columns:
        if col not in ["year", "country", "population"]:
            tb_with_per_capita[col].metadata.origins = tb[col].metadata.origins
        if col in ["Import_TOTAL MOT", "Export_TOTAL MOT", "net_export"]:
            tb_with_per_capita[f"{col}_per_capita"] = tb_with_per_capita[col] / tb_with_per_capita["population"]
            # Add origins to per capital variable
            tb_with_per_capita[f"{col}_per_capita"].metadata.origins = tb[col].metadata.origins

    # Drop unnecessary column.
    tb_with_per_capita = tb_with_per_capita.drop(columns=["population"])

    return tb_with_per_capita


def add_share_from_total(tb: Table) -> Table:
    """
    Calculate the share of imports and exports for each country relative to
    the world total and add these as new columns to the input dataframe.

    The function performs the following steps:
    - Extracts world totals for each year for the columns 'Import_TOTAL MOT' and 'Export_TOTAL MOT'.
    - Merges the world totals with the main dataframe on the 'year' column.
    - Calculates the import and export share for each country as a percentage of the world total.
    - Drops the columns used for intermediate calculations.

    Parameters
    ----------
    tb : pandas.DataFrame
        The input data frame, which must contain at least the following columns:
        - 'country': The name of the country.
        - 'year': The year of the data.
        - 'Import_TOTAL MOT': The total import value for the country in the given year.
        - 'Export_TOTAL MOT': The total export value for the country in the given year.

    Returns
    -------
    merged_df : pandas.DataFrame
        A dataframe with the same columns as `tb`, plus two additional columns:
        - 'import_share': The share of the country's imports relative to the world total, expressed as a percentage.
        - 'export_share': The share of the country's exports relative to the world total, expressed as a percentage.

    """

    # Extract the World totals for each year
    world_totals = tb[tb["country"] == "World"][["year", "Import_TOTAL MOT", "Export_TOTAL MOT"]]

    # Merge these totals with the main dataframe on the year column
    merged_df = pr.merge(tb, world_totals, on="year", suffixes=("", "_World"))

    # Calculate the shares for each country
    merged_df["import_share"] = (merged_df["Import_TOTAL MOT"] / merged_df["Import_TOTAL MOT_World"]) * 100
    merged_df["export_share"] = (merged_df["Export_TOTAL MOT"] / merged_df["Export_TOTAL MOT_World"]) * 100
    # Drop the intermediate columns used for calculations
    merged_df = merged_df.drop(columns=["Import_TOTAL MOT_World", "Export_TOTAL MOT_World"])

    return merged_df
