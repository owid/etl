"""Load a meadow dataset and create a garden dataset."""

import owid.catalog.processing as pr
from owid.catalog import Table

from etl.data_helpers import geo
from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

REGIONS = [
    "Europe",
    "Asia",
    "North America",
    "South America",
    "Africa",
    "Oceania",
    "High-income countries",
    "Low-income countries",
    "Lower-middle-income countries",
    "Upper-middle-income countries",
    "European Union (27)",
    "World",
]


def run() -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("plastic_waste_2023_2024")
    ds_garden_up_to_2023 = paths.load_dataset("plastic_waste")
    ds_regions = paths.load_dataset("regions")
    ds_income_groups = paths.load_dataset("income_groups")

    # Read table from meadow dataset.
    tb = ds_meadow.read("plastic_waste_2023_2024")
    tb_up_to_2023 = ds_garden_up_to_2023.read("plastic_waste")

    #
    # Process data.
    #
    # Harmonize country names.
    tb = geo.harmonize_countries(df=tb, countries_file=paths.country_mapping_path)

    # Pivot to get exports and imports by each mode of transport
    tb = tb.pivot(
        index=["year", "country"], columns=["export_vs_import", "mode_of_transport"], values="qty"
    ).reset_index()

    tb.columns = [f"{col[0]}_{col[1]}" if col[0] not in ["year", "country"] else col[0] for col in tb.columns]

    tb = geo.add_regions_to_table(
        tb,
        ds_regions=ds_regions,
        ds_income_groups=ds_income_groups,
        regions=REGIONS,
    )

    tb["net_export"] = tb["Export_TOTAL MOT"] - tb["Import_TOTAL MOT"]

    tb = add_per_capita_variables(tb)

    tb = add_share_from_total(tb)

    tb = add_share_transported_by_air(tb)

    # Improve table format.
    tb = tb.format(["country", "year"])

    # Convert columns that are not per capita/ share of to tonnes
    for col in tb.columns:
        if "per_capita" not in col and "share" not in col:
            tb[col] = tb[col] / 1000

    tb = tb.reset_index()

    # Combine with data up to 2023
    tb_up_to_2023 = tb_up_to_2023[tb.columns]
    tb = pr.concat([tb, tb_up_to_2023], ignore_index=True)

    # Remove share values for World
    tb.loc[tb["country"] == "World", ["import_share", "export_share"]] = None

    tb = tb.format(["country", "year"])

    #
    # Save outputs.
    #
    # Initialize a new garden dataset.
    ds_garden = paths.create_dataset(tables=[tb], default_metadata=ds_meadow.metadata)

    # Save garden dataset.
    ds_garden.save()


def add_per_capita_variables(tb: Table) -> Table:
    """
    Add per-capita variables for total exports and imports of plastic waste.
    """
    tb_with_per_capita = tb.copy()

    ds_population = paths.load_dataset("population")
    # Estimate per-capita variables.
    ## Add population variable
    tb_with_per_capita = geo.add_population_to_table(
        tb_with_per_capita,
        ds_population,
    )
    ## Calculate per capita indicators
    for col in tb_with_per_capita.columns:
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
    """

    # Extract the World totals for each year
    world_totals = tb[tb["country"] == "World"][["year", "Import_TOTAL MOT", "Export_TOTAL MOT"]]
    # Merge these totals with the main dataframe on the year column
    merged_df = pr.merge(tb, world_totals, on="year", suffixes=("", "_World"))

    # Calculate the shares for each country (excluding World)
    merged_df["import_share"] = (merged_df["Import_TOTAL MOT"] / merged_df["Import_TOTAL MOT_World"]) * 100
    merged_df["export_share"] = (merged_df["Export_TOTAL MOT"] / merged_df["Export_TOTAL MOT_World"]) * 100

    # Drop the intermediate columns used for calculations
    merged_df = merged_df.drop(columns=["Import_TOTAL MOT_World", "Export_TOTAL MOT_World"])

    return merged_df


def add_share_transported_by_air(tb: Table) -> Table:
    """
    Calculate the share of plastic waste transported by air for exports and imports.
    """
    tb_with_air_share = tb.copy()

    # Calculate share transported by air for exports
    tb_with_air_share["export_share_transported_by_air"] = (
        tb_with_air_share["Export_Air"] / tb_with_air_share["Export_TOTAL MOT"]
    ) * 100
    tb_with_air_share["export_share_transported_by_air"].metadata.origins = tb["Export_Air"].metadata.origins

    # Calculate share transported by air for imports
    tb_with_air_share["import_share_transported_by_air"] = (
        tb_with_air_share["Import_Air"] / tb_with_air_share["Import_TOTAL MOT"]
    ) * 100
    tb_with_air_share["import_share_transported_by_air"].metadata.origins = tb["Import_Air"].metadata.origins

    return tb_with_air_share
