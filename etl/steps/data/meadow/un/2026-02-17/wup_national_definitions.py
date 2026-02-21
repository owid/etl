"""Load a snapshot and create a meadow dataset for UN World Urbanization Prospects (National Definitions)."""

import owid.catalog.processing as pr
import pandas as pd
from owid.catalog import Table

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot files
    snap_population = paths.load_snapshot("wup_urban_rural_population.xlsx")
    snap_share = paths.load_snapshot("wup_urban_rural_population_share.xlsx")
    snap_growth = paths.load_snapshot("wup_urban_rural_population_growth.xlsx")
    snap_share_growth = paths.load_snapshot("wup_urban_rural_population_share_growth.xlsx")

    #
    # Process data.
    #

    tb_population = process_file(snap_population, metric="population")

    tb_share = process_file(snap_share, metric="share")

    tb_growth = process_file(snap_growth, metric="growth_rate")

    tb_share_growth = process_file(snap_share_growth, metric="share_growth_rate")

    tb = pr.merge(
        tb_population,
        tb_share,
        on=[
            "country",
            "loc_id",
            "year",
            "area_type",
        ],
        how="outer",
    )
    tb = pr.merge(
        tb,
        tb_growth,
        on=["country", "loc_id", "year", "area_type"],
        how="outer",
    )
    tb = pr.merge(
        tb,
        tb_share_growth,
        on=[
            "country",
            "loc_id",
            "year",
            "area_type",
        ],
        how="outer",
    )

    tb = tb.format(["country", "loc_id", "year", "area_type"])
    #
    # Save outputs.
    #
    # Create a new meadow dataset with all tables
    ds_meadow = paths.create_dataset(tables=[tb], default_metadata=snap_population.metadata)

    # Save changes in the new meadow dataset.
    ds_meadow.save()


def process_file(snap, metric: str) -> Table:
    """Process one Excel file and return a table."""
    # Check which sheets are available (not all files have 'Total')
    xl = pd.ExcelFile(snap.path)
    available_sheets = xl.sheet_names

    # Read all available area type sheets (Urban, Rural, and optionally Total)
    tables = []

    for area_type in ["Urban", "Rural", "Total"]:
        if area_type not in available_sheets:
            continue
        # Read the sheet
        tb = snap.read(sheet_name=area_type)

        # Keep only relevant columns
        id_cols = ["Index", "Location", "LocID"]
        # Year columns are all numeric column names
        year_cols = [col for col in tb.columns if str(col).isdigit()]

        tb = tb[id_cols + year_cols].copy()

        # Rename columns to snake_case
        tb = tb.rename(
            columns={
                "Location": "country",
                "LocID": "loc_id",
            }
        )

        # Melt to long format
        tb = tb.melt(
            id_vars=["country", "loc_id"],
            value_vars=year_cols,
            var_name="year",
            value_name=metric,
        )

        # Add area type column
        tb["area_type"] = area_type.lower()

        # Convert year to int
        tb["year"] = tb["year"].astype(int)

        tables.append(tb)

    # Concatenate all area types
    tb = pr.concat(tables, ignore_index=True)

    # Drop rows where the metric value is NaN (missing data for all years)
    tb = tb.dropna(subset=[metric])

    return tb
