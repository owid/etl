"""Load a snapshot and create the World Inequality Dataset meadow dataset."""

from typing import cast

import numpy as np
import pandas as pd
from owid.catalog import Dataset, Table
from structlog import get_logger

from etl.helpers import PathFinder
from etl.snapshot import Snapshot
from etl.steps.data.converters import convert_snapshot_metadata

# Initialize logger.
log = get_logger()

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# List of countries/regions not included in the ISO2 standard, but added by WID
iso2_missing = {
    "CN-RU": "China (rural)",
    "CN-UR": "China (urban)",
    "DD": "East Germany",
    "KS": "Kosovo",
    "OA": "Other Russia and Central Asia (WID)",
    "OB": "Other East Asia (WID)",
    "OC": "Other Western Europe (WID)",
    "OD": "Other Latin America (WID)",
    "OE": "Other MENA (WID)",
    "OH": "Other North America and Oceania (WID)",
    "OI": "Other South & South-East Asia (WID)",
    "OJ": "Other Sub-Saharan Africa (WID)",
    "QB": "Africa (WID)",
    "QD": "Asia (WID)",
    "QE": "Europe (WID)",
    "QF": "Oceania (WID)",
    "QJ": "Central Asia (WID)",
    "QK": "East Africa (WID)",
    "QL": "East Asia (WID)",
    "QM": "Eastern Europe (WID)",
    "QN": "Middle Africa (WID)",
    "QO": "North Africa (WID)",
    "QP": "North America (WID)",
    "QS": "South-East Asia (WID)",
    "QT": "South Africa region (WID)",
    "QU": "South Asia (WID)",
    "QV": "West Africa (WID)",
    "QW": "West Asia (WID)",
    "QX": "Western Europe (WID)",
    "QY": "European Union (WID)",
    "WO": "World",
    "XA": "Asia (excluding Middle East) (WID)",
    "XB": "North America and Oceania (WID)",
    "XF": "Sub-Saharan Africa (WID)",
    "XL": "Latin America (WID)",
    "XM": "Middle East (WID)",
    "XN": "MENA (WID)",
    "XR": "Russia and Central Asia (WID)",
    "XS": "South & South-East Asia (WID)",
    "ZZ": "Zanzibar",
}

# We are not using these entities
# iso2_missing_mer = {
#     "OA-MER": "Other Russia and Central Asia (at market exchange rate) (WID)",
#     "OB-MER": "Other East Asia (at market exchange rate) (WID)",
#     "OC-MER": "Other Western Europe (at market exchange rate) (WID)",
#     "OD-MER": "Other Latin America (at market exchange rate) (WID)",
#     "OE-MER": "Other MENA (at market exchange rate) (WID)",
#     "OH-MER": "Other North America and Oceania (at market exchange rate) (WID)",
#     "OI-MER": "Other South & South-East Asia (at market exchange rate) (WID)",
#     "OJ-MER": "Other Sub-Saharan Africa (at market exchange rate) (WID)",
#     "QB-MER": "Africa (at market exchange rate) (WID)",
#     "QD-MER": "Asia (at market exchange rate) (WID)",
#     "QE-MER": "Europe (at market exchange rate) (WID)",
#     "QF-MER": "Oceania (at market exchange rate) (WID)",
#     "QJ-MER": "Central Asia (at market exchange rate) (WID)",
#     "QK-MER": "East Africa (at market exchange rate) (WID)",
#     "QL-MER": "East Asia (at market exchange rate) (WID)",
#     "QM-MER": "Eastern Europe (at market exchange rate) (WID)",
#     "QN-MER": "Middle Africa (at market exchange rate) (WID)",
#     "QO-MER": "North Africa (at market exchange rate) (WID)",
#     "QP-MER": "North America (at market exchange rate) (WID)",
#     "QS-MER": "South-East Asia (at market exchange rate) (WID)",
#     "QT-MER": "South Africa region (at market exchange rate) (WID)",
#     "QU-MER": "South Asia (at market exchange rate) (WID)",
#     "QV-MER": "West Africa (at market exchange rate) (WID)",
#     "QW-MER": "West Asia (at market exchange rate) (WID)",
#     "QX-MER": "Western Europe (at market exchange rate) (WID)",
#     "QY-MER": "European Union (at market exchange rate) (WID)",
#     "WO-MER": "World (at market exchange rate) (WID)",
#     "XA-MER": "Asia (excluding Middle East) (at market exchange rate) (WID)",
#     "XB-MER": "North America and Oceania (at market exchange rate) (WID)",
#     "XF-MER": "Sub-Saharan Africa (at market exchange rate) (WID)",
#     "XL-MER": "Latin America (at market exchange rate) (WID)",
#     "XM-MER": "Middle East (at market exchange rate) (WID)",
#     "XN-MER": "MENA (at market exchange rate) (WID)",
#     "XR-MER": "Russia and Central Asia (at market exchange rate) (WID)",
#     "XS-MER": "South & South-East Asia (at market exchange rate) (WID)",
# }

# Create a dictionary with the names of the snapshots and their id variables
snapshots_dict = {
    "world_inequality_database": ["country", "year"],
    "world_inequality_database_distribution": ["country", "year", "welfare", "p", "percentile"],
}


# Country harmonization function, using both the reference country/regional OWID dataset and WID's `iso2_missing` list
def harmonize_countries(df: pd.DataFrame, iso2_missing: dict) -> pd.DataFrame:
    # Load reference file with country names in OWID standard
    df_countries_regions = cast(Dataset, paths.load_dependency("regions"))["regions"]

    # Merge dataset and country dictionary to get the name of the country
    df = pd.merge(
        df, df_countries_regions[["name", "iso_alpha2"]], left_on="country", right_on="iso_alpha2", how="left"
    )

    # Several countries are not matched, because WID amends the ISO-2 list with additional countries and regions
    # See https://wid.world/codes-dictionary/#country-code

    # Replace missing items
    for x, y in iso2_missing.items():
        df["name"] = np.where(df["country"] == x, y, df["name"])

    missing_list = list(df[df["name"].isnull()]["country"].unique())
    missing_count = len(missing_list)

    # Warns if there are still entities missing
    if missing_count > 0:
        log.warning(
            f"There are still unnamed {missing_count} WID countries/regions and will be deleted! Take a look at this list:\n {missing_list}"
        )

    # Drop rows without match
    df = df[~df["name"].isnull()].reset_index(drop=True)
    # Drop old country and ISO alpha 2 variable. Rename the newly built variable as `country`
    df = df.drop(columns=["country", "iso_alpha2"])
    df = df.rename(columns={"name": "country"})

    # Move country and year to the beginning
    cols_to_move = ["country", "year"]
    df = df[cols_to_move + [col for col in df.columns if col not in cols_to_move]]

    return df


def run(dest_dir: str) -> None:
    log.info("world_inequality_database.start")

    # Create a new meadow dataset with the same metadata as the snapshot.
    snap = paths.load_dependency("world_inequality_database.csv")
    ds_meadow = Dataset.create_empty(dest_dir, metadata=convert_snapshot_metadata(snap.metadata))

    # Ensure the version of the new dataset corresponds to the version of current step.
    ds_meadow.metadata.version = paths.version
    ds_meadow.metadata.short_name = "world_inequality_database"

    #
    # Load inputs.

    for ds_name, ds_ids in snapshots_dict.items():
        # Load data from snapshot.
        # `keep_default_na` and `na_values` are included because there is a country labeled NA, Namibia, which becomes null without the parameters
        na_values = [
            "-1.#IND",
            "1.#QNAN",
            "1.#IND",
            "-1.#QNAN",
            "#N/A N/A",
            "#N/A",
            "N/A",
            "n/a",
            "",
            "#NA",
            "NULL",
            "null",
            "NaN",
            "-NaN",
            "nan",
            "-nan",
            "",
        ]

        # Retrieve snapshot.
        snap: Snapshot = paths.load_dependency(f"{ds_name}.csv")
        df = pd.read_csv(
            snap.path,
            keep_default_na=False,
            na_values=na_values,
        )

        # Retrieve snapshot with extrapolations
        snap: Snapshot = paths.load_dependency(f"{ds_name}_with_extrapolations.csv")
        # Load data from snapshot.
        # `keep_default_na` and `na_values` are included because there is a country labeled NA, Namibia, which becomes null without the parameters
        df_extrapolations = pd.read_csv(
            snap.path,
            keep_default_na=False,
            na_values=na_values,
        )

        # Combine both datasets
        df = pd.merge(df, df_extrapolations, on=ds_ids, how="outer", suffixes=("", "_extrapolated"))

        #
        # Process data.
        #
        # Harmonize countries
        df = harmonize_countries(df, iso2_missing)

        # Create a new table and ensure all columns are snake-case.
        tb = Table(df, short_name=ds_name, underscore=True)

        # Set index and sort
        tb = tb.set_index(ds_ids, verify_integrity=True).sort_index()

        # Add the new table to the meadow dataset.
        ds_meadow.add(tb)

    # Save changes in the new garden dataset.
    ds_meadow.save()

    log.info("world_inequality_database.end")
