"""Load a snapshot and create the World Inequality Dataset meadow dataset."""

import numpy as np
import pandas as pd
from owid.catalog import Dataset, Table
from structlog import get_logger

from etl.helpers import PathFinder, create_dataset
from etl.paths import REFERENCE_DATASET
from etl.snapshot import Snapshot

# Initialize logger.
log = get_logger()

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# List of countries/regions not included in the ISO2 standard, but added by WID
iso2_missing = {
    "CN-RU": "China - rural",
    "CN-UR": "China - urban",
    "DD": "East Germany",
    "KS": "Kosovo",
    "OA": "Other Russia and Central Asia (WID)",
    "OA-MER": "Other Russia and Central Asia (at market exchange rate) (WID)",
    "OB": "Other East Asia (WID)",
    "OB-MER": "Other East Asia (at market exchange rate) (WID)",
    "OC": "Other Western Europe (WID)",
    "OC-MER": "Other Western Europe (at market exchange rate) (WID)",
    "OD": "Other Latin America (WID)",
    "OD-MER": "Other Latin America (at market exchange rate) (WID)",
    "OE": "Other MENA (WID)",
    "OE-MER": "Other MENA (at market exchange rate) (WID)",
    "OH": "Other North America and Oceania (WID)",
    "OH-MER": "Other North America and Oceania (at market exchange rate) (WID)",
    "OI": "Other South & South-East Asia (WID)",
    "OI-MER": "Other South & South-East Asia (at market exchange rate) (WID)",
    "OJ": "Other Sub-Saharan Africa (WID)",
    "OJ-MER": "Other Sub-Saharan Africa (at market exchange rate) (WID)",
    "QB": "Africa (WID)",
    "QB-MER": "Africa (at market exchange rate) (WID)",
    "QD": "Asia (WID)",
    "QD-MER": "Asia (at market exchange rate) (WID)",
    "QE": "Europe (WID)",
    "QE-MER": "Europe (at market exchange rate) (WID)",
    "QF": "Oceania (WID)",
    "QF-MER": "Oceania (at market exchange rate) (WID)",
    "QJ": "Central Asia (WID)",
    "QJ-MER": "Central Asia (at market exchange rate) (WID)",
    "QK": "East Africa (WID)",
    "QK-MER": "East Africa (at market exchange rate) (WID)",
    "QL": "East Asia (WID)",
    "QL-MER": "East Asia (at market exchange rate) (WID)",
    "QM": "Eastern Europe (WID)",
    "QM-MER": "Eastern Europe (at market exchange rate) (WID)",
    "QN": "Middle Africa (WID)",
    "QN-MER": "Middle Africa (at market exchange rate) (WID)",
    "QO": "North Africa (WID)",
    "QO-MER": "North Africa (at market exchange rate) (WID)",
    "QP": "North America (WID)",
    "QP-MER": "North America (at market exchange rate) (WID)",
    "QS": "South-East Asia (WID)",
    "QS-MER": "South-East Asia (at market exchange rate) (WID)",
    "QT": "South Africa region (WID)",
    "QT-MER": "South Africa region (at market exchange rate) (WID)",
    "QU": "South Asia (WID)",
    "QU-MER": "South Asia (at market exchange rate) (WID)",
    "QV": "West Africa (WID)",
    "QV-MER": "West Africa (at market exchange rate) (WID)",
    "QW": "West Asia (WID)",
    "QW-MER": "West Asia (at market exchange rate) (WID)",
    "QX": "Western Europe (WID)",
    "QX-MER": "Western Europe (at market exchange rate) (WID)",
    "QY": "European Union (WID)",
    "QY-MER": "European Union (at market exchange rate) (WID)",
    "WO": "World (WID)",
    "WO-MER": "World (at market exchange rate) (WID)",
    "XA": "Asia (excluding Middle East) (WID)",
    "XA-MER": "Asia (excluding Middle East) (at market exchange rate) (WID)",
    "XB": "North America and Oceania (WID)",
    "XB-MER": "North America and Oceania (at market exchange rate) (WID)",
    "XF": "Sub-Saharan Africa (WID)",
    "XF-MER": "Sub-Saharan Africa (at market exchange rate) (WID)",
    "XL": "Latin America (WID)",
    "XL-MER": "Latin America (at market exchange rate) (WID)",
    "XM": "Middle East (WID)",
    "XM-MER": "Middle East (at market exchange rate) (WID)",
    "XN": "MENA (WID)",
    "XN-MER": "MENA (at market exchange rate) (WID)",
    "XR": "Russia and Central Asia (WID)",
    "XR-MER": "Russia and Central Asia (at market exchange rate) (WID)",
    "XS": "South & South-East Asia (WID)",
    "XS-MER": "South & South-East Asia (at market exchange rate) (WID)",
    "ZZ": "Zanzibar",
}

# Country harmonization function, using both the reference country/regional OWID dataset and WID's `iso2_missing` list
def harmonize_countries(df: pd.DataFrame, iso2_missing: dict) -> pd.DataFrame:

    # Load reference file with country names in OWID standard
    ds_reference = Dataset(REFERENCE_DATASET)
    df_countries_regions = pd.DataFrame(ds_reference["countries_regions"]).reset_index()

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
            f"There are still {missing_count} WID countries/regions without a name! Take a look at this list:\n {missing_list}"
        )

    # Drop old country and ISO alpha 2 variable. Rename the newly built variable as `country`
    df = df.drop(columns=["country", "iso_alpha2"])
    df = df.rename(columns={"name": "country"})

    return df


def run(dest_dir: str) -> None:
    log.info("world_inequality_database.start")

    #
    # Load inputs.

    # Retrieve snapshot.
    snap: Snapshot = paths.load_dependency("world_inequality_database.csv")

    # Load data from snapshot.
    # `keep_default_na` and `na_values` are included because there is a country labeled NA, Namibia, which becomes null without the parameters
    df = pd.read_csv(
        snap.path,
        keep_default_na=False,
        na_values=[
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
        ],
    )

    #
    # Process data.
    #
    # Harmonize countries
    df = harmonize_countries(df, iso2_missing)

    # Move country and year to the beginning
    cols_to_move = ["country", "year"]
    df = df[cols_to_move + [col for col in df.columns if col not in cols_to_move]]

    # Create a new table and ensure all columns are snake-case.
    tb = Table(df, short_name=paths.short_name, underscore=True)

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot and add the table.
    ds_meadow = create_dataset(dest_dir, tables=[tb], default_metadata=snap.metadata)

    # Save changes in the new garden dataset.
    ds_meadow.save()

    log.info("world_inequality_database.end")
