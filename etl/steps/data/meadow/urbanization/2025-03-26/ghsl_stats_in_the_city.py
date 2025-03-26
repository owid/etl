"""Load a snapshot and create a meadow dataset."""

import os
import zipfile

import numpy as np
import owid.catalog.processing as pr
import pandas as pd

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Sheets and columns to extract.
SHEETS_AND_COLUMNS = {
    "SOCIOECONOMIC": [
        "SC_SEC_PCF_",  # Share of female population
        "SC_SEC_PCM_",  # Share of male population
        "SC_SEC_PCY_",  # Share of young population (0-14 years)
        "SC_SEC_PCA_",  # Share of adult population (15-64 years)
        "SC_SEC_PCO_",  # Share of old population (> 65 years)
    ],
    "GREENNESS": ["GR_SHB_GRN_"],  # Share of green area in built-up area (prefix)
    "EXPOSURE": ["EX_LEC_SHP_"],  # Share of population living in LECZ (below 10m)
    "HAZARD_RISK": [
        "HZ_CEV_EAR_",  # Earthquake occurrence
        "HZ_CEV_EWI_",  # Extreme wind occurrence
        "HZ_CEV_TSU_",  # Tsunami occurrence
        "HZ_CEV_HEW_",  # Heatwave occurrence
        "HZ_CEV_DRO_",  # Drought occurrence
        "HZ_CEV_FLO_",  # Flood occurrence
        "HZ_CEV_TCY_",  # Tropical Cyclone occurrence
        "HZ_CEV_VOL_",  # Volcano occurrence
        "HZ_CEV_LAN_",  # Landslide occurrence
        "HZ_CEV_COW_",  # Coldwave occurrence
    ],
}
# Column and indicator mapping.
COLUMN_MAPPING = {
    "SC_SEC_PCF_": "Share of female population (%)",
    "SC_SEC_PCM_": "Share of male population (%)",
    "SC_SEC_PCY_": "Share of young population (0-14 years) (%)",
    "SC_SEC_PCA_": "Share of adult population (15-64 years) (%)",
    "SC_SEC_PCO_": "Share of old population (> 65 years) (%)",
    "GR_SHB_GRN_": "Share of green area in built-up area (%)",
    "EX_LEC_SHP_": "Share of population living in Low Elevation Coastal Zones (<10m) (%)",
    "HZ_CEV_EAR_": "Earthquake occurrence (events per year)",
    "HZ_CEV_EWI_": "Extreme wind occurrence (events per year)",
    "HZ_CEV_TSU_": "Tsunami occurrence (events per year)",
    "HZ_CEV_HEW_": "Heatwave occurrence (events per year)",
    "HZ_CEV_DRO_": "Drought occurrence (events per year)",
    "HZ_CEV_FLO_": "Flood occurrence (events per year)",
    "HZ_CEV_TCY_": "Tropical Cyclone occurrence (events per year)",
    "HZ_CEV_VOL_": "Volcano occurrence (events per year)",
    "HZ_CEV_LAN_": "Landslide occurrence (events per year)",
    "HZ_CEV_COW_": "Coldwave occurrence (events per year)",
}


def run() -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("ghsl_stats_in_the_city.zip")
    extract_dir = os.path.dirname(snap.path)
    #
    # Process data.
    #
    # Open the ZIP archive
    with zipfile.ZipFile(snap.path, "r") as z:
        # Extract the file from the ZIP archive
        z.extract("GHS_UCDB_GLOBE_R2024A.xlsx", extract_dir)
    # Create the full file path
    file_path = os.path.join(extract_dir, "GHS_UCDB_GLOBE_R2024A.xlsx")

    # Extract capitals only
    tb_capitals = pr.read_excel(file_path, sheet_name="GENERAL_CHARACTERISTICS", metadata=snap.to_table_metadata())
    tb_capitals = tb_capitals[tb_capitals["GC_UCM_CAP"] == 1][["GC_UCN_MAI_2025", "GC_CNT_GAD_2025"]]

    # Extract the required data from each sheet
    extracted_data = []
    city_country = ["GC_UCN_MAI_2025", "GC_CNT_GAD_2025"]

    for sheet, column_prefixes in SHEETS_AND_COLUMNS.items():
        tb = pr.read_excel(file_path, sheet_name=sheet, metadata=snap.to_table_metadata())
        # Filter the required columns: exact matches and prefixes (with year suffixes)
        selected_columns = [col for col in tb.columns if any(col.startswith(prefix) for prefix in column_prefixes)]
        # Include common columns if they exist in the sheet
        selected_columns += [col for col in city_country if col in tb.columns]

        # Filter rows to include only those present in df_capitals
        tb = tb[selected_columns]

        # **Merge to filter rows that match capitals**
        tb = pr.merge(tb_capitals, tb, on=["GC_UCN_MAI_2025", "GC_CNT_GAD_2025"], how="inner")
        tb = tb.rename(columns={"GC_UCN_MAI_2025": "city", "GC_CNT_GAD_2025": "country"})
        # Identify columns that need to be melted (those with prefixes in the mapping)
        value_vars = [col for col in tb.columns if any(col.startswith(prefix) for prefix in COLUMN_MAPPING.keys())]

        # Melt the dataframe
        tb = tb.melt(id_vars=["city", "country"], value_vars=value_vars, var_name="indicator", value_name="value")

        # Replace "-" with NaN
        tb["value"] = tb["value"].replace("-", np.nan)

        # Extract year (last 4 characters of the column name)
        tb["year"] = tb["indicator"].str[-4:]

        # Remove the year from the indicator name
        tb["indicator"] = tb["indicator"].str[:-4]  # Removing the year

        # Replace indicator names with their mapped full names
        tb["indicator"] = tb["indicator"].map(COLUMN_MAPPING)
        extracted_data.append(tb)

    tb = pr.concat(extracted_data)

    # Add origins metadata.
    tb["value"].metadata.origins = [snap.m.origin]

    # Improve tables format.
    tb = tb.format(["country", "city", "year", "indicator"])

    #
    # Save outputs.
    #
    # Initialize a new meadow dataset.
    ds_meadow = paths.create_dataset(tables=[tb], default_metadata=snap.metadata)

    # Save meadow dataset.
    ds_meadow.save()
