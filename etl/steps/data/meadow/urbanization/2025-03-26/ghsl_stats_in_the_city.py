"""Load a snapshot and create a meadow dataset."""

import os
import zipfile

import numpy as np
import owid.catalog.processing as pr

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Sheets and columns to extract.
SHEETS_AND_COLUMNS = {
    "EXPOSURE": ["EX_L05_SHP_"],  # Share of population living in Low Elevation Coastal Zones (<5m) (%)
    "CLIMATE": [
        "CL_WDS_CUR_",  # Share of days exceeding the historical 90th percentile of maximum temperature for that calendar day
        "CL_B01_CUR_",  # Annual Mean Temperature in the decade
        "CL_B12_CUR_",  # Annual Precipitation in the decade
    ],
    "HEALTH": [
        "HL_FCL_HOS_",  # Number of hospitals
        "HL_FCL_PHA_",  # Number of pharmacies
        "HL_FPC_HOS_",  # Number of hospitals per capita
        "HL_FPC_PHA_",  # Number of pharmacies per capita
        "HL_FDE_HOS_",  # Number of hospitals per urban centre area
        "HL_FDE_PHA_",  # Number of pharmacies per urban centre area
        "HL_SHP_HOS_",  # Share of urban population within 1 km of a hospital
        "HL_SHP_PHA_",  # Share of urban population within 1 km of a pharmacy
        "HL_POP_HOS_",  # Population within 1 km of a hospital
        "HL_POP_PHA_",  # Population within 1 km of a pharmacy
    ],
    "SOCIOECONOMIC": [
        "SC_CON_DSF_",  # Average download speed
        "SC_SEC_LET_",  # Life expectancy
    ],
}
# Column and indicator mapping.
COLUMN_MAPPING = {
    "EX_L05_SHP_": "Share of population living in Low Elevation Coastal Zones (<5m) (%)",
    "CL_WDS_CUR_": "Share of days exceeding the historical 90th percentile of maximum temperature for that calendar day",
    "HL_FCL_HOS_": "Number of hospitals",
    "HL_FCL_PHA_": "Number of pharmacies",
    "HL_FPC_HOS_": "Number of hospitals per capita",
    "HL_FPC_PHA_": "Number of pharmacies per capita",
    "HL_FDE_HOS_": "Number of hospitals per urban centre area",
    "HL_FDE_PHA_": "Number of pharmacies per urban centre area",
    "HL_SHP_HOS_": "Share of urban centre population within 1 km of a hospital",
    "HL_SHP_PHA_": "Share of urban centre population within 1 km of a pharmacy",
    "HL_POP_HOS_": "Population within 1 km of a hospital",
    "HL_POP_PHA_": "Population within 1 km of a pharmacy",
    "CL_B01_CUR_": "Annual mean temperature in the decade",
    "CL_B12_CUR_": "Annual precipitation in the decade",
    "SC_CON_DSF_": "Average download speed",
    "SC_SEC_LET_": "Life expectancy",
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

    # Select the top 100 cities with the largest population in GC_POP_TOT_2025
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
