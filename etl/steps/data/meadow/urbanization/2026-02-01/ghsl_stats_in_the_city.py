"""Load a snapshot and create a meadow dataset."""

import numpy as np
import owid.catalog.processing as pr

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Sheets and columns to extract.
SHEETS_AND_COLUMNS = {
    "EXPOSURE": [
        "EX_L05_SHP_",  # Share of population living in Low Elevation Coastal Zones (<5m) (%)
    ],
    "CLIMATE": [
        "CL_WDS_CUR_",  # Share of days exceeding the historical 90th percentile of maximum temperature for that calendar day
        "CL_B01_CUR_",  # Annual Mean Temperature in the decade
        "CL_B12_CUR_",  # Annual Precipitation in the decade
        "CL_LCZ_A01_",  # Share of the urban center in LCZ 1 (compact highrise)",
        "CL_LCZ_A02_",  # Share of the urban center in LCZ 2 (compact midrise)
        "CL_LCZ_A03_",  # Share of the urban center in LCZ 3 (compact lowrise)
        "CL_LCZ_A04_",  # Share of the urban center in LCZ 4 (open highrise)
        "CL_LCZ_A05_",  # Share of the urban center in LCZ 5 (open midrise)
        "CL_LCZ_A06_",  # Share of the urban center in LCZ 6 (open lowrise)
        "CL_LCZ_A07_",  # Share of the urban center in LCZ 7 (lightweight lowrise)
        "CL_LCZ_A08_",  # Share of the urban center in LCZ 8 (large lowrise)
        "CL_LCZ_A09_",  # Share of the urban center in LCZ 9 (sparsely built)
        "CL_LCZ_A10_",  # Share of the urban center in LCZ 10 (heavy industry)
        "CL_LCZ_A11_",  # Share of the urban center in LCZ 11 (dense trees)
        "CL_LCZ_A12_",  # Share of the urban center in LCZ 12 (scattered trees)
        "CL_LCZ_A13_",  # Share of the urban center in LCZ 13 (bush, scrub)
        "CL_LCZ_A14_",  # Share of the urban center in LCZ 14 (low plants)
        "CL_LCZ_A15_",  # Share of the urban center in LCZ 15 (bare rock or paved)
        "CL_LCZ_A16_",  # Share of the urban center in LCZ 16 (bare soil or sand)
        "CL_LCZ_A17_",  # Share of the urban center in LCZ 17 (water)
        "CL_REN_PVO_",  # Average daily photovoltaic potential
    ],
    "SOCIOECONOMIC": [
        "SC_CON_DSF_",  # Average download speed
    ],
    "EMISSIONS": [
        "EM_CO2_PEC_",  # CO2 emissions per capita
        "EM_GHG_PEC_",  # Greenhouse gas emissions per capita
        "EM_ENE_PER_",  # Share of energy emissions in total emissions
        "EM_RES_PER_",  # Share of residential emissions in total emissions
        "EM_IND_PER_",  # Share of industrial emissions in total emissions
        "EM_TRA_PER_",  # Share of transport emissions in total emissions
        "EM_WAS_PER_",  # Share of waste emissions in total emissions
        "EM_AGR_PER_",  # Share of agricultural emissions in total emissions
    ],
    "SDG": ["SD_POP_HGR_"],  # Share of population living in the high green area
    "INFRASTRUCTURES": ["IN_ROA_DEN_"],  # Road network density
    "HEALTH": ["HL_SHP_HOS_", "HL_SHP_PHA_"],  # Share of population with access to hospitals and pharmacies
}
# Column and indicator mapping.
COLUMN_MAPPING = {
    "EX_L05_SHP_": "Share of population living in Low Elevation Coastal Zones (<5m) (%)",
    "CL_WDS_CUR_": "Share of days exceeding the historical 90th percentile of maximum temperature for that calendar day",
    "CL_B01_CUR_": "Annual mean temperature in the decade",
    "CL_B12_CUR_": "Annual precipitation in the decade",
    "SC_CON_DSF_": "Average download speed",
    "EM_CO2_PEC_": "CO2 emissions per capita",
    "EM_GHG_PEC_": "Greenhouse gas emissions per capita",
    "EM_ENE_PER_": "Share of energy emissions in total emissions",
    "EM_RES_PER_": "Share of residential emissions in total emissions",
    "EM_IND_PER_": "Share of industrial emissions in total emissions",
    "EM_TRA_PER_": "Share of transport emissions in total emissions",
    "EM_WAS_PER_": "Share of waste emissions in total emissions",
    "EM_AGR_PER_": "Share of agricultural emissions in total emissions",
    "CL_LCZ_A01_": "Share of the urban center in LCZ 1 (compact highrise)",
    "CL_LCZ_A02_": "Share of the urban center in LCZ 2 (compact midrise)",
    "CL_LCZ_A03_": "Share of the urban center in LCZ 3 (compact lowrise)",
    "CL_LCZ_A04_": "Share of the urban center in LCZ 4 (open highrise)",
    "CL_LCZ_A05_": "Share of the urban center in LCZ 5 (open midrise)",
    "CL_LCZ_A06_": "Share of the urban center in LCZ 6 (open lowrise)",
    "CL_LCZ_A07_": "Share of the urban center in LCZ 7 (lightweight lowrise)",
    "CL_LCZ_A08_": "Share of the urban center in LCZ 8 (large lowrise)",
    "CL_LCZ_A09_": "Share of the urban center in LCZ 9 (sparsely built)",
    "CL_LCZ_A10_": "Share of the urban center in LCZ 10 (heavy industry)",
    "CL_LCZ_A11_": "Share of the urban center in LCZ 11 (dense trees)",
    "CL_LCZ_A12_": "Share of the urban center in LCZ 12 (scattered trees)",
    "CL_LCZ_A13_": "Share of the urban center in LCZ 13 (bush, scrub)",
    "CL_LCZ_A14_": "Share of the urban center in LCZ 14 (low plants)",
    "CL_LCZ_A15_": "Share of the urban center in LCZ 15 (bare rock or paved)",
    "CL_LCZ_A16_": "Share of the urban center in LCZ 16 (bare soil or sand)",
    "CL_LCZ_A17_": "Share of the urban center in LCZ 17 (water)",
    "CL_REN_PVO_": "Average daily photovoltaic potential",
    "SD_POP_HGR_": "Share of population living in the high green area",
    "IN_ROA_DEN_": "Road network density",
    "HL_SHP_HOS_": "Share of population with access to hospitals",
    "HL_SHP_PHA_": "Share of population with access to pharmacies",
}
# Define a public-friendly mapping to simplify LCZ descriptive categories
LCZ_CATEGORY_MAPPING = {
    "Share of the urban center in LCZ 1 (compact highrise)": "Tightly built city areas",
    "Share of the urban center in LCZ 2 (compact midrise)": "Tightly built city areas",
    "Share of the urban center in LCZ 3 (compact lowrise)": "Tightly built city areas",
    "Share of the urban center in LCZ 4 (open highrise)": "Buildings surrounded by green areas",
    "Share of the urban center in LCZ 5 (open midrise)": "Buildings surrounded by green areas",
    "Share of the urban center in LCZ 6 (open lowrise)": "Buildings surrounded by green areas",
    "Share of the urban center in LCZ 7 (lightweight lowrise)": "Dense single-story lightweight housing",
    "Share of the urban center in LCZ 8 (large lowrise)": "Large low-rise buildings with paved surroundings",
    "Share of the urban center in LCZ 9 (sparsely built)": "Scattered buildings in natural settings",
    "Share of the urban center in LCZ 10 (heavy industry)": "Industrial zones",
    "Share of the urban center in LCZ 11 (dense trees)": "Forests, parks, and greenery",
    "Share of the urban center in LCZ 12 (scattered trees)": "Forests, parks, and greenery",
    "Share of the urban center in LCZ 13 (bush, scrub)": "Forests, parks, and greenery",
    "Share of the urban center in LCZ 14 (low plants)": "Forests, parks, and greenery",
    "Share of the urban center in LCZ 15 (bare rock or paved)": "Rocky or sandy land",
    "Share of the urban center in LCZ 16 (bare soil or sand)": "Rocky or sandy land",
    "Share of the urban center in LCZ 17 (water)": "Water bodies",
}


def run() -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("ghsl_stats_in_the_city.zip")
    #
    # Process data.
    #
    # Open the ZIP archive
    # Extract the required data from each sheet
    extracted_data = []
    city_country = ["GC_UCN_MAI_2025", "GC_CNT_GAD_2025", "ID_UC_G0"]

    with snap.extracted() as archive:
        tb = archive.read("GHS_UCDB_GLOBE_R2024A.xlsx", sheet_name="GENERAL_CHARACTERISTICS")

        # Select the top 100 cities with the largest population in GC_POP_TOT_2025
        tb_capitals = tb[tb["GC_UCM_CAP"] == 1][["GC_UCN_MAI_2025", "GC_CNT_GAD_2025"]].copy()

        for sheet, column_prefixes in SHEETS_AND_COLUMNS.items():
            tb = archive.read("GHS_UCDB_GLOBE_R2024A.xlsx", sheet_name=sheet)
            # Filter the required columns: exact matches and prefixes (with year suffixes)
            selected_columns = [col for col in tb.columns if any(col.startswith(prefix) for prefix in column_prefixes)]
            # Include common columns if they exist in the sheet
            selected_columns += [col for col in city_country if col in tb.columns]
            tb = tb[selected_columns]

            # Merge to filter rows that match capitals
            tb = pr.merge(tb_capitals, tb, on=["GC_UCN_MAI_2025", "GC_CNT_GAD_2025"], how="inner")
            tb = tb.drop("ID_UC_G0", axis=1)
            tb = tb.rename(columns={"GC_UCN_MAI_2025": "city", "GC_CNT_GAD_2025": "country"})
            # Identify columns that need to be melted (those with prefixes in the mapping)
            value_vars = [col for col in tb.columns if any(col.startswith(prefix) for prefix in COLUMN_MAPPING.keys())]

            # Melt the dataframe
            tb = tb.melt(id_vars=["city", "country"], value_vars=value_vars, var_name="indicator", value_name="value")

            # Replace "-" with NaN and convert to numeric
            tb["value"] = tb["value"].replace("-", np.nan)
            tb["value"] = pr.to_numeric(tb["value"], errors="coerce")

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
    # Simplify LCZ categories
    tb["indicator"] = tb["indicator"].map(LCZ_CATEGORY_MAPPING).fillna(tb["indicator"])

    # Aggregate the data by simplified categories
    tb = (
        tb.groupby(["country", "city", "year", "indicator"], as_index=False).agg(
            {"value": "sum"}
        )  # Aggregate by summing the values
    )
    # Calculate the "Unknown" indicator
    lcz_indicators = [
        "Tightly built city areas",
        "Buildings surrounded by green areas",
        "Dense single-story lightweight housing",
        "Large low-rise buildings with paved surroundings",
        "Scattered buildings in natural settings",
        "Industrial zones",
        "Forests, parks, and greenery",
        "Rocky or sandy land",
        "Water bodies",
    ]
    # Calculate the sum of LCZ values for each city and year
    lcz_sums = (
        tb[tb["indicator"].isin(lcz_indicators)].groupby(["country", "city", "year"], as_index=False)["value"].sum()
    )
    lcz_sums = lcz_sums.rename(columns={"value": "lcz_sum"})
    lcz_sums["value"] = 100 - lcz_sums["lcz_sum"]
    lcz_sums["indicator"] = "Unknown"
    lcz_sums = lcz_sums.drop(columns="lcz_sum")
    # Ensure there are no negative or zero values in the "Unknown" indicator
    if (lcz_sums["value"] <= 0).any():
        raise ValueError("Dataset contains negative or zero values in the 'Unknown' indicator.")
    # Append the "Unknown" rows to the main table
    tb = pr.concat([tb, lcz_sums], ignore_index=True)

    # Improve tables format.
    tb = tb.format(["country", "city", "year", "indicator"])

    #
    # Save outputs.
    #
    # Initialize a new meadow dataset.
    ds_meadow = paths.create_dataset(tables=[tb], default_metadata=snap.metadata)

    # Save meadow dataset.
    ds_meadow.save()
