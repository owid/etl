"""Load a snapshot and create a meadow dataset."""

import numpy as np
import owid.catalog.processing as pr

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Sheets and columns to extract.
SHEETS_AND_COLUMNS = {
    "SOCIOECONOMIC": [
        "SC_CON_DSF_",  # Average download speed
    ],
    "SDG": ["SD_POP_HGR_"],  # Share of population living in the high green area
    "INFRASTRUCTURES": [
        "IN_ROA_DEN_",  # Road network density
        "IN_ROA_LEN_",  # Road length
    ],
    "HEALTH": [
        "HL_SHP_HOS_",  # Share of population with access to hospitals
        "HL_SHP_PHA_",  # Share of population with access to pharmacies
        "HL_FCL_HOS_",  # Number of hospitals
        "HL_FCL_PHA_",  # Number of pharmacies
    ],
}
# Column and indicator mapping.
COLUMN_MAPPING = {
    "SC_CON_DSF_": "Average download speed",
    "SD_POP_HGR_": "Share of population living in the high green area",
    "IN_ROA_DEN_": "Road network density",
    "IN_ROA_LEN_": "Road length",
    "HL_SHP_HOS_": "Share of population with access to hospitals",
    "HL_SHP_PHA_": "Share of population with access to pharmacies",
    "HL_FCL_HOS_": "Number of hospitals",
    "HL_FCL_PHA_": "Number of pharmacies",
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
        # Get population data from GHSL sheet for all years
        tb_ghsl = archive.read("GHS_UCDB_GLOBE_R2024A.xlsx", sheet_name="GHSL")
        pop_columns = [col for col in tb_ghsl.columns if col.startswith("GH_POP_TOT_")]
        tb_population = tb_ghsl[["GC_UCN_MAI_2025", "GC_CNT_GAD_2025", "ID_UC_G0"] + pop_columns].copy()

        for sheet, column_prefixes in SHEETS_AND_COLUMNS.items():
            tb = archive.read("GHS_UCDB_GLOBE_R2024A.xlsx", sheet_name=sheet)
            # Filter the required columns: exact matches and prefixes (with year suffixes)
            selected_columns = [col for col in tb.columns if any(col.startswith(prefix) for prefix in column_prefixes)]
            # Include common columns if they exist in the sheet
            selected_columns += [col for col in city_country if col in tb.columns]
            tb = tb[selected_columns]

            # Merge with population data to get all urban centers with their populations
            tb = pr.merge(tb, tb_population, on=["GC_UCN_MAI_2025", "GC_CNT_GAD_2025", "ID_UC_G0"], how="inner")
            tb = tb.rename(columns={"GC_UCN_MAI_2025": "city", "GC_CNT_GAD_2025": "country"})
            # Identify columns that need to be melted (those with prefixes in the mapping)
            value_vars = [col for col in tb.columns if any(col.startswith(prefix) for prefix in COLUMN_MAPPING.keys())]

            # Keep ID_UC_G0 and population columns as id_vars for now
            id_vars = ["city", "country", "ID_UC_G0"] + pop_columns

            # Melt the dataframe
            tb = tb.melt(id_vars=id_vars, value_vars=value_vars, var_name="indicator", value_name="value")

            # Replace "-" with NaN and convert to numeric
            tb["value"] = tb["value"].replace("-", np.nan)
            tb["value"] = pr.to_numeric(tb["value"], errors="coerce")

            # Extract year (last 4 characters of the column name)
            tb["year"] = tb["indicator"].str[-4:]

            # Remove the year from the indicator name
            tb["indicator"] = tb["indicator"].str[:-4]  # Removing the year
            # Replace indicator names with their mapped full names
            tb["indicator"] = tb["indicator"].map(COLUMN_MAPPING)

            # Melt population columns to match with year
            tb_pop_melted = tb_population.melt(
                id_vars=["GC_UCN_MAI_2025", "GC_CNT_GAD_2025", "ID_UC_G0"],
                value_vars=pop_columns,
                var_name="pop_col",
                value_name="population",
            )
            # Extract year from population column name (e.g., GH_POP_TOT_2020 -> 2020)
            tb_pop_melted["year"] = tb_pop_melted["pop_col"].str[-4:]
            tb_pop_melted = tb_pop_melted.rename(
                columns={
                    "GC_UCN_MAI_2025": "city",
                    "GC_CNT_GAD_2025": "country",
                }
            )
            tb_pop_melted = tb_pop_melted.drop(columns=["pop_col"])

            # Drop population columns from main table
            tb = tb.drop(columns=pop_columns)

            # Merge population data matching by year
            tb = pr.merge(tb, tb_pop_melted, on=["city", "country", "ID_UC_G0", "year"], how="left")

            # Drop ID_UC_G0 as it's no longer needed
            tb = tb.drop(columns=["ID_UC_G0"])

            extracted_data.append(tb)

    tb = pr.concat(extracted_data)

    # Add origins metadata.
    tb["value"].metadata.origins = [snap.m.origin]

    # Aggregate to country level using different methods for different indicator types
    # Identify indicator types
    tb["is_share"] = tb["indicator"].str.contains("Share", case=False, na=False)
    tb["is_count"] = tb["indicator"].str.contains("Number of", case=False, na=False)
    tb["is_road_length"] = tb["indicator"] == "Road length"

    # For Share indicators: calculate population-weighted average
    tb_share = tb[tb["is_share"]].copy()
    tb_share["weighted_value"] = tb_share["value"] * tb_share["population"]

    tb_share_agg = tb_share.groupby(["country", "year", "indicator"], as_index=False).agg(
        {"weighted_value": "sum", "population": "sum"}
    )
    tb_share_agg["value"] = tb_share_agg["weighted_value"] / tb_share_agg["population"]
    tb_share_agg = tb_share_agg[["country", "year", "indicator", "value"]]

    # For count indicators (Number of hospitals/pharmacies): sum counts, then calculate per capita
    tb_count = tb[tb["is_count"]].copy()
    tb_count_agg = tb_count.groupby(["country", "year", "indicator"], as_index=False).agg(
        {"value": "sum", "population": "sum"}
    )
    # Calculate per 100,000 population
    tb_count_agg["value"] = (tb_count_agg["value"] / tb_count_agg["population"]) * 100000
    tb_count_agg = tb_count_agg[["country", "year", "indicator", "value"]]

    # For road length: sum total length, then calculate meters per inhabitant using 2020 population
    tb_road_length = tb[tb["is_road_length"]].copy()

    # Get 2020 population for each country
    tb_pop_2020 = tb[tb["year"] == "2020"].groupby("country", as_index=False)["population"].sum()
    tb_pop_2020 = tb_pop_2020.rename(columns={"population": "population_2020"})

    # Sum road length by country and year
    tb_road_length_agg = tb_road_length.groupby(["country", "year", "indicator"], as_index=False).agg({"value": "sum"})

    # Merge with 2020 population
    tb_road_length_agg = pr.merge(tb_road_length_agg, tb_pop_2020, on="country", how="left")

    # Calculate meters per inhabitant using 2020 population
    tb_road_length_agg["value"] = tb_road_length_agg["value"] / tb_road_length_agg["population_2020"]
    tb_road_length_agg["indicator"] = "Road length per inhabitant"
    tb_road_length_agg = tb_road_length_agg[["country", "year", "indicator", "value"]]

    # For other non-Share, non-count, non-road-length indicators: calculate simple mean
    tb_other = tb[~tb["is_share"] & ~tb["is_count"] & ~tb["is_road_length"]].copy()
    tb_other_agg = tb_other.groupby(["country", "year", "indicator"], as_index=False).agg({"value": "mean"})
    tb_other_agg = tb_other_agg[["country", "year", "indicator", "value"]]

    # Combine all aggregated datasets
    tb = pr.concat([tb_share_agg, tb_count_agg, tb_road_length_agg, tb_other_agg], ignore_index=True)

    # Improve tables format.
    tb = tb.format(["country", "year", "indicator"])

    #
    # Save outputs.
    #
    # Initialize a new meadow dataset.
    ds_meadow = paths.create_dataset(tables=[tb], default_metadata=snap.metadata)

    # Save meadow dataset.
    ds_meadow.save()
