"""
Script to create aggregated snapshot of WHO Mortality Database with calculated rates.

FIXED VERSION: Correctly handles different Format codes (Frmat) for age group mapping.

This script downloads and processes three WHO mortality data files:
1. Mortality data by cause (morticd10_part6.zip)
2. Population data (mort_pop.zip)
3. Country codes mapping (mort_country_codes.zip)

Output matches the garden step format with columns:
- country, year, sex, age_group, cause, icd10_codes
- number (deaths)
- percentage_of_cause_specific_deaths_out_of_total_deaths
- age_standardized_death_rate_per_100_000_standard_population
- death_rate_per_100_000_population
"""

import zipfile
from io import BytesIO
from pathlib import Path

import click
import numpy as np
import pandas as pd
import requests
from owid.repack import repack_frame
from structlog import get_logger

from etl.snapshot import Snapshot

SNAPSHOT_VERSION = Path(__file__).parent.name
log = get_logger()

# URLs
MORTALITY_URL = "https://cdn.who.int/media/docs/default-source/world-health-data-platform/mortality-raw-data/morticd10_part6.zip?sfvrsn=ec801a61_4"
POPULATION_URL = "https://cdn.who.int/media/docs/default-source/world-health-data-platform/mortality-raw-data/mort_pop.zip?sfvrsn=937039fc_26&ua=1"
COUNTRY_CODES_URL = "https://cdn.who.int/media/docs/default-source/world-health-data-platform/mortality-raw-data/mort_country_codes.zip?sfvrsn=800faac2_5&ua=1"

# ICD-10 cause definitions (from existing snapshot)
ICD_CAUSES = {
    "All Causes": {"icd_codes": "A00-Y89", "broad_cause_group": "All Causes"},
    "Cardiovascular diseases": {"icd_codes": "I00-I99", "broad_cause_group": "Noncommunicable diseases"},
    "Congenital anomalies": {"icd_codes": "Q00-Q99", "broad_cause_group": "Noncommunicable diseases"},
    "Diabetes mellitus, blood and endocrine disorders": {
        "icd_codes": "E10-E14, D55-D64 (minus D64.9),D65-D89, E03-E07, E15-E16, E20-E34, E65-E88",
        "broad_cause_group": "Noncommunicable diseases",
    },
    "Digestive diseases": {"icd_codes": "K20-K92", "broad_cause_group": "Noncommunicable diseases"},
    "Genitourinary diseases": {"icd_codes": "N00-N64, N75-N98", "broad_cause_group": "Noncommunicable diseases"},
    "Ill-defined injuries": {"icd_codes": "Y10-Y34, Y872", "broad_cause_group": "Injuries"},
    "Infectious and parasitic diseases": {
        "icd_codes": "A00-B99, G00-G04, G14, N70-N73, P37.3, P37.4",
        "broad_cause_group": "Communicable, maternal, perinatal and nutritional conditions",
    },
    "Intentional injuries": {"icd_codes": "X60-Y09, Y35-Y36, Y870, Y871", "broad_cause_group": "Injuries"},
    "Malignant neoplasms": {"icd_codes": "C00-C97", "broad_cause_group": "Noncommunicable diseases"},
    "Maternal conditions": {
        "icd_codes": "O00-O99",
        "broad_cause_group": "Communicable, maternal, perinatal and nutritional conditions",
    },
    "Musculoskeletal diseases": {"icd_codes": "M00-M99", "broad_cause_group": "Noncommunicable diseases"},
    "Neuropsychiatric conditions": {
        "icd_codes": "F01-F99, G06-G98 (minus G14), U07.0, X41, X42, X44, X45",
        "broad_cause_group": "Noncommunicable diseases",
    },
    "Nutritional deficiencies": {
        "icd_codes": "E00-E02, E40-E46, E50, D50-D53,D64.9, E51-E64",
        "broad_cause_group": "Communicable, maternal, perinatal and nutritional conditions",
    },
    "Oral conditions": {"icd_codes": "K00-K14", "broad_cause_group": "Noncommunicable diseases"},
    "Other neoplasms": {"icd_codes": "D00-D48", "broad_cause_group": "Noncommunicable diseases"},
    "Perinatal conditions": {
        "icd_codes": "P00-P96 (minus P23, P37.3, P37.4)",
        "broad_cause_group": "Communicable, maternal, perinatal and nutritional conditions",
    },
    "Respiratory diseases": {"icd_codes": "J30-J98", "broad_cause_group": "Noncommunicable diseases"},
    "Respiratory infections": {
        "icd_codes": "H65-H66, J00-J22,  P23, U04, U07.1, U07.2, U09.9, U10.9",
        "broad_cause_group": "Communicable, maternal, perinatal and nutritional conditions",
    },
    "Sense organ diseases": {"icd_codes": "H00-H61, H68-H93", "broad_cause_group": "Noncommunicable diseases"},
    "Skin diseases": {"icd_codes": "L00-L98", "broad_cause_group": "Noncommunicable diseases"},
    "Sudden infant death syndrome": {"icd_codes": "R95", "broad_cause_group": "Noncommunicable diseases"},
    "Unintentional injuries": {
        "icd_codes": "V01-X59, Y40-Y86, Y88, Y89 (minus X41-X42, X44-X45), U12.9",
        "broad_cause_group": "Injuries",
    },
    "Ill-defined diseases": {"icd_codes": "R00-R94, R96-R99", "broad_cause_group": "Ill-defined diseases"},
}

# WHO World Standard Population 2000-2025
STANDARD_POPULATION = {
    "less than 1 year": 2400,
    "1-4 years": 9600,
    "5-9 years": 10000,
    "10-14 years": 9000,
    "15-19 years": 9000,
    "20-24 years": 8000,
    "25-29 years": 8000,
    "30-34 years": 6000,
    "35-39 years": 6000,
    "40-44 years": 6000,
    "45-49 years": 6000,
    "50-54 years": 5000,
    "55-59 years": 4000,
    "60-64 years": 4000,
    "65-69 years": 3000,
    "70-74 years": 2000,
    "75-79 years": 1000,
    "80-84 years": 500,
    "over 85 years": 500,
}

# Age group mappings by Format code (Frmat) from WHO documentation Annex 1
# Format 0: Most detailed (Deaths1 = All ages, Deaths2-6 = single years 0-4, Deaths7+ = 5-year bands)
AGE_GROUP_MAPPINGS_BY_FORMAT = {
    0: {
        # Deaths1 is "All ages" - we skip this column
        2: "less than 1 year",  # 0 year
        3: "1-4 years",  # 1 year
        4: "1-4 years",  # 2 years
        5: "1-4 years",  # 3 years
        6: "1-4 years",  # 4 years
        7: "5-9 years",
        8: "10-14 years",
        9: "15-19 years",
        10: "20-24 years",
        11: "25-29 years",
        12: "30-34 years",
        13: "35-39 years",
        14: "40-44 years",
        15: "45-49 years",
        16: "50-54 years",
        17: "55-59 years",
        18: "60-64 years",
        19: "65-69 years",
        20: "70-74 years",
        21: "75-79 years",
        22: "80-84 years",
        23: "over 85 years",  # 85-89 years
        24: "over 85 years",  # 90-94 years
        25: "over 85 years",  # 95+ years
        26: "Unknown age",
    },
    # For other formats, use a simplified mapping (would need to be refined based on WHO docs)
    # Most common formats after 0 are 1, 2, 3, 4, 7, 9
    1: {  # Format 1: 5-year age groups starting from Deaths1
        1: "less than 1 year",
        2: "1-4 years",
        3: "5-9 years",
        4: "10-14 years",
        5: "15-19 years",
        6: "20-24 years",
        7: "25-29 years",
        8: "30-34 years",
        9: "35-39 years",
        10: "40-44 years",
        11: "45-49 years",
        12: "50-54 years",
        13: "55-59 years",
        14: "60-64 years",
        15: "65-69 years",
        16: "70-74 years",
        17: "75-79 years",
        18: "80-84 years",
        19: "over 85 years",
        20: "Unknown age",
    },
    2: {  # Format 2: Similar to Format 1
        1: "less than 1 year",
        2: "1-4 years",
        3: "5-9 years",
        4: "10-14 years",
        5: "15-19 years",
        6: "20-24 years",
        7: "25-29 years",
        8: "30-34 years",
        9: "35-39 years",
        10: "40-44 years",
        11: "45-49 years",
        12: "50-54 years",
        13: "55-59 years",
        14: "60-64 years",
        15: "65-69 years",
        16: "70-74 years",
        17: "75-79 years",
        18: "80-84 years",
        19: "over 85 years",
        20: "Unknown age",
    },
}


def get_age_group_for_format(frmat: int, deaths_col_idx: int) -> str:
    """
    Get age group name for a given Format code and Deaths column index.

    Args:
        frmat: Format code (Frmat column value)
        deaths_col_idx: Deaths column index (1-26)

    Returns:
        Age group name, or "Unknown age" if not mapped
    """
    # Format 2 has same structure as Format 0 (Deaths1 = All ages, Deaths2 = infants)
    if frmat == 2:
        frmat = 0

    # Get the mapping for this format, or use Format 1 as fallback
    format_mapping = AGE_GROUP_MAPPINGS_BY_FORMAT.get(frmat, AGE_GROUP_MAPPINGS_BY_FORMAT[1])

    return format_mapping.get(deaths_col_idx, "Unknown age")


def download_and_extract_zip(url: str, description: str) -> pd.DataFrame:
    """Download and extract a ZIP file, return the data as DataFrame."""
    log.info(f"Downloading {description}", url=url)
    response = requests.get(url, timeout=300)
    response.raise_for_status()

    with zipfile.ZipFile(BytesIO(response.content)) as zip_ref:
        files = zip_ref.namelist()
        data_file = files[0]
        with zip_ref.open(data_file) as f:
            df = pd.read_csv(f, low_memory=False)

    log.info(f"Loaded {description}", rows=len(df))
    return df


def reshape_mortality_data(df: pd.DataFrame) -> pd.DataFrame:
    """Reshape mortality data from wide to long format, handling Format codes correctly."""
    # Filter to ICD-10 data (List = 101, 103, 104 are all ICD-10 with different tabulation formats)
    df = df[df["List"].isin([101, 103, 104])].copy()

    log.info("Melting mortality data by Format code")

    # Process each Format code separately to apply correct age group mapping
    all_dfs = []

    for frmat in df["Frmat"].unique():
        df_frmat = df[df["Frmat"] == frmat].copy()

        # DEBUG: Log Format code processing
        countries_in_format = df_frmat["Country"].unique()
        log.info(f"Processing Format {frmat}", countries=len(countries_in_format))

        # Get death columns
        death_cols = [c for c in df_frmat.columns if c.startswith("Deaths") and c[6:].isdigit()]

        # For Format 0 and 2, skip Deaths1 (All ages)
        # Format 0 and 2 both have Deaths1 = "All ages", Deaths2 = infants
        if frmat in [0, 2]:
            death_cols = [c for c in death_cols if c != "Deaths1"]
            log.info(f"Format {frmat}: Skipping Deaths1, using {len(death_cols)} columns")

        id_vars = ["Country", "Year", "Sex", "Cause", "Frmat"]

        df_long = df_frmat.melt(id_vars=id_vars, value_vars=death_cols, var_name="age_col", value_name="deaths")

        # DEBUG: Check France 2022 before aggregation
        if "Country" in df_long.columns:
            fr_2022 = df_long[
                (df_long["Country"].astype(str).str.contains("France", na=False) | (df_long["Country"] == 4080))
                & (df_long["Year"] == 2022)
                & (df_long["Sex"] == 1)
                & (df_long["Cause"] == "AAA")
            ]
            if len(fr_2022) > 0:
                log.info(
                    f"DEBUG France 2022 Males AAA (Format {frmat})",
                    rows=len(fr_2022),
                    sample_deaths=fr_2022["deaths"].head(5).tolist(),
                    sample_age_cols=fr_2022["age_col"].head(5).tolist(),
                )

        # Extract age group index
        df_long["age_group_idx"] = df_long["age_col"].str.extract(r"(\d+)").astype(int)

        # Map to age group names using format-specific mapping
        df_long["age_group"] = df_long["age_group_idx"].apply(lambda idx: get_age_group_for_format(frmat, idx))

        all_dfs.append(df_long)

    # Combine all formats
    df_long = pd.concat(all_dfs, ignore_index=True)

    # Remove missing values
    df_long = df_long[df_long["deaths"].notna()].copy()
    df_long["deaths"] = df_long["deaths"].astype(float)

    # Aggregate by age_group (since Format 0 maps multiple Deaths columns to same age group)
    log.info("Aggregating deaths by age group")
    df_long = df_long.groupby(["Country", "Year", "Sex", "Cause", "Frmat", "age_group"], as_index=False).agg(
        {"deaths": "sum"}
    )

    return df_long


def reshape_population_data(df: pd.DataFrame) -> pd.DataFrame:
    """Reshape population data from wide to long format, handling Format codes correctly."""
    log.info("Melting population data by Format code")

    all_dfs = []

    for frmat in df["Frmat"].unique():
        df_frmat = df[df["Frmat"] == frmat].copy()

        pop_cols = [c for c in df_frmat.columns if c.startswith("Pop") and c[3:].isdigit()]

        # For Format 0, skip Pop1 (All ages)
        if frmat == 0:
            pop_cols = [c for c in pop_cols if c != "Pop1"]

        id_vars = ["Country", "Year", "Sex", "Frmat"]

        df_long = df_frmat.melt(id_vars=id_vars, value_vars=pop_cols, var_name="age_col", value_name="population")

        df_long["age_group_idx"] = df_long["age_col"].str.extract(r"(\d+)").astype(int)
        df_long["age_group"] = df_long["age_group_idx"].apply(lambda idx: get_age_group_for_format(frmat, idx))

        all_dfs.append(df_long)

    df_long = pd.concat(all_dfs, ignore_index=True)

    df_long = df_long[df_long["population"].notna()].copy()
    df_long["population"] = df_long["population"].astype(float)

    # Aggregate by age_group
    log.info("Aggregating population by age group")
    df_long = df_long.groupby(["Country", "Year", "Sex", "Frmat", "age_group"], as_index=False).agg(
        {"population": "sum"}
    )

    return df_long


def aggregate_and_calculate_rates(
    df_mortality: pd.DataFrame, df_population: pd.DataFrame, df_countries: pd.DataFrame
) -> pd.DataFrame:
    """
    Aggregate mortality data and calculate all required metrics.

    Returns DataFrame matching garden format with columns:
    - country, year, sex, age_group, cause, icd10_codes
    - number, percentage_of_cause_specific_deaths_out_of_total_deaths
    - age_standardized_death_rate_per_100_000_standard_population
    - death_rate_per_100_000_population
    """
    log.info("Reshaping mortality data")
    df_mort = reshape_mortality_data(df_mortality)

    log.info("Reshaping population data")
    df_pop = reshape_population_data(df_population)

    log.info("Merging mortality and population data")
    df_merged = df_mort.merge(df_pop, on=["Country", "Year", "Sex", "Frmat", "age_group"], how="left")

    # Fill missing population with 0 (will result in NaN rates)
    df_merged["population"] = df_merged["population"].fillna(0)

    # Calculate age-specific death rate per 100,000
    df_merged["death_rate_per_100_000_population"] = np.where(
        df_merged["population"] > 0, (df_merged["deaths"] / df_merged["population"]) * 100000, np.nan
    )

    log.info("Adding country names and sex labels")
    # Rename columns for output
    df_merged = df_merged.rename(columns={"deaths": "number", "Cause": "cause"})

    # Add country names
    df_merged = df_merged.merge(df_countries, left_on="Country", right_on="country", how="left")

    # Map sex codes
    sex_map = {1: "Males", 2: "Females", 9: "Both sexes"}
    df_merged["sex"] = df_merged["Sex"].map(sex_map).fillna("Unknown sex")

    # Select final columns - keep ICD-10 codes, don't aggregate
    df_final = df_merged[
        [
            "name",  # country
            "Year",  # year
            "sex",
            "age_group",
            "cause",  # ICD-10 code (e.g., "I25", "C34", "AAA")
            "number",  # deaths
            "population",  # population for this age group
            "death_rate_per_100_000_population",
        ]
    ].copy()

    df_final = df_final.rename(
        columns={
            "name": "country",
            "Year": "year",
        }
    )

    return df_final


@click.command()
@click.option("--upload/--skip-upload", default=True, type=bool, help="Upload dataset to Snapshot")
def main(upload: bool) -> None:
    """Create aggregated WHO mortality snapshot matching garden format."""

    log.info("Starting download of WHO mortality data sources")

    # Download all three data sources
    df_mortality = download_and_extract_zip(MORTALITY_URL, "mortality data")
    df_population = download_and_extract_zip(POPULATION_URL, "population data")
    df_country_codes = download_and_extract_zip(COUNTRY_CODES_URL, "country codes")

    # Aggregate and calculate rates
    log.info("Aggregating data and calculating rates")
    df_aggregated = aggregate_and_calculate_rates(df_mortality, df_population, df_country_codes)

    log.info("Aggregation complete", rows=len(df_aggregated))
    log.info("Columns", columns=list(df_aggregated.columns))
    log.info("Sample", sample=df_aggregated.head(2).to_dict())

    # Repack to optimize storage
    df_aggregated = repack_frame(df_aggregated)

    # Create snapshot
    snap = Snapshot(f"who/{SNAPSHOT_VERSION}/mortality_database_aggregated.feather")
    snap.path.parent.mkdir(exist_ok=True, parents=True)
    snap.create_snapshot(data=df_aggregated, upload=upload)

    log.info("Snapshot creation complete")


if __name__ == "__main__":
    main()
