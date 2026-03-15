"""
Script to create aggregated snapshot of WHO Mortality Database.

Downloads and processes WHO mortality and population data, transforming from wide format
(Deaths1-Deaths26, Pop1-Pop26) to long format with proper age group labels.
"""

import zipfile
from io import BytesIO
from pathlib import Path

import click
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


# Age group mappings based on WHO format table
# Each format has 26 positions (Deaths1-26, Pop1-26), mapped to age groups
AGE_GROUP_MAPPINGS = {
    0: [
        "All ages",
        "0",
        "1",
        "2",
        "3",
        "4",
        "5-9",
        "10-14",
        "15-19",
        "20-24",
        "25-29",
        "30-34",
        "35-39",
        "40-44",
        "45-49",
        "50-54",
        "55-59",
        "60-64",
        "65-69",
        "70-74",
        "75-79",
        "80-84",
        "85-89",
        "90-94",
        "95 &+",
        "Unknown",
    ],
    1: [
        "All ages",
        "0",
        "1",
        "2",
        "3",
        "4",
        "5-9",
        "10-14",
        "15-19",
        "20-24",
        "25-29",
        "30-34",
        "35-39",
        "40-44",
        "45-49",
        "50-54",
        "55-59",
        "60-64",
        "65-69",
        "70-74",
        "75-79",
        "80-84",
        "85 &+",
        None,
        None,
        "Unknown",
    ],
    2: [
        "All ages",
        "0",
        "1-4",
        None,
        None,
        "5-9",
        "10-14",
        "15-19",
        "20-24",
        "25-29",
        "30-34",
        "35-39",
        "40-44",
        "45-49",
        "50-54",
        "55-59",
        "60-64",
        "65-69",
        "70-74",
        "75-79",
        "80-84",
        None,
        None,
        None,
        None,
        "Unknown",
    ],
    3: [
        "All ages",
        "0",
        "1",
        "2",
        "3",
        "4",
        "5-9",
        "10-14",
        "15-19",
        "20-24",
        "25-29",
        "30-34",
        "35-39",
        "40-44",
        "45-49",
        "50-54",
        "55-59",
        "60-64",
        "65-69",
        "70-74",
        "75 &+",
        None,
        None,
        None,
        None,
        "Unknown",
    ],
    4: [
        "All ages",
        "0",
        "1-4",
        None,
        None,
        "5-9",
        "10-14",
        "15-19",
        "20-24",
        "25-29",
        "30-34",
        "35-39",
        "40-44",
        "45-49",
        "50-54",
        "55-59",
        "60-64",
        "65-69",
        "70-74",
        "75 &+",
        None,
        None,
        None,
        None,
        None,
        "Unknown",
    ],
    5: [
        "All ages",
        "0",
        "1-4",
        None,
        None,
        "5-9",
        "10-14",
        "15-19",
        "20-24",
        "25-29",
        "30-34",
        "35-39",
        "40-44",
        "45-49",
        "50-54",
        "55-59",
        "60-64",
        "65-69",
        "70 &+",
        None,
        None,
        None,
        None,
        None,
        None,
        "Unknown",
    ],
    6: [
        "All ages",
        "0",
        None,
        None,
        None,
        None,
        "5-9",
        "10-14",
        "15-19",
        "20-24",
        "25-29",
        "30-34",
        "35-39",
        "40-44",
        "45-49",
        "50-54",
        "55-59",
        "60-64",
        "65 &+",
        None,
        None,
        None,
        None,
        None,
        None,
        "Unknown",
    ],
    7: [
        "All ages",
        "0",
        "1-4",
        None,
        None,
        "5-14",
        None,
        None,
        None,
        None,
        None,
        None,
        "15-24",
        None,
        None,
        None,
        "25-34",
        None,
        None,
        None,
        "35-44",
        None,
        None,
        None,
        "45-54",
        "55-64",
        "65-74",
        "75 &+",
        None,
        None,
        None,
        None,
        "Unknown",
    ],
    8: [
        "All ages",
        "0",
        "1-4",
        None,
        None,
        "5-14",
        None,
        None,
        None,
        None,
        None,
        None,
        "15-24",
        None,
        None,
        None,
        "25-34",
        None,
        None,
        None,
        "35-44",
        None,
        None,
        None,
        "45-54",
        "55-64",
        "65 &+",
        None,
        None,
        None,
        None,
        None,
        "Unknown",
    ],
    9: [
        "All ages",
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
    ],
}


def get_age_group(frmat: int, col_idx: int) -> str | None:
    """Get age group label for given format and column index (1-based)."""
    if frmat not in AGE_GROUP_MAPPINGS:
        return None

    mapping = AGE_GROUP_MAPPINGS[frmat]
    if col_idx < 1 or col_idx > len(mapping):
        return None

    return mapping[col_idx - 1]


def transform_to_long_format(
    df_mortality: pd.DataFrame, df_population: pd.DataFrame, df_country_codes: pd.DataFrame
) -> pd.DataFrame:
    """Transform wide format data to long format with age group labels."""
    # Create country code to name mapping
    country_map = dict(zip(df_country_codes["country"], df_country_codes["name"]))

    log.info("Processing mortality data")

    # Process mortality data
    mortality_rows = []
    for _, row in df_mortality.iterrows():
        frmat = int(row["Frmat"])
        country_code = row["Country"]
        base_info = {
            "country": country_map.get(country_code, str(country_code)),
            "year": row["Year"],
            "sex": row["Sex"],
            "cause": row["Cause"],
        }

        for i in range(1, 27):
            col_name = f"Deaths{i}"
            if col_name not in df_mortality.columns:
                continue

            deaths = row[col_name]
            if pd.isna(deaths) or deaths == 0:
                continue

            age_group = get_age_group(frmat, i)
            if age_group is None:
                continue

            mortality_rows.append(
                {
                    **base_info,
                    "age_group": age_group,
                    "deaths": deaths,
                }
            )

    df_mort_long = pd.DataFrame(mortality_rows)
    log.info("Mortality data processed", rows=len(df_mort_long))

    # Process population data
    log.info("Processing population data")
    population_rows = []
    for _, row in df_population.iterrows():
        frmat = int(row["Frmat"])
        country_code = row["Country"]
        base_info = {
            "country": country_map.get(country_code, str(country_code)),
            "year": row["Year"],
            "sex": row["Sex"],
        }

        for i in range(1, 27):
            col_name = f"Pop{i}"
            if col_name not in df_population.columns:
                continue

            pop = row[col_name]
            if pd.isna(pop) or pop == 0:
                continue

            age_group = get_age_group(frmat, i)
            if age_group is None:
                continue

            population_rows.append(
                {
                    **base_info,
                    "age_group": age_group,
                    "population": pop,
                }
            )

    df_pop_long = pd.DataFrame(population_rows)
    log.info("Population data processed", rows=len(df_pop_long))

    # Merge mortality and population
    log.info("Merging mortality and population data")
    df_combined = pd.merge(
        df_mort_long,
        df_pop_long,
        on=["country", "year", "sex", "age_group"],
        how="left",
    )

    # Drop rows where deaths are NaN
    log.info("Dropping rows where deaths are NaN", rows_before=len(df_combined))
    df_combined = df_combined[df_combined["deaths"].notna()]
    log.info("Data cleaned", rows_after=len(df_combined))

    return df_combined


def download_and_extract_zip(url: str, description: str) -> pd.DataFrame:
    """Download ZIP file and extract CSV."""
    log.info("Downloading source", description=description, url=url)
    response = requests.get(url, timeout=600)
    response.raise_for_status()

    with zipfile.ZipFile(BytesIO(response.content)) as z:
        all_files = z.namelist()
        log.info("Files in ZIP", files=all_files)

        # Try CSV/TXT files first, otherwise use the first file
        data_files = [f for f in all_files if f.endswith(".csv") or f.endswith(".txt")]
        if not data_files:
            # Use first non-directory file
            data_files = [f for f in all_files if not f.endswith("/")]

        if not data_files:
            raise ValueError(f"No data file found in {description} ZIP")

        with z.open(data_files[0]) as f:
            df = pd.read_csv(f, encoding="latin1")

    log.info("Loaded source", description=description, file=data_files[0], rows=len(df))
    return df


@click.command()
@click.option("--upload/--skip-upload", default=True, type=bool, help="Upload dataset to Snapshot")
def main(upload: bool) -> None:
    """Create aggregated WHO mortality snapshot matching garden format."""
    log.info("Starting download of WHO mortality data sources")

    df_mortality = download_and_extract_zip(MORTALITY_URL, "mortality data")
    df_population = download_and_extract_zip(POPULATION_URL, "population data")
    df_country_codes = download_and_extract_zip(COUNTRY_CODES_URL, "country codes")

    log.info("Transforming data to long format with age groups")
    df_aggregated = transform_to_long_format(df_mortality, df_population, df_country_codes)
    log.info("Aggregation complete", rows=len(df_aggregated))
    df_aggregated = repack_frame(df_aggregated)

    snap = Snapshot(f"who/{SNAPSHOT_VERSION}/mortality_database_aggregated.feather")
    snap.path.parent.mkdir(exist_ok=True, parents=True)
    snap.create_snapshot(data=df_aggregated, upload=upload)

    log.info("Snapshot creation complete")


if __name__ == "__main__":
    main()
