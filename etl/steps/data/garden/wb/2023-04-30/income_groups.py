"""Load a meadow dataset and create a garden dataset."""

import numpy as np
import pandas as pd
from owid.catalog import Dataset, Table, VariableMeta
from structlog import get_logger

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

log = get_logger()

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

EXPECTED_MISSING_COUNTRIES_IN_LATEST_RELEASE = {
    "Czechoslovakia",
    "Mayotte",
    "Netherlands Antilles",
    "Serbia and Montenegro",
    "USSR",
    "Venezuela",
    "Yugoslavia",
}


def run(dest_dir: str) -> None:
    log.info("income_groups: start")

    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow: Dataset = paths.load_dependency("income_groups")

    # Read table from meadow dataset.
    tb_meadow = ds_meadow["income_groups"]

    # Create a dataframe with data from the table.
    df = pd.DataFrame(tb_meadow)

    #
    # Process data for main table.
    #
    log.info("income_groups: harmonize_countries")
    df = geo.harmonize_countries(df=df, countries_file=paths.country_mapping_path)
    # Harmonize income group labels
    log.info("income_groups: harmonize income group labels")
    df = harmonize_income_group_labels(df)
    # Set index, drop code column
    df = df.drop(columns=["country_code"]).set_index(["country", "year"], verify_integrity=True).sort_index()

    #
    # Process data for table with values of latest release
    #
    # Get classifications for each country for the latest year available.
    df_latest = df.reset_index().drop_duplicates(subset=["country"], keep="last")
    # Sanity checks of missing countries for latest release
    missing_countries = set(df_latest.loc[df_latest["year"] != df_latest["year"].max(), "country"])
    assert (
        missing_countries == EXPECTED_MISSING_COUNTRIES_IN_LATEST_RELEASE
    ), f"Unexpected missing countries in latest release. All missing countries: {missing_countries}"
    # Get data only for latest release (and remove column year)
    df_latest = df_latest[df_latest["year"] == df_latest["year"].max()].drop(columns=["year"])
    # Set index, drop code column
    df_latest = df_latest.set_index(["country"], verify_integrity=True).sort_index()

    #
    # Create tables from dataframes
    #
    tb_garden = Table(df, short_name=paths.short_name)
    tb_garden_latest = Table(df_latest, short_name=f"{paths.short_name}_latest")

    # Fill metadata for table with latest values
    # It makes references to latest release year, so it is helpful to do it in code
    assert (num_sources := len(ds_meadow.metadata.sources) == 1), f"Number of sources should be 1, not {num_sources}"
    release_year = ds_meadow.metadata.sources[0].publication_year
    tb_garden_latest["classification"].metadata = VariableMeta(
        title=f"World Bank income classification ({release_year} dataset release)",
        description=f"Classification (as per the {release_year} dataset release) by the World Bank based on the country's income.",
        unit="",
    )

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(dest_dir, tables=[tb_garden, tb_garden_latest], default_metadata=ds_meadow.metadata)
    # Save changes in the new garden dataset.
    ds_garden.save()

    log.info("income_groups: end")


def harmonize_income_group_labels(df: pd.DataFrame) -> pd.DataFrame:
    # Check raw labels are as expected
    log.info("income_groups: harmonizing classification labels (e.g. 'LM' -> 'Low-income countries')")
    assert (labels := set(df["classification"])) == {
        "..",
        "H",
        "L",
        "LM",
        "LM*",
        "UM",
        np.nan,
    }, f"Unknown income group label! Check {labels}"
    # Check unusual LM* label
    msk = df["classification"] == "LM*"
    lm_special = set(df[msk]["country_code"].astype(str) + df[msk]["year"].astype(str))
    assert lm_special == {"YEM1987", "YEM1988"}, f"Unexpected entries with classification 'LM*': {df[msk]}"
    # Rename labels
    MAPPING_CLASSIFICATION = {
        "..": np.nan,  # no available classification for country-year (maybe country didn't exist yet/anymore)
        "L": "Low-income countries",
        "H": "High-income countries",
        "UM": "Upper-middle-income countries",
        "LM": "Lower-middle-income countries",
        "LM*": "Lower-middle-income countries",
    }
    df["classification"] = df["classification"].map(MAPPING_CLASSIFICATION)
    # Drop years with no country classification
    df = df.dropna(subset="classification")
    return df
