import json
import zipfile

import pandas as pd
from owid.catalog import Table
from owid.catalog.utils import underscore
from pandas.api.types import is_numeric_dtype  # type: ignore
from structlog import get_logger

from etl.helpers import PathFinder

# Initialize logger.
log = get_logger()

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def create_metadata_table(legacy_json: dict, new_json: dict) -> Table:
    """Create metadata table by merging legacy and new API metadata.

    Legacy JSON (from api.worldbank.org/v2) provides core fields:
    - indicator_code, indicator_name, unit, source, topic

    New JSON (from ddh-openapi.worldbank.org) provides rich descriptions:
    - long_definition, short_definition, limitations_and_exceptions,
      statistical_concept_and_methodology, development_relevance, notes_from_original_source
    """
    # Parse legacy metadata (core fields)
    df_legacy = pd.DataFrame(legacy_json["data"])
    df_legacy.rename(columns={"indicator_code": "series_code"}, inplace=True)

    # Parse new metadata (rich descriptions)
    indicators_data = []
    for indicator in new_json["data"]:
        fields = indicator.get("fields", [])
        indicator_info = {"series_code": None}

        for field in fields:
            field_name = field.get("name", "")
            field_description = field.get("description", "")
            if field_name == "Series Code":
                indicator_info["series_code"] = field_description
            elif field_name == "Code" and not indicator_info["series_code"]:
                indicator_info["series_code"] = field_description
            elif field_name == "Long definition":
                indicator_info["long_definition"] = field_description
            elif field_name == "Short definition":
                indicator_info["short_definition"] = field_description
            elif field_name == "Limitations and exceptions":
                indicator_info["limitations_and_exceptions"] = field_description
            elif field_name == "Statistical concept and methodology":
                indicator_info["statistical_concept_and_methodology"] = field_description
            elif field_name == "Development relevance":
                indicator_info["development_relevance"] = field_description
            elif field_name == "Notes from original source":
                indicator_info["notes_from_original_source"] = field_description

        if indicator_info["series_code"]:
            indicators_data.append(indicator_info)

    df_new = pd.DataFrame(indicators_data)

    # Drop duplicates from new metadata (keep first occurrence)
    df_new = df_new.drop_duplicates(subset=["series_code"], keep="first")

    # Merge: legacy as base, add rich descriptions from new
    df_meta = df_legacy.merge(df_new, on="series_code", how="left")

    # Create Table with proper naming
    tb_meta = Table(df_meta, short_name="wdi_metadata", underscore=True)

    return tb_meta


def run() -> None:
    log.info("wdi.start")

    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot()

    # Load data from snapshot.
    zf = zipfile.ZipFile(snap.path)

    df = pd.read_csv(zf.open("WDICSV.csv"))

    df.dropna(how="all", axis=1, inplace=True)

    # drops rows with only NaN values
    years = df.columns[df.columns.str.contains(r"^\d{4}$")].sort_values().tolist()
    df.dropna(subset=years, how="all", inplace=True)

    # converts columns and indicator_code to snake case
    df.columns = df.columns.map(lambda x: x if x in years else underscore(x))
    orig_indicator_code = df["indicator_code"].copy()
    df["indicator_code"] = df["indicator_code"].astype("category").map(underscore)
    indicator_code_map = dict(zip(df["indicator_code"], orig_indicator_code))

    assert df["country_name"].notnull().all()
    assert df["indicator_code"].notnull().all()
    assert df[years].apply(lambda s: is_numeric_dtype(s), axis=0).all(), "One or more {year} columns is non-numeric"

    # variable code <-> variable name should be a 1:1 mapping
    assert (
        df.groupby("indicator_code", observed=True)["indicator_name"].apply(lambda gp: gp.nunique()) == 1
    ).all(), "A variable code in `WDIData.csv` has multiple variable names."
    assert (
        df.groupby("indicator_name", observed=True)["indicator_code"].apply(lambda gp: gp.nunique()) == 1
    ).all(), "A variable name in `WDIData.csv` has multiple variable codes."

    # reshapes data from `country indicator 1960 1961 ...` format to long format `country indicator_code year value`
    df_long = (
        df.set_index(["country_name", "indicator_code"])[years]
        .stack()
        .sort_index()
        .reset_index()
        .rename(columns={"country_name": "country", "level_2": "year", 0: "value"})
    )

    # reshape from long format to wide `country year EG.CFT.ACCS.ZS SH.HIV.INCD.YG ...`
    df_wide = (
        df_long.set_index(["country", "year", "indicator_code"], verify_integrity=True)
        .squeeze()
        .unstack("indicator_code")
        .dropna(how="all")
    )
    assert not df_wide.isnull().all(axis=1).any(), "Unexpected state: One or more rows contains only NaN values."

    #
    # Process data.
    #
    # Create a new table and ensure all columns are snake-case.
    tb = Table(df_wide, short_name=paths.short_name, underscore=True)

    # Add origin to all indicators
    for col in tb.columns:
        tb[col].m.origins = [snap.m.origin]

        # Add original code as titles
        tb[col].m.title = indicator_code_map[col]

    # Load metadata from both JSON files in snapshot
    # Legacy JSON provides core fields (indicator_name, source), new JSON provides rich descriptions
    legacy_json = json.load(zf.open("WDIMetadataLegacy.json"))
    new_json = json.load(zf.open("WDIMetadata.json"))
    tb_meta = create_metadata_table(legacy_json, new_json)

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = paths.create_dataset(tables=[tb, tb_meta], default_metadata=snap.metadata)

    # Save changes in the new garden dataset.
    ds_meadow.save()

    log.info("wdi.end")
