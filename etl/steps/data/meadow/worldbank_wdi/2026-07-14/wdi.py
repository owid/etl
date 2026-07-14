import json
import zipfile

import pandas as pd
from owid.catalog import Table
from owid.catalog.utils import underscore
from pandas.api.types import is_numeric_dtype  # ty: ignore
from structlog import get_logger

from etl.helpers import PathFinder

# Initialize logger.
log = get_logger()

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Rich-metadata columns we pull out of WDISeries.csv (left = CSV header, right = column
# the garden step expects). WDISeries.csv is the per-field metadata table the WB
# ships inside the WDI zip; it tracks the data portal's rich glossary fields and
# is generally fresher than the DDH API for the same fields.
WDI_SERIES_RICH_COLUMNS: dict[str, str] = {
    "Long definition": "long_definition",
    "Short definition": "short_definition",
    "Aggregation method": "aggregation_method",
    "Periodicity": "periodicity",
    "Base Period": "base_period",
    "Limitations and exceptions": "limitations_and_exceptions",
    "Statistical concept and methodology": "statistical_concept_and_methodology",
    "Development relevance": "development_relevance",
    "Notes from original source": "notes_from_original_source",
    "Other notes": "other_notes",
    "Related source links": "related_source_links",
    "Other web links": "other_web_links",
    "Related indicators": "related_indicators",
    "General comments": "general_comments",
    "License Type": "license_type",
}


def create_metadata_table(legacy_json: dict, series_csv) -> Table:
    """Build the wdi_metadata table from two complementary WB sources.

    - Legacy v2 JSON (api.worldbank.org/v2): core fields used as join keys
      downstream — `indicator_code`, `indicator_name`, `unit`, `source`, `topic`.
      `source` is the `rawName` the garden step matches against `wdi.sources.json`,
      so we keep legacy as the authoritative source for these four fields. CSV's
      `Source` agrees with legacy on 1499 of 1516 indicators, but the 17 mismatches
      are CSV-NaN cases where legacy still has a value (Enterprise Surveys group).

    - WDISeries.csv (inside the WDI zip): per-field rich metadata. WB keeps this
      in sync with their data portal display, while the DDH API lags. CSV `Source`
      and `Topic` are intentionally omitted here — legacy v2 wins for those.
    """
    # Legacy JSON → core join-key columns.
    df_legacy = pd.DataFrame(legacy_json["data"])
    df_legacy.rename(columns={"indicator_code": "series_code"}, inplace=True)

    # WDISeries.csv → rich glossary fields, snake_cased to the garden-expected names.
    df_csv = pd.read_csv(series_csv, usecols=["Series Code", *WDI_SERIES_RICH_COLUMNS.keys()])
    df_csv = df_csv.rename(columns={"Series Code": "series_code", **WDI_SERIES_RICH_COLUMNS})

    df_meta = df_legacy.merge(df_csv, on="series_code", how="left")

    return Table(df_meta, short_name="wdi_metadata", underscore=True)


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
    assert (df.groupby("indicator_code", observed=True)["indicator_name"].apply(lambda gp: gp.nunique()) == 1).all(), (
        "A variable code in `WDIData.csv` has multiple variable names."
    )
    assert (df.groupby("indicator_name", observed=True)["indicator_code"].apply(lambda gp: gp.nunique()) == 1).all(), (
        "A variable name in `WDIData.csv` has multiple variable codes."
    )

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

    # Load metadata: legacy JSON for core join keys, WDISeries.csv for rich glossary fields.
    legacy_json = json.load(zf.open("WDIMetadataLegacy.json"))
    tb_meta = create_metadata_table(legacy_json, zf.open("WDISeries.csv"))

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = paths.create_dataset(tables=[tb, tb_meta], default_metadata=snap.metadata)

    # Save changes in the new garden dataset.
    ds_meadow.save()

    log.info("wdi.end")
