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


def create_metadata_table_from_json(json_data: dict) -> Table:
    """Parse JSON metadata from World Bank API and create a metadata table."""
    indicators_data = []

    # Extract indicators from the 'data' field in JSON
    for indicator in json_data["data"]:
        # Extract field information from the indicator
        fields = indicator.get("fields", [])

        # Create a dictionary with indicator metadata
        indicator_info = {
            "id": indicator.get("id"),
            "title": indicator.get("title"),
            "description": indicator.get("description", ""),
            "url": indicator.get("url", ""),
            "dataset": indicator.get("dataset", {}).get("title", ""),
        }

        # Add fields as key:name, value:description pairs
        for field in fields:
            field_name = field.get("name", "")
            # We already have description in the main dict
            if field_name == "Description":
                continue

            field_description = field.get("description", "")
            if field_name:
                indicator_info[field_name] = field_description

        indicators_data.append(indicator_info)

    # Create DataFrame
    df_meta_new = pd.DataFrame(indicators_data)

    # Certain indicators have Code, but not Series Code
    df_meta_new["Series Code"] = df_meta_new["Series Code"].fillna(df_meta_new["Code"])
    df_meta_new.drop(columns=["Code"], inplace=True)

    # !!! There are DUPLICATES. It looks like they're keeping old versions of indicators and there's no way to distinguish them.
    # Strategy: If duplicates all have non-null "Base Period", keep the one with highest Base Period (most recent)
    # Otherwise, keep the first occurrence

    """
    gf = df_meta_new[df_meta_new["Series Code"].str.lower().str.replace(".", "_") == "dt_oda_oatl_kd"]

    def deduplicate_by_base_period(group):
        if len(group) == 1:
            return group

        # Check if all duplicates have non-null Base Period
        base_periods = group["Base Period"].dropna()
        if len(base_periods) == len(group) and len(base_periods) > 1:
            # All have non-null base periods, convert to numeric and keep the highest
            try:
                # Convert Base Period to numeric (handle cases like "2021", "2017", etc.)
                numeric_periods = pd.to_numeric(base_periods, errors="coerce")
                if not numeric_periods.isna().all():
                    # Keep the row with the highest base period
                    max_idx = numeric_periods.idxmax()
                    # log.info(
                    #     f"Duplicate series {group['Series Code'].iloc[0]}: keeping base period {group.loc[max_idx, 'Base Period']} over others"
                    # )
                    return group.loc[[max_idx]]
            except:
                pass

        # Fallback: keep first occurrence
        # log.info(
        #     f"Duplicate series {group['Series Code'].iloc[0]}: keeping first occurrence (no clear base period distinction)"
        # )
        return group.iloc[[0]]

    # Group by Series Code and apply deduplication logic
    df_meta_new = (
        df_meta_new.groupby("Series Code", as_index=False, group_keys=False)
        .apply(deduplicate_by_base_period)
        .reset_index(drop=True)
    )
    """

    # Drop duplicate indicators
    vc = df_meta_new["Series Code"].value_counts()
    vc = vc[vc > 1]
    if not vc.empty:
        log.warning(f"Dropping {len(vc)} duplicate indicators: {vc.index.tolist()[:5]}...")
        df_meta_new = df_meta_new[~df_meta_new["Series Code"].isin(vc.index)]

    # Create Table with proper naming
    tb_meta_new = Table(df_meta_new, short_name="wdi_metadata", underscore=True)

    return tb_meta_new


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

    # Load metadata from snapshot.
    tb_meta = Table(pd.read_csv(zf.open("WDISeries.csv")), short_name="wdi_metadata", underscore=True)

    # Load and parse JSON metadata from API
    json_data = json.load(zf.open("WDIMetadata.json"))
    tb_meta_new = create_metadata_table_from_json(json_data)

    tb_meta = tb_meta_new

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = paths.create_dataset(tables=[tb, tb_meta], default_metadata=snap.metadata)

    # Save changes in the new garden dataset.
    ds_meadow.save()

    log.info("wdi.end")
